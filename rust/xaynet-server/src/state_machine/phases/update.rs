use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};
use xaynet_core::{
    mask::{Aggregation, MaskObject},
    LocalSeedDict,
    UpdateParticipantPublicKey,
};

use crate::{
    state_machine::{
        events::DictionaryUpdate,
        phases::{Handler, Phase, PhaseName, PhaseState, PhaseStateError, Shared, Sum2},
        requests::{StateMachineRequest, UpdateRequest},
        RequestError,
        StateMachine,
    },
    storage::{Storage, StorageError},
};

/// Error that occurs during the update phase.
#[derive(Error, Debug)]
pub enum UpdateStateError {
    #[error("seed dictionary does not exists")]
    NoSeedDict,
    #[error("fetching seed dictionary failed: {0}")]
    FetchSeedDict(StorageError),
}

/// The update state.
#[derive(Debug)]
pub struct Update {
    /// The aggregator for masked models.
    model_agg: Aggregation,
    /// The number of update messages successfully processed.
    accepted: u64,
    /// The number of update messages failed to processed.
    rejected: u64,
    /// The number of update messages discarded without being processed.
    discarded: u64,
}

#[async_trait]
impl<S> Phase<S> for PhaseState<Update, S>
where
    Self: Handler,
    S: Storage,
{
    const NAME: PhaseName = PhaseName::Update;

    async fn run(&mut self) -> Result<(), PhaseStateError> {
        let min_time = self.shared.state.update.time.min;
        let max_time = self.shared.state.update.time.max;
        debug!(
            "in update phase for min {} and max {} seconds",
            min_time, max_time,
        );
        self.process_during(Duration::from_secs(min_time)).await?;

        let time_left = max_time - min_time;
        timeout(Duration::from_secs(time_left), self.process_until_enough()).await??;

        info!(
            "in total {} update messages accepted (min {} and max {} required)",
            self.private.accepted,
            self.shared.state.update.count.min,
            self.shared.state.update.count.max,
        );
        info!(
            "in total {} update messages rejected",
            self.private.rejected,
        );
        info!(
            "in total {} update messages discarded",
            self.private.discarded,
        );

        let seed_dict = self
            .shared
            .store
            .seed_dict()
            .await
            .map_err(UpdateStateError::FetchSeedDict)?
            .ok_or(UpdateStateError::NoSeedDict)?;

        info!("broadcasting the global seed dictionary");
        self.shared
            .events
            .broadcast_seed_dict(DictionaryUpdate::New(Arc::new(seed_dict)));

        Ok(())
    }

    fn next(self) -> Option<StateMachine<S>> {
        Some(PhaseState::<Sum2, _>::new(self.shared, self.private.model_agg).into())
    }
}

#[async_trait]
impl<S> Handler for PhaseState<Update, S>
where
    S: Storage,
{
    async fn handle_request(&mut self, req: StateMachineRequest) -> Result<(), RequestError> {
        if let StateMachineRequest::Update(UpdateRequest {
            participant_pk,
            local_seed_dict,
            masked_model,
        }) = req
        {
            self.update_seed_dict_and_aggregate_mask(
                &participant_pk,
                &local_seed_dict,
                masked_model,
            )
            .await
        } else {
            Err(RequestError::MessageRejected)
        }
    }

    fn has_enough_messages(&self) -> bool {
        self.private.accepted >= self.shared.state.update.count.min
    }

    fn has_overmuch_messages(&self) -> bool {
        self.private.accepted >= self.shared.state.update.count.max
    }

    fn increment_accepted(&mut self) {
        self.private.accepted += 1;
        debug!(
            "{} update messages accepted (min {} and max {} required)",
            self.private.accepted,
            self.shared.state.update.count.min,
            self.shared.state.update.count.max,
        );
    }

    fn increment_rejected(&mut self) {
        self.private.rejected += 1;
        debug!("{} update messages rejected", self.private.rejected);
    }

    fn increment_discarded(&mut self) {
        self.private.discarded += 1;
        debug!("{} update messages discarded", self.private.discarded);
    }
}

impl<S> PhaseState<Update, S>
where
    S: Storage,
{
    /// Creates a new update state.
    pub fn new(shared: Shared<S>) -> Self {
        Self {
            private: Update {
                model_agg: Aggregation::new(
                    shared.state.round_params.mask_config,
                    shared.state.round_params.model_length,
                ),
                accepted: 0,
                rejected: 0,
                discarded: 0,
            },
            shared,
        }
    }

    /// Updates the local seed dict and aggregates the masked model.
    async fn update_seed_dict_and_aggregate_mask(
        &mut self,
        pk: &UpdateParticipantPublicKey,
        local_seed_dict: &LocalSeedDict,
        mask_object: MaskObject,
    ) -> Result<(), RequestError> {
        // Check if aggregation can be performed. It is important to
        // do that _before_ updating the seed dictionary, because we
        // don't want to add the local seed dict if the corresponding
        // masked model is invalid
        debug!("checking whether the masked model can be aggregated");
        self.private
            .model_agg
            .validate_aggregation(&mask_object)
            .map_err(|e| {
                warn!("model aggregation error: {}", e);
                RequestError::AggregationFailed
            })?;

        // Try to update local seed dict first. If this fail, we do
        // not want to aggregate the model.
        info!("updating the global seed dictionary");
        self.add_local_seed_dict(pk, local_seed_dict)
            .await
            .map_err(|err| {
                warn!("invalid local seed dictionary, ignoring update message");
                err
            })?;

        info!("aggregating the masked model and scalar");
        self.private.model_agg.aggregate(mask_object);
        Ok(())
    }

    /// Adds a local seed dictionary to the seed dictionary.
    ///
    /// # Error
    ///
    /// Fails if the local seed dict cannot be added due to a PET or [`StorageError`].
    async fn add_local_seed_dict(
        &mut self,
        pk: &UpdateParticipantPublicKey,
        local_seed_dict: &LocalSeedDict,
    ) -> Result<(), RequestError> {
        self.shared
            .store
            .add_local_seed_dict(pk, local_seed_dict)
            .await?
            .into_inner()
            .map_err(RequestError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use xaynet_core::{SeedDict, SumDict};

    use crate::{
        state_machine::{
            coordinator::CoordinatorState,
            events::{EventPublisher, EventSubscriber, ModelUpdate},
            tests::{
                utils::{
                    assert_event_updated,
                    enable_logging,
                    init_shared,
                    send_update_messages,
                    EventSnapshot,
                },
                CoordinatorStateBuilder,
                EventBusBuilder,
            },
        },
        storage::{
            tests::{utils::create_global_model, MockCoordinatorStore, MockModelStore},
            LocalSeedDictAdd,
            Store,
        },
    };

    fn events_from_sum_phase(state: &CoordinatorState) -> (EventPublisher, EventSubscriber) {
        EventBusBuilder::new(state)
            .broadcast_phase(PhaseName::Sum)
            .broadcast_sum_dict(DictionaryUpdate::New(Arc::new(SumDict::new())))
            .broadcast_seed_dict(DictionaryUpdate::Invalidate)
            .broadcast_model(ModelUpdate::New(Arc::new(create_global_model(10))))
            .build()
    }

    fn assert_after_phase_success(
        state_before: &CoordinatorState,
        events_before: &EventSnapshot,
        state_after: &CoordinatorState,
        events_after: &EventSnapshot,
    ) {
        assert_eq!(state_after, state_before);

        assert_event_updated(&events_after.phase, &events_before.phase);
        assert_event_updated(&events_after.seed_dict, &events_before.seed_dict);
        assert_eq!(events_after.keys, events_before.keys);
        assert_eq!(events_after.params, events_before.params);
        assert_eq!(events_after.phase.event, PhaseName::Update);
        assert_eq!(events_after.sum_dict, events_before.sum_dict);
        assert_eq!(events_after.model, events_before.model);
    }

    fn assert_after_phase_failure(
        state_before: &CoordinatorState,
        events_before: &EventSnapshot,
        state_after: &CoordinatorState,
        events_after: &EventSnapshot,
    ) {
        assert_eq!(state_after, state_before);

        assert_event_updated(&events_after.phase, &events_before.phase);
        assert_eq!(events_after.keys, events_before.keys);
        assert_eq!(events_after.params, events_before.params);
        assert_eq!(events_after.phase.event, PhaseName::Update);
        assert_eq!(events_after.sum_dict, events_before.sum_dict);
        assert_eq!(events_after.seed_dict, events_before.seed_dict);
        assert_eq!(events_after.model, events_before.model);
    }

    #[tokio::test]
    async fn test_update_to_sum2_phase() {
        // No Storage errors
        // lets pretend we come from the sum phase
        //
        // What should happen:
        // 1. broadcast Update phase
        // 2. accept 10 update messages
        // 3. fetch seed dict
        // 4. broadcast seed dict
        // 5. move into sum2 phase
        //
        // What should not happen:
        // - the shared state has been changed
        // - the global model has been invalidated
        // - the sum dict has been invalidated
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_add_local_seed_dict()
            .times(10)
            .returning(move |_, _| Ok(LocalSeedDictAdd(Ok(()))));
        cs.expect_seed_dict()
            .return_once(move || Ok(Some(SeedDict::new())));
        let store = Store::new(cs, MockModelStore::new());
        let state = CoordinatorStateBuilder::new()
            .with_round_id(1)
            .with_update_count_min(10)
            .with_update_count_max(10)
            .with_update_time_min(1)
            .build();

        let (event_publisher, event_subscriber) = events_from_sum_phase(&state);
        let events_before_update = EventSnapshot::from(&event_subscriber);
        let state_before_update = state.clone();

        let (shared, request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<Update, _>::new(shared));
        assert!(state_machine.is_update());

        send_update_messages(10, request_tx.clone());

        let state_machine = state_machine.next().await.unwrap();

        let state_after_update = state_machine.shared_state_as_ref().clone();
        let events_after_update = EventSnapshot::from(&event_subscriber);
        assert_after_phase_success(
            &state_before_update,
            &events_before_update,
            &state_after_update,
            &events_after_update,
        );

        assert!(state_machine.is_sum2());
    }

    #[tokio::test]
    async fn test_update_to_sum2_fetch_seed_dict_failed() {
        // Storage errors
        // - seed_dict fails
        //
        // What should happen:
        // 1. broadcast Update phase
        // 2. accept 1 update message
        // 3. fetch seed dict (fails)
        // 4. move into error phase
        //
        // What should not happen:
        // - the shared state has been changed
        // - the global model has been invalidated
        // - the sum dict has been invalidated
        // - the seed dict has been broadcasted
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_add_local_seed_dict()
            .times(1)
            .returning(move |_, _| Ok(LocalSeedDictAdd(Ok(()))));
        cs.expect_seed_dict().return_once(move || Err(anyhow!("")));
        let store = Store::new(cs, MockModelStore::new());
        let state = CoordinatorStateBuilder::new()
            .with_round_id(1)
            .with_update_count_min(1)
            .with_update_count_max(1)
            .with_update_time_min(1)
            .with_update_time_max(5)
            .build();

        let (event_publisher, event_subscriber) = events_from_sum_phase(&state);
        let events_before_update = EventSnapshot::from(&event_subscriber);
        let state_before_update = state.clone();

        let (shared, request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<Update, _>::new(shared));
        assert!(state_machine.is_update());

        send_update_messages(1, request_tx.clone());
        let state_machine = state_machine.next().await.unwrap();

        let state_after_update = state_machine.shared_state_as_ref().clone();
        let events_after_update = EventSnapshot::from(&event_subscriber);
        assert_after_phase_failure(
            &state_before_update,
            &events_before_update,
            &state_after_update,
            &events_after_update,
        );

        assert!(state_machine.is_error());
        assert!(matches!(
            state_machine.into_error_phase_state().private,
            PhaseStateError::Update(UpdateStateError::FetchSeedDict(_))
        ))
    }

    #[tokio::test]
    async fn test_update_to_sum2_seed_dict_none() {
        // No Storage errors
        //
        // What should happen:
        // 1. broadcast Update phase
        // 2. accept 1 update message
        // 3. fetch seed dict (no storage error but the seed dict is None)
        // 4. move into error phase
        //
        // What should not happen:
        // - the shared state has been changed
        // - the global model has been invalidated
        // - the sum dict has been invalidated
        // - the seed dict has been broadcasted
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_add_local_seed_dict()
            .times(1)
            .returning(move |_, _| Ok(LocalSeedDictAdd(Ok(()))));
        cs.expect_seed_dict().return_once(move || Ok(None));
        let store = Store::new(cs, MockModelStore::new());
        let state = CoordinatorStateBuilder::new()
            .with_round_id(1)
            .with_update_count_min(1)
            .with_update_count_max(1)
            .with_update_time_min(1)
            .with_update_time_max(5)
            .build();

        let (event_publisher, event_subscriber) = events_from_sum_phase(&state);
        let events_before_update = EventSnapshot::from(&event_subscriber);
        let state_before_update = state.clone();

        let (shared, request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<Update, _>::new(shared));
        assert!(state_machine.is_update());

        send_update_messages(1, request_tx.clone());
        let state_machine = state_machine.next().await.unwrap();

        let state_after_update = state_machine.shared_state_as_ref().clone();
        let events_after_update = EventSnapshot::from(&event_subscriber);
        assert_after_phase_failure(
            &state_before_update,
            &events_before_update,
            &state_after_update,
            &events_after_update,
        );

        assert!(state_machine.is_error());
        assert!(matches!(
            state_machine.into_error_phase_state().private,
            PhaseStateError::Update(UpdateStateError::NoSeedDict)
        ))
    }
}

// update message (size of mask)
