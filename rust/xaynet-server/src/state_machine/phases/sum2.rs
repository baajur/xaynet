use async_trait::async_trait;
use tokio::time::{timeout, Duration};
use tracing::{debug, info};
use xaynet_core::{
    mask::{Aggregation, MaskObject},
    SumParticipantPublicKey,
};

use crate::{
    state_machine::{
        phases::{Handler, Phase, PhaseName, PhaseState, PhaseStateError, Shared, Unmask},
        requests::{StateMachineRequest, Sum2Request},
        RequestError,
        StateMachine,
    },
    storage::Storage,
};

/// The sum2 state.
#[derive(Debug)]
pub struct Sum2 {
    /// The aggregator for masked models.
    model_agg: Aggregation,
    /// The number of sum2 messages successfully processed.
    accepted: u64,
    /// The number of sum2 messages failed to processed.
    rejected: u64,
    /// The number of sum2 messages discarded without being processed.
    discarded: u64,
}

#[async_trait]
impl<S> Phase<S> for PhaseState<Sum2, S>
where
    Self: Handler,
    S: Storage,
{
    const NAME: PhaseName = PhaseName::Sum2;

    async fn run(&mut self) -> Result<(), PhaseStateError> {
        let min_time = self.shared.state.sum2.time.min;
        let max_time = self.shared.state.sum2.time.max;
        debug!(
            "in sum2 phase for min {} and max {} seconds",
            min_time, max_time,
        );
        self.process_during(Duration::from_secs(min_time)).await?;

        let time_left = max_time - min_time;
        timeout(Duration::from_secs(time_left), self.process_until_enough()).await??;

        info!(
            "in total {} sum2 messages accepted (min {} and max {} required)",
            self.private.accepted,
            self.shared.state.sum2.count.min,
            self.shared.state.sum2.count.max,
        );
        info!("in total {} sum2 messages rejected", self.private.rejected);
        info!(
            "in total {} sum2 messages discarded",
            self.private.discarded,
        );

        Ok(())
    }

    /// Moves from the sum2 state to the next state.
    ///
    /// See the [module level documentation] for more details.
    ///
    /// [module level documentation]: crate::state_machine
    fn next(self) -> Option<StateMachine<S>> {
        Some(PhaseState::<Unmask, _>::new(self.shared, self.private.model_agg).into())
    }
}

#[async_trait]
impl<S> Handler for PhaseState<Sum2, S>
where
    S: Storage,
{
    async fn handle_request(&mut self, req: StateMachineRequest) -> Result<(), RequestError> {
        if let StateMachineRequest::Sum2(Sum2Request {
            participant_pk,
            model_mask,
        }) = req
        {
            self.update_mask_dict(participant_pk, model_mask).await
        } else {
            Err(RequestError::MessageRejected)
        }
    }

    fn has_enough_messages(&self) -> bool {
        self.private.accepted >= self.shared.state.sum2.count.min
    }

    fn has_overmuch_messages(&self) -> bool {
        self.private.accepted >= self.shared.state.sum2.count.max
    }

    fn increment_accepted(&mut self) {
        self.private.accepted += 1;
        debug!(
            "{} sum2 messages accepted (min {} and max {} required)",
            self.private.accepted,
            self.shared.state.sum2.count.min,
            self.shared.state.sum2.count.max,
        );
    }

    fn increment_rejected(&mut self) {
        self.private.rejected += 1;
        debug!("{} sum2 messages rejected", self.private.rejected);
    }

    fn increment_discarded(&mut self) {
        self.private.discarded += 1;
        debug!("{} sum2 messages discarded", self.private.discarded);
    }
}

impl<S> PhaseState<Sum2, S>
where
    S: Storage,
{
    /// Creates a new sum2 state.
    pub fn new(shared: Shared<S>, model_agg: Aggregation) -> Self {
        Self {
            private: Sum2 {
                model_agg,
                accepted: 0,
                rejected: 0,
                discarded: 0,
            },
            shared,
        }
    }

    /// Updates the mask dict with a sum2 participant request.
    async fn update_mask_dict(
        &mut self,
        participant_pk: SumParticipantPublicKey,
        model_mask: MaskObject,
    ) -> Result<(), RequestError> {
        self.shared
            .store
            .incr_mask_score(&participant_pk, &model_mask)
            .await?
            .into_inner()
            .map_err(RequestError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use xaynet_core::{SeedDict, SumDict};

    use crate::{
        state_machine::{
            coordinator::CoordinatorState,
            events::{DictionaryUpdate, EventPublisher, EventSubscriber, ModelUpdate},
            tests::{
                utils::{
                    assert_event_updated,
                    enable_logging,
                    init_shared,
                    send_sum2_messages,
                    EventSnapshot,
                },
                CoordinatorStateBuilder,
                EventBusBuilder,
            },
        },
        storage::{
            tests::{utils::create_global_model, MockCoordinatorStore, MockModelStore},
            MaskScoreIncr,
            Store,
        },
    };

    fn events_from_sum_phase(state: &CoordinatorState) -> (EventPublisher, EventSubscriber) {
        EventBusBuilder::new(state)
            .broadcast_phase(PhaseName::Update)
            .broadcast_sum_dict(DictionaryUpdate::New(Arc::new(SumDict::new())))
            .broadcast_seed_dict(DictionaryUpdate::New(Arc::new(SeedDict::new())))
            .broadcast_model(ModelUpdate::New(Arc::new(create_global_model(10))))
            .build()
    }

    fn assert_after_phase(
        state_before: &CoordinatorState,
        events_before: &EventSnapshot,
        state_after: &CoordinatorState,
        events_after: &EventSnapshot,
    ) {
        assert_eq!(state_after, state_before);

        assert_event_updated(&events_after.phase, &events_before.phase);
        assert_eq!(events_after.keys, events_before.keys);
        assert_eq!(events_after.params, events_before.params);
        assert_eq!(events_after.phase.event, PhaseName::Sum2);
        assert_eq!(events_after.sum_dict, events_before.sum_dict);
        assert_eq!(events_after.seed_dict, events_before.seed_dict);
        assert_eq!(events_after.model, events_before.model);
    }

    #[tokio::test]
    async fn test_sum2_to_unmask_phase() {
        // No Storage errors
        // lets pretend we come from the update phase
        //
        // What should happen:
        // 1. broadcast Sum2 phase
        // 2. accept 10 sum2 messages
        // 3. move into unmask phase
        //
        // What should not happen:
        // - the shared state has been changed
        // - events have been broadcasted (except phase event)
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_incr_mask_score()
            .times(10)
            .returning(move |_, _| Ok(MaskScoreIncr(Ok(()))));

        let store = Store::new(cs, MockModelStore::new());
        let state = CoordinatorStateBuilder::new()
            .with_round_id(1)
            .with_sum2_count_min(10)
            .with_sum2_count_max(10)
            .with_sum2_time_min(1)
            .build();

        let (event_publisher, event_subscriber) = events_from_sum_phase(&state);
        let events_before_sum2 = EventSnapshot::from(&event_subscriber);
        let state_before_sum2 = state.clone();

        let (shared, request_tx) = init_shared(state, store, event_publisher);
        let agg = Aggregation::new(
            state_before_sum2.round_params.mask_config,
            state_before_sum2.round_params.model_length,
        );
        let state_machine = StateMachine::from(PhaseState::<Sum2, _>::new(shared, agg));
        assert!(state_machine.is_sum2());

        send_sum2_messages(10, request_tx.clone());

        let state_machine = state_machine.next().await.unwrap();

        let state_after_sum2 = state_machine.shared_state_as_ref().clone();
        let events_after_sum2 = EventSnapshot::from(&event_subscriber);
        assert_after_phase(
            &state_before_sum2,
            &events_before_sum2,
            &state_after_sum2,
            &events_after_sum2,
        );

        assert!(state_machine.is_unmask());
    }
}
