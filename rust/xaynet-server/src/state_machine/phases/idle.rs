use async_trait::async_trait;
use sodiumoxide::crypto::hash::sha256;
use thiserror::Error;
use tracing::{debug, info};
use xaynet_core::{
    common::RoundSeed,
    crypto::{ByteObject, EncryptKeyPair, SigningKeySeed},
};

use crate::{
    metric,
    metrics::Measurement,
    state_machine::{
        events::DictionaryUpdate,
        phases::{Phase, PhaseName, PhaseState, Shared, Sum},
        PhaseStateError,
        StateMachine,
    },
    storage::{Storage, StorageError},
};

/// Error that occurs during the idle phase.
#[derive(Error, Debug)]
pub enum IdleStateError {
    #[error("setting the coordinator state failed: {0}")]
    SetCoordinatorState(StorageError),
    #[error("deleting the dictionaries failed: {0}")]
    DeleteDictionaries(StorageError),
}

/// Idle state
#[derive(Debug)]
pub struct Idle;

#[async_trait]
impl<S> Phase<S> for PhaseState<Idle, S>
where
    S: Storage,
{
    const NAME: PhaseName = PhaseName::Idle;

    async fn run(&mut self) -> Result<(), PhaseStateError> {
        info!("updating the keys");
        self.gen_round_keypair();

        info!("updating round thresholds");
        self.update_round_thresholds();

        info!("updating round seeds");
        self.update_round_seed();

        let events = &mut self.shared.events;

        info!("broadcasting new keys");
        events.broadcast_keys(self.shared.state.keys.clone());

        info!("broadcasting invalidation of sum dictionary from previous round");
        events.broadcast_sum_dict(DictionaryUpdate::Invalidate);

        info!("broadcasting invalidation of seed dictionary from previous round");
        events.broadcast_seed_dict(DictionaryUpdate::Invalidate);

        self.shared
            .store
            .set_coordinator_state(&self.shared.state)
            .await
            .map_err(IdleStateError::SetCoordinatorState)?;

        self.shared
            .store
            .delete_dicts()
            .await
            .map_err(IdleStateError::DeleteDictionaries)?;

        info!("broadcasting new round parameters");
        events.broadcast_params(self.shared.state.round_params.clone());

        metric!(Measurement::RoundTotalNumber, self.shared.state.round_id);
        metric!(
            Measurement::RoundParamSum,
            self.shared.state.round_params.sum,
            ("round_id", self.shared.state.round_id),
            ("phase", Self::NAME as u8),
        );
        metric!(
            Measurement::RoundParamUpdate,
            self.shared.state.round_params.update,
            ("round_id", self.shared.state.round_id),
            ("phase", Self::NAME as u8),
        );

        Ok(())
    }

    fn next(self) -> Option<StateMachine<S>> {
        Some(PhaseState::<Sum, _>::new(self.shared).into())
    }
}

impl<S> PhaseState<Idle, S>
where
    S: Storage,
{
    /// Creates a new idle state.
    pub fn new(mut shared: Shared<S>) -> Self {
        // Since some events are emitted very early, the round id must
        // be correct when the idle phase starts. Therefore, we update
        // it here, when instantiating the idle PhaseState.
        shared.set_round_id(shared.round_id() + 1);
        debug!("new round ID = {}", shared.round_id());
        Self {
            private: Idle,
            shared,
        }
    }

    fn update_round_thresholds(&mut self) {}

    /// Updates the seed round parameter.
    fn update_round_seed(&mut self) {
        // Safe unwrap: `sk` and `seed` have same number of bytes
        let (_, sk) =
            SigningKeySeed::from_slice_unchecked(self.shared.state.keys.secret.as_slice())
                .derive_signing_key_pair();
        let signature = sk.sign_detached(
            &[
                self.shared.state.round_params.seed.as_slice(),
                &self.shared.state.round_params.sum.to_le_bytes(),
                &self.shared.state.round_params.update.to_le_bytes(),
            ]
            .concat(),
        );
        // Safe unwrap: the length of the hash is 32 bytes
        self.shared.state.round_params.seed =
            RoundSeed::from_slice_unchecked(sha256::hash(signature.as_slice()).as_ref());
    }

    /// Generates fresh round credentials.
    fn gen_round_keypair(&mut self) {
        self.shared.state.keys = EncryptKeyPair::generate();
        self.shared.state.round_params.pk = self.shared.state.keys.public;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use anyhow::anyhow;
    use xaynet_core::{common::RoundParameters, SeedDict, SumDict};

    use crate::{
        state_machine::{
            coordinator::CoordinatorState,
            events::{EventPublisher, EventSubscriber, ModelUpdate},
            tests::{
                utils::{assert_event_updated_with_id, enable_logging, init_shared, EventSnapshot},
                CoordinatorStateBuilder,
                EventBusBuilder,
            },
        },
        storage::{
            tests::{utils::create_global_model, MockCoordinatorStore, MockModelStore},
            Store,
        },
    };

    fn state_and_events_from_unmask_phase() -> (CoordinatorState, EventPublisher, EventSubscriber) {
        let state = CoordinatorStateBuilder::new().build();

        let (event_publisher, event_subscriber) = EventBusBuilder::new(&state)
            .broadcast_phase(PhaseName::Unmask)
            .broadcast_sum_dict(DictionaryUpdate::New(Arc::new(SumDict::new())))
            .broadcast_seed_dict(DictionaryUpdate::New(Arc::new(SeedDict::new())))
            .broadcast_model(ModelUpdate::New(Arc::new(create_global_model(10))))
            .build();

        (state, event_publisher, event_subscriber)
    }

    fn assert_params(params1: &RoundParameters, params2: &RoundParameters) {
        assert_ne!(params1.pk, params2.pk);
        assert_ne!(params1.seed, params2.seed);
        assert!((params1.sum - params2.sum).abs() < f64::EPSILON);
        assert!((params1.update - params2.update).abs() < f64::EPSILON);
        assert_eq!(params1.mask_config, params2.mask_config);
        assert_eq!(params1.model_length, params2.model_length);
    }

    fn assert_after_phase_failure(
        state_before: &CoordinatorState,
        events_before: &EventSnapshot,
        state_after: &CoordinatorState,
        events_after: &EventSnapshot,
    ) {
        assert_params(&state_after.round_params, &state_before.round_params);
        assert_ne!(state_after.keys, state_before.keys);
        assert_ne!(state_after.round_id, state_before.round_id);
        assert_eq!(state_after.sum, state_before.sum);
        assert_eq!(state_after.update, state_before.update);
        assert_eq!(state_after.sum2, state_before.sum2);
        assert_eq!(state_after.keys.public, state_after.round_params.pk);
        assert_eq!(state_after.round_id, 1);

        assert_event_updated_with_id(&events_after.keys, &events_before.keys);
        assert_event_updated_with_id(&events_after.phase, &events_before.phase);
        assert_event_updated_with_id(&events_after.sum_dict, &events_before.sum_dict);
        assert_event_updated_with_id(&events_after.seed_dict, &events_before.seed_dict);
        assert_eq!(events_after.phase.event, PhaseName::Idle);
        assert_eq!(events_after.params, events_before.params);
        assert_eq!(events_after.model, events_before.model);
    }

    #[tokio::test]
    async fn test_idle_to_sum_phase() {
        // No Storage errors
        // lets pretend we come from the unmask phase
        //
        // What should happen:
        // 1. increase round id by 1
        // 2. broadcast Idle phase
        // 3. update coordinator keys
        // 4. update round thresholds (not implemented yet)
        // 5. update round seeds
        // 6. broadcast updated keys
        // 7. broadcast invalidation of sum dictionary from previous round
        // 8. broadcast invalidation of seed dictionary from previous round
        // 9. save the new coordinator state
        // 10. delete the sum/seed/mask dict
        // 11. broadcast new round parameters
        // 12. move into sum phase
        //
        // What should not happen:
        // - the global model has been invalidated
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_set_coordinator_state()
            .return_once(move |_| Ok(()));
        cs.expect_delete_dicts().return_once(move || Ok(()));
        let store = Store::new(cs, MockModelStore::new());

        let (state, event_publisher, event_subscriber) = state_and_events_from_unmask_phase();
        let events_before_idle = EventSnapshot::from(&event_subscriber);
        let state_before_idle = state.clone();

        let (shared, _request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<Idle, _>::new(shared));
        assert!(state_machine.is_idle());

        let state_machine = state_machine.next().await.unwrap();

        let state_after_idle = state_machine.shared_state_as_ref().clone();
        assert_params(
            &state_after_idle.round_params,
            &state_before_idle.round_params,
        );
        assert_ne!(state_after_idle.keys, state_before_idle.keys);
        assert_ne!(state_after_idle.round_id, state_before_idle.round_id);
        assert_eq!(state_after_idle.sum, state_before_idle.sum);
        assert_eq!(state_after_idle.update, state_before_idle.update);
        assert_eq!(state_after_idle.sum2, state_before_idle.sum2);
        assert_eq!(
            state_after_idle.keys.public,
            state_after_idle.round_params.pk
        );
        assert_eq!(state_after_idle.round_id, 1);

        let events_after_idle = EventSnapshot::from(&event_subscriber);
        assert_event_updated_with_id(&events_after_idle.keys, &events_before_idle.keys);
        assert_event_updated_with_id(&events_after_idle.params, &events_before_idle.params);
        assert_event_updated_with_id(&events_after_idle.phase, &events_before_idle.phase);
        assert_event_updated_with_id(&events_after_idle.sum_dict, &events_before_idle.sum_dict);
        assert_event_updated_with_id(&events_after_idle.seed_dict, &events_before_idle.seed_dict);
        assert_eq!(events_after_idle.phase.event, PhaseName::Idle);
        assert_eq!(events_after_idle.model, events_before_idle.model);

        assert!(state_machine.is_sum());
    }

    #[tokio::test]
    async fn test_idle_to_sum_save_state_failed() {
        // Storage:
        // - set_coordinator_state fails
        //
        // What should happen:
        // 1. increase round id by 1
        // 2. broadcast Idle phase
        // 3. update coordinator keys
        // 4. update round thresholds (not implemented yet)
        // 5. update round seeds
        // 6. broadcast updated keys
        // 7. broadcast invalidation of sum dictionary from previous round
        // 8. broadcast invalidation of seed dictionary from previous round
        // 9. save the new coordinator state (fails)
        // 10. move into error phase
        //
        // What should not happen:
        // - the sum/seed/mask dict have been deleted
        // - new round parameters have been broadcast
        // - the global model has been invalidated
        // - the state machine has moved into sum phase
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_set_coordinator_state()
            .return_once(move |_| Err(anyhow!("")));
        let store = Store::new(cs, MockModelStore::new());

        let (state, event_publisher, event_subscriber) = state_and_events_from_unmask_phase();
        let events_before_idle = EventSnapshot::from(&event_subscriber);
        let state_before_idle = state.clone();

        let (shared, _request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<Idle, _>::new(shared));
        assert!(state_machine.is_idle());

        let state_machine = state_machine.next().await.unwrap();

        let state_after_idle = state_machine.shared_state_as_ref().clone();
        let events_after_idle = EventSnapshot::from(&event_subscriber);
        assert_after_phase_failure(
            &state_before_idle,
            &events_before_idle,
            &state_after_idle,
            &events_after_idle,
        );

        assert!(state_machine.is_error());
        assert!(matches!(
            state_machine.into_error_phase_state().private,
            PhaseStateError::Idle(IdleStateError::SetCoordinatorState(_))
        ))
    }

    #[tokio::test]
    async fn test_idle_to_sum_delete_dicts_failed() {
        // Storage:
        // - delete_dicts fails
        //
        // What should happen:
        // 1. increase round id by 1
        // 2. broadcast Idle phase
        // 3. update coordinator keys
        // 4. update round thresholds (not implemented yet)
        // 5. update round seeds
        // 6. broadcast updated keys
        // 7. broadcast invalidation of sum dictionary from previous round
        // 8. broadcast invalidation of seed dictionary from previous round
        // 9. save the new coordinator state
        // 10. delete the sum/seed/mask dict (fails)
        // 11. move into error phase
        //
        // What should not happen:
        // - new round parameters have been broadcast
        // - the global model has been invalidated
        // - the state machine has moved into sum phase
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_set_coordinator_state()
            .return_once(move |_| Ok(()));
        cs.expect_delete_dicts()
            .return_once(move || Err(anyhow!("")));
        let store = Store::new(cs, MockModelStore::new());

        let (state, event_publisher, event_subscriber) = state_and_events_from_unmask_phase();
        let events_before_idle = EventSnapshot::from(&event_subscriber);
        let state_before_idle = state.clone();

        let (shared, _request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<Idle, _>::new(shared));
        assert!(state_machine.is_idle());

        let state_machine = state_machine.next().await.unwrap();

        let state_after_idle = state_machine.shared_state_as_ref().clone();
        let events_after_idle = EventSnapshot::from(&event_subscriber);
        assert_after_phase_failure(
            &state_before_idle,
            &events_before_idle,
            &state_after_idle,
            &events_after_idle,
        );

        assert!(state_machine.is_error());
        assert!(matches!(
            state_machine.into_error_phase_state().private,
            PhaseStateError::Idle(IdleStateError::DeleteDictionaries(_))
        ))
    }
}
