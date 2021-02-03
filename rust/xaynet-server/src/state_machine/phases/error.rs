use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{error, info};

use crate::{
    event,
    state_machine::{
        phases::{
            idle::IdleStateError,
            sum::SumStateError,
            unmask::UnmaskStateError,
            update::UpdateStateError,
            Idle,
            Phase,
            PhaseName,
            PhaseState,
            Shared,
            Shutdown,
        },
        StateMachine,
    },
    storage::Storage,
};

/// Error that can occur during the execution of the [`StateMachine`].
#[derive(Error, Debug)]
pub enum PhaseStateError {
    #[error("request channel error: {0}")]
    RequestChannel(&'static str),

    #[error("phase timeout")]
    PhaseTimeout(#[from] tokio::time::error::Elapsed),

    #[error("idle phase failed: {0}")]
    Idle(#[from] IdleStateError),

    #[error("sum phase failed: {0}")]
    Sum(#[from] SumStateError),

    #[error("update phase failed: {0}")]
    Update(#[from] UpdateStateError),

    #[error("unmask phase failed: {0}")]
    Unmask(#[from] UnmaskStateError),
}

impl<S> PhaseState<PhaseStateError, S>
where
    S: Storage,
{
    /// Creates a new error phase.
    pub fn new(shared: Shared<S>, error: PhaseStateError) -> Self {
        Self {
            private: error,
            shared,
        }
    }

    /// Waits until the [`crate::storage::Store`] is ready.
    async fn wait_for_store_readiness(&mut self) {
        while let Err(err) = <S as Storage>::is_ready(&mut self.shared.store).await {
            error!("store not ready: {}", err);
            info!("try again in 5 sec");
            sleep(Duration::from_secs(5)).await;
        }
    }
}

#[async_trait]
impl<S> Phase<S> for PhaseState<PhaseStateError, S>
where
    S: Storage,
{
    const NAME: PhaseName = PhaseName::Error;

    async fn run(&mut self) -> Result<(), PhaseStateError> {
        error!("phase state error: {}", self.private);

        event!("Phase error", self.private.to_string());

        match self.private {
            // skip store readiness check if the state machine
            // moves into the shutdown phase
            PhaseStateError::RequestChannel(_) => (),
            _ => self.wait_for_store_readiness().await,
        }

        Ok(())
    }

    fn next(self) -> Option<StateMachine<S>> {
        Some(match self.private {
            PhaseStateError::RequestChannel(_) => {
                PhaseState::<Shutdown, _>::new(self.shared).into()
            }
            _ => PhaseState::<Idle, _>::new(self.shared).into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use tokio::time::{timeout, Duration, Instant};

    use crate::{
        state_machine::tests::{
            utils::{enable_logging, init_shared, EventSnapshot},
            CoordinatorStateBuilder,
            EventBusBuilder,
        },
        storage::{
            tests::{MockCoordinatorStore, MockModelStore},
            Store,
        },
    };

    #[tokio::test]
    async fn error_to_idle_phase() {
        // TODO: The state machine could stay in this phase for a long
        // time if there was a major storage problem.
        // We should decide whether we can invalidate the dicts as
        // soon as the state machine is in the error phase. Every
        // sum/update client would otherwise create unnecessary
        // messages that can no longer be processed.
        //
        // No Storage errors
        //
        // What should happen:
        // 1. broadcast Error phase
        // 2. check if store is ready to process requests
        // 3. move into idle phase
        //
        // What should not happen:
        // - the shared state has been changed
        //   (except for`round_id` when moving into idle phase)
        // - events have been broadcasted (except phase event)
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_is_ready().return_once(move || Ok(()));

        let mut ms = MockModelStore::new();
        ms.expect_is_ready().return_once(move || Ok(()));

        let store = Store::new(cs, ms);

        let state = CoordinatorStateBuilder::new().build();
        let (event_publisher, event_subscriber) = EventBusBuilder::new(&state).build();
        let events_before_error = EventSnapshot::from(&event_subscriber);
        let state_before_error = state.clone();

        let (shared, _request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<PhaseStateError, _>::new(
            shared,
            PhaseStateError::Idle(IdleStateError::DeleteDictionaries(anyhow!(""))),
        ));
        assert!(state_machine.is_error());

        let state_machine = state_machine.next().await.unwrap();

        let state_after_error = state_machine.shared_state_as_ref().clone();

        // round id is updated in idle phase
        assert_ne!(state_after_error.round_id, state_before_error.round_id);
        assert_eq!(
            state_after_error.round_params,
            state_before_error.round_params
        );
        assert_eq!(state_after_error.keys, state_before_error.keys);
        assert_eq!(state_after_error.sum, state_before_error.sum);
        assert_eq!(state_after_error.update, state_before_error.update);
        assert_eq!(state_after_error.sum2, state_before_error.sum2);

        let events_after_error = EventSnapshot::from(&event_subscriber);
        assert_ne!(events_after_error.phase, events_before_error.phase);
        assert_eq!(events_after_error.keys, events_before_error.keys);
        assert_eq!(events_after_error.params, events_before_error.params);
        assert_eq!(events_after_error.sum_dict, events_before_error.sum_dict);
        assert_eq!(events_after_error.seed_dict, events_before_error.seed_dict);
        assert_eq!(events_after_error.model, events_before_error.model);
        assert_eq!(events_after_error.phase.event, PhaseName::Error);

        assert!(state_machine.is_idle());
    }

    #[tokio::test]
    async fn test_error_to_shutdown_phase() {
        // No Storage errors
        //
        // What should happen:
        // 1. broadcast Error phase
        // 2. previous phase failed with PhaseStateError::RequestChannel
        //    which means that the state machine should be shut down
        // 3. move into shutdown phase
        //
        // What should not happen:
        // - the shared state has been changed
        // - events have been broadcasted (except phase event)
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        cs.expect_is_ready().return_once(move || Ok(()));

        let mut ms = MockModelStore::new();
        ms.expect_is_ready().return_once(move || Ok(()));

        let store = Store::new(cs, ms);

        let state = CoordinatorStateBuilder::new().build();
        let (event_publisher, event_subscriber) = EventBusBuilder::new(&state).build();
        let events_before_error = EventSnapshot::from(&event_subscriber);
        let state_before_error = state.clone();

        let (shared, _request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<PhaseStateError, _>::new(
            shared,
            PhaseStateError::RequestChannel(""),
        ));
        assert!(state_machine.is_error());

        let state_machine = state_machine.next().await.unwrap();

        let state_after_error = state_machine.shared_state_as_ref().clone();

        assert_eq!(state_after_error, state_before_error);

        let events_after_error = EventSnapshot::from(&event_subscriber);
        assert_ne!(events_after_error.phase, events_before_error.phase);
        assert_eq!(events_after_error.keys, events_before_error.keys);
        assert_eq!(events_after_error.params, events_before_error.params);
        assert_eq!(events_after_error.sum_dict, events_before_error.sum_dict);
        assert_eq!(events_after_error.seed_dict, events_before_error.seed_dict);
        assert_eq!(events_after_error.model, events_before_error.model);
        assert_eq!(events_after_error.phase.event, PhaseName::Error);

        assert!(state_machine.is_shutdown());
    }

    #[tokio::test]
    async fn test_error_to_idle_store_failed() {
        // Storage error:
        // - first call on `is_ready` the coordinator store and model store fails
        // - second call on `is_ready` the coordinator store fails and model store passes
        // - third call on `is_ready` the coordinator store passes and model store fails
        // - forth call on `is_ready` the coordinator store and model store passes
        //
        // What should happen:
        // 1. broadcast Error phase
        // 2. check if store is ready to process requests
        // 3. wait until store is ready again (15 sec)
        // 4. move into idle phase
        //
        // What should not happen:
        // - the shared state has been changed
        //   (except for`round_id` when moving into idle phase)
        // - events have been broadcasted (except phase event)
        enable_logging();

        let mut cs = MockCoordinatorStore::new();
        let mut cs_counter = 0;
        cs.expect_is_ready().returning(move || {
            let res = match cs_counter {
                0 => Err(anyhow!("")),
                1 => Err(anyhow!("")),
                2 => Ok(()),
                3 => Ok(()),
                _ => panic!(""),
            };
            cs_counter += 1;
            res
        });

        let mut ms = MockModelStore::new();
        let mut ms_counter = 0;
        ms.expect_is_ready().returning(move || {
            let res = match ms_counter {
                // we skip step 1 and 2 because Storage::is_ready does not call
                // MockModelStore::is_ready if MockCoordinatorStore::is_ready
                // has already failed
                0 => Err(anyhow!("")),
                1 => Ok(()),
                _ => panic!(""),
            };
            ms_counter += 1;
            res
        });

        let store = Store::new(cs, ms);

        let state = CoordinatorStateBuilder::new().build();
        let (event_publisher, _event_subscriber) = EventBusBuilder::new(&state).build();
        let (shared, _request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<PhaseStateError, _>::new(
            shared,
            PhaseStateError::Idle(IdleStateError::DeleteDictionaries(anyhow!(""))),
        ));

        assert!(state_machine.is_error());

        let now = Instant::now();

        let state_machine = timeout(Duration::from_secs(20), state_machine.next())
            .await
            .unwrap()
            .unwrap();

        assert!(now.elapsed().as_secs() > 14);

        assert!(state_machine.is_idle());
    }

    #[tokio::test]
    async fn test_error_to_shutdown_skip_store_readiness_check() {
        // Storage error:
        //
        // What should happen:
        // 1. broadcast Error phase
        // 2. previous phase failed with PhaseStateError::RequestChannel
        //    which means that the state machine should be shut down
        // 3. skip store readiness check
        // 4. move into shutdown phase
        //
        // What should not happen:
        // - wait for the store to be ready again
        // - the shared state has been changed
        // - events have been broadcasted (except phase event)
        enable_logging();

        let store = Store::new(MockCoordinatorStore::new(), MockModelStore::new());

        let state = CoordinatorStateBuilder::new().build();
        let (event_publisher, _event_subscriber) = EventBusBuilder::new(&state).build();
        let (shared, _request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<PhaseStateError, _>::new(
            shared,
            PhaseStateError::RequestChannel(""),
        ));

        assert!(state_machine.is_error());

        let state_machine = timeout(Duration::from_secs(5), state_machine.next())
            .await
            .unwrap()
            .unwrap();

        assert!(state_machine.is_shutdown());
    }
}
