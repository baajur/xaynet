use async_trait::async_trait;

use crate::{
    state_machine::{
        phases::{Phase, PhaseName, PhaseState, Shared},
        PhaseStateError,
        StateMachine,
    },
    storage::Storage,
};

/// Shutdown state
#[derive(Debug)]
pub struct Shutdown;

#[async_trait]
impl<S> Phase<S> for PhaseState<Shutdown, S>
where
    S: Storage,
{
    const NAME: PhaseName = PhaseName::Shutdown;

    async fn run(&mut self) -> Result<(), PhaseStateError> {
        // clear the request channel
        self.shared.request_rx.close();
        while self.shared.request_rx.recv().await.is_some() {}
        Ok(())
    }

    fn next(self) -> Option<StateMachine<S>> {
        None
    }
}

impl<S> PhaseState<Shutdown, S>
where
    S: Storage,
{
    /// Creates a new shutdown state.
    pub fn new(shared: Shared<S>) -> Self {
        Self {
            private: Shutdown,
            shared,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        state_machine::tests::{
            utils::{enable_logging, init_shared},
            CoordinatorStateBuilder,
            EventBusBuilder,
        },
        storage::{
            tests::{MockCoordinatorStore, MockModelStore},
            Store,
        },
    };

    #[tokio::test]
    async fn test_shutdown_to_none() {
        // No Storage errors
        //
        // What should happen:
        // 1. broadcast Shutdown phase
        // 1. request channel is closed
        // 2. state machine is stopped
        //
        // What should not happen:
        // - events have been broadcasted (except phase event)
        enable_logging();
        let store = Store::new(MockCoordinatorStore::new(), MockModelStore::new());

        let state = CoordinatorStateBuilder::new().build();
        let (event_publisher, _event_subscriber) = EventBusBuilder::new(&state).build();
        let (shared, request_tx) = init_shared(state, store, event_publisher);
        let state_machine = StateMachine::from(PhaseState::<Shutdown, _>::new(shared));

        assert!(state_machine.is_shutdown());

        assert!(!request_tx.is_closed());

        let state_machine = state_machine.next().await;

        assert!(request_tx.is_closed());
        assert!(state_machine.is_none());
    }
}
