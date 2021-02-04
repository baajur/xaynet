use async_trait::async_trait;

use crate::{
    impl_phase_method_for_phasestate,
    impl_phase_purge_for_phasestate,
    state_machine::{
        phases::{Phase, PhaseName, PhaseState, Shared},
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

    impl_phase_method_for_phasestate! {
        async fn process(self_: &mut PhaseState<Shutdown, S>) -> Result<(), PhaseStateError> {
            // clear the request channel
            self_.shared.request_rx.close();
            while self_.shared.request_rx.recv().await.is_some() {}

            Ok(())
        }
    }

    impl_phase_purge_for_phasestate! { Shutdown, S }

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
