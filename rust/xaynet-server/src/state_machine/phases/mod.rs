//! This module provides the `PhaseStates` of the [`StateMachine`].

mod error;
mod idle;
mod shutdown;
mod sum;
mod sum2;
mod unmask;
mod update;

use std::fmt;

use async_trait::async_trait;
use derive_more::Display;
use futures::StreamExt;
use tracing::{debug, error, error_span, info, warn, Span};
use tracing_futures::Instrument;

pub use self::{
    error::PhaseStateError,
    idle::{Idle, IdleStateError},
    shutdown::Shutdown,
    sum::{Sum, SumStateError},
    sum2::Sum2,
    unmask::{Unmask, UnmaskStateError},
    update::{Update, UpdateStateError},
};
use crate::{
    impl_phase_method_for_phasestate,
    metric,
    metrics::Measurement,
    state_machine::{
        coordinator::CoordinatorState,
        events::EventPublisher,
        requests::{RequestReceiver, ResponseSender, StateMachineRequest},
        RequestError,
        StateMachine,
    },
    storage::Storage,
};

/// The name of the current phase.
#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum PhaseName {
    #[display(fmt = "Idle")]
    Idle,
    #[display(fmt = "Sum")]
    Sum,
    #[display(fmt = "Update")]
    Update,
    #[display(fmt = "Sum2")]
    Sum2,
    #[display(fmt = "Unmask")]
    Unmask,
    #[display(fmt = "Error")]
    Error,
    #[display(fmt = "Shutdown")]
    Shutdown,
}

/// A trait that must be implemented by a state in order to move to a next state.
///
/// See the [module level documentation] for more details.
///
/// [module level documentation]: crate::state_machine
#[async_trait]
pub trait Phase<S>
where
    S: Storage,
    Self: Sync,
{
    /// The name of the current phase.
    const NAME: PhaseName;

    impl_phase_method_for_phasestate! {
        /// Performs the tasks of this phase.
        async fn process(&mut self_) -> Result<(), PhaseStateError>;
    }
    // NOTE: we have to implement `async fn process()` manually for now (with the help of macros to
    // reduce boilerplate), because the `#[async_trait]` attribute macro has unroll order issues in
    // combination with other macros, see: https://github.com/dtolnay/async-trait/issues/112

    /// Purges the outstanding messages of this phase. Defaults to no purging.
    fn purge(&mut self) -> Result<(), PhaseStateError> {
        Ok(())
    }
    // TODO: add a filter service in PetMessageHandler that only passes through messages if
    // the state machine is in one of the Sum, Update or Sum2 phases. then we can add a Purge
    // phase here which gets broadcasted when the purge starts to prevent further incomming
    // messages, which means we can use the default impl for all phases except Sum, Update and Sum.
    // until then we have to have a purge impl in every phase, which also means that the metrics
    // can be a bit off.

    /// Broadcasts data of this phase. Defaults to no broadcasting.
    async fn broadcast(&mut self) -> Result<(), PhaseStateError> {
        Ok(())
    }

    /// Moves from this state to the next state.
    fn next(self) -> Option<StateMachine<S>>;
}

/// A trait that must be implemented by a state to handle a request.
#[async_trait]
pub trait Handler {
    /// Handles a request.
    ///
    /// # Errors
    /// Fails on PET and storage errors.
    async fn handle_request(&mut self, req: StateMachineRequest) -> Result<(), RequestError>;

    /// Checks whether enough requests have been processed successfully wrt the PET settings.
    fn has_enough_messages(&self) -> bool;

    /// Checks whether too many requests are processed wrt the PET settings.
    fn has_overmuch_messages(&self) -> bool;

    /// Increments the counter for accepted requests.
    fn increment_accepted(&mut self);

    /// Increments the counter for rejected requests.
    fn increment_rejected(&mut self);

    /// Increments the counter for discarded requests.
    fn increment_discarded(&mut self);
}

/// A struct that contains the coordinator state and the I/O interfaces that are shared and
/// accessible by all `PhaseState`s.
pub struct Shared<S>
where
    S: Storage,
{
    /// The coordinator state.
    pub(in crate::state_machine) state: CoordinatorState,
    /// The request receiver half.
    pub(in crate::state_machine) request_rx: RequestReceiver,
    /// The event publisher.
    pub(in crate::state_machine) events: EventPublisher,
    /// The store for storing coordinator and model data.
    pub(in crate::state_machine) store: S,
}

impl<S> fmt::Debug for Shared<S>
where
    S: Storage,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("state", &self.state)
            .field("request_rx", &self.request_rx)
            .field("events", &self.events)
            .finish()
    }
}

impl<S> Shared<S>
where
    S: Storage,
{
    /// Creates a new shared state.
    pub fn new(
        coordinator_state: CoordinatorState,
        publisher: EventPublisher,
        request_rx: RequestReceiver,
        store: S,
    ) -> Self {
        Self {
            state: coordinator_state,
            request_rx,
            events: publisher,
            store,
        }
    }

    /// Sets the round ID to the given value.
    pub fn set_round_id(&mut self, id: u64) {
        self.state.round_id = id;
        self.events.set_round_id(id);
    }

    /// Returns the current round ID.
    pub fn round_id(&self) -> u64 {
        self.state.round_id
    }
}

/// The state corresponding to a phase of the PET protocol.
///
/// This contains the state-dependent `private` state and the state-independent `shared` state
/// which is shared across state transitions.
pub struct PhaseState<S, T>
where
    T: Storage,
{
    /// The private state.
    pub(in crate::state_machine) private: S,
    /// The shared coordinator state and I/O interfaces.
    pub(in crate::state_machine) shared: Shared<T>,
}

/// Implements a [`Phase`] method for [`PhaseState`] with a given method body.
///
/// Hides as much boilerplate as possible, which is necessary due to the `#[async_trait]` issue.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_phase_method_for_phasestate {
    // declaration
    (
        $(#[$attribute: meta])*
        async fn $name: ident (&mut self_) -> Result<(), PhaseStateError>;
    ) => {
        $(#[$attribute])*
        fn $name<'a>(
            &'a mut self,
        ) -> std::pin::Pin<std::boxed::Box<
            dyn std::future::Future<Output =
                std::result::Result<(), crate::state_machine::phases::PhaseStateError>
            > + std::marker::Send + 'a
        >>;
    };

    // implementation
    (
        $(#[$attribute: meta])*
        async fn $name: ident (
            $self_: ident : &mut PhaseState<$phase: ty, $storage: ty> $(,)?
        ) -> Result<(), PhaseStateError>
        $body: block
    ) => {
        $(#[$attribute])*
        fn $name<'a>(
            &'a mut self,
        ) -> std::pin::Pin<std::boxed::Box<
            dyn std::future::Future<Output =
                std::result::Result<(), crate::state_machine::phases::PhaseStateError>
            > + std::marker::Send + 'a
        >> {
            async fn call<S_>(
                $self_: &mut crate::state_machine::phases::PhaseState<$phase, S_>,
            ) -> std::result::Result<(), crate::state_machine::phases::PhaseStateError>
            where
                S_: crate::storage::Storage,
            $body

            std::boxed::Box::pin(call(self))
        }
    };
}

/// Implements [`Phase`]`::`[`process()`] for [`PhaseState`]`: `[`Handler`].
///
/// - Processes at most `count.max` requests during the time interval `[now, now + time.min]`.
/// - Processes requests until there are enough (ie `count.min`) for the time interval
/// `[now + time.min, now + time.max]`.
/// - Aborts if either all connections were dropped or not enough requests were processed until
/// timeout.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_phase_process_for_phasestate_handler {
    ($phase: ty, $storage: ty $(,)?) => {
        paste::paste! {
            crate::impl_phase_method_for_phasestate! {
                async fn process(
                    self_: &mut PhaseState<$phase, $storage>,
                ) -> Result<(), PhaseStateError> {
                    let phase = <
                        crate::state_machine::phases::PhaseState<$phase, S_>
                        as crate::state_machine::phases::Phase<S_>
                    >::NAME;
                    let crate::state_machine::coordinator::PhaseParameters { count, time } =
                        self_.shared.state.[<$phase:lower>];
                    tracing::info!("processing requests in {} phase", phase);
                    tracing::debug!(
                        "in {} phase for min {} and max {} seconds",
                        phase, time.min, time.max,
                    );
                    self_.process_during(tokio::time::Duration::from_secs(time.min)).await?;

                    let time_left = time.max - time.min;
                    tokio::time::timeout(
                        tokio::time::Duration::from_secs(time_left),
                        self_.process_until_enough()
                    ).await??;

                    tracing::info!(
                        "in total {} {} messages accepted (min {} and max {} required)",
                        self_.private.accepted,
                        phase,
                        count.min,
                        count.max,
                    );
                    tracing::info!(
                        "in total {} {} messages rejected",
                        self_.private.rejected,
                        phase,
                    );
                    tracing::info!(
                        "in total {} {} messages discarded (purged not included)",
                        self_.private.discarded,
                        phase,
                    );

                    std::result::Result::Ok(())
                }
            }
        }
    };
}

/// Implements [`Phase`]`::`[`purge()`] for [`PhaseState`].
///
/// Circumvents the infeasibility of default trait impls due to the dependency on internal state.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_phase_purge_for_phasestate {
    ($phase: ty, $storage: ty $(,)?) => {
        fn purge(
            &mut self,
        ) -> std::result::Result<(), crate::state_machine::phases::PhaseStateError> {
            let phase = <
                crate::state_machine::phases::PhaseState<$phase, $storage>
                as crate::state_machine::phases::Phase<$storage>
            >::NAME;
            tracing::info!("purging outdated request from {} phase", phase);

            while let std::option::Option::Some((_, span, resp_tx)) = self.try_next_request()? {
                let _span_guard = span.enter();
                crate::metric!(discarded: self.shared.state.round_id, phase);
                let _ = resp_tx.send(
                    std::result::Result::Err(crate::state_machine::RequestError::MessageDiscarded)
                );
            }

            std::result::Result::Ok(())
        }
    };
}

/// Implements all [`Handler`] methods for [`PhaseState`] except for [`handle_request()`].
///
/// Circumvents the infeasibility of default trait impls due to the dependency on internal state.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_handler_for_phasestate {
    ($phase: ty) => {
        paste::paste! {
            fn has_enough_messages(&self) -> bool {
                self.private.accepted >= self.shared.state.[<$phase:lower>].count.min
            }

            fn has_overmuch_messages(&self) -> bool {
                self.private.accepted >= self.shared.state.[<$phase:lower>].count.max
            }

            fn increment_accepted(&mut self) {
                let phase = <Self as crate::state_machine::phases::Phase<_>>::NAME;
                self.private.accepted += 1;
                tracing::debug!(
                    "{} {} messages accepted (min {} and max {} required)",
                    self.private.accepted,
                    phase,
                    self.shared.state.[<$phase:lower>].count.min,
                    self.shared.state.[<$phase:lower>].count.max,
                );
                crate::metric!(accepted: self.shared.state.round_id, phase);
            }

            fn increment_rejected(&mut self) {
                let phase = <Self as crate::state_machine::phases::Phase<_>>::NAME;
                self.private.rejected += 1;
                tracing::debug!("{} {} messages rejected", self.private.rejected, phase);
                crate::metric!(rejected: self.shared.state.round_id, phase);
            }

            fn increment_discarded(&mut self) {
                let phase = <Self as crate::state_machine::phases::Phase<_>>::NAME;
                self.private.discarded += 1;
                tracing::debug!("{} {} messages discarded", self.private.discarded, phase);
                crate::metric!(discarded: self.shared.state.round_id, phase);
            }
        }
    };
}

impl<S, T> PhaseState<S, T>
where
    Self: Handler + Phase<T>,
    T: Storage,
{
    /// Processes requests for as long as the given duration.
    async fn process_during(&mut self, dur: tokio::time::Duration) -> Result<(), PhaseStateError> {
        let phase = <Self as Phase<_>>::NAME;
        let deadline = tokio::time::sleep(dur);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => {
                    debug!("{} duration elapsed", phase);
                    break Ok(());
                }
                next = self.next_request() => {
                    let (req, span, resp_tx) = next?;
                    self.process_single(req, span, resp_tx).await;
                }
            }
        }
    }

    /// Processes requests until there are enough.
    async fn process_until_enough(&mut self) -> Result<(), PhaseStateError> {
        while !self.has_enough_messages() {
            let (req, span, resp_tx) = self.next_request().await?;
            self.process_single(req, span, resp_tx).await;
        }
        Ok(())
    }

    /// Processes a single request.
    ///
    /// The request is discarded if the maximum message count is reached, accepted if processed
    /// successfully and rejected otherwise.
    async fn process_single(
        &mut self,
        req: StateMachineRequest,
        span: Span,
        resp_tx: ResponseSender,
    ) {
        let _span_guard = span.enter();

        let response = if self.has_overmuch_messages() {
            self.increment_discarded();
            Err(RequestError::MessageDiscarded)
        } else {
            let response = self.handle_request(req).await;
            if response.is_ok() {
                self.increment_accepted();
            } else {
                self.increment_rejected();
            }
            response
        };

        // This may error out if the receiver has already been dropped but it doesn't matter for us.
        let _ = resp_tx.send(response);
    }
}

impl<'a, S, T> PhaseState<S, T>
where
    S: Send + 'a,
    T: Storage,
    Self: Phase<T> + Sync + 'a,
{
    /// Runs the current phase to completion.
    ///
    /// 1. Performs the phase tasks.
    /// 2. Purges outdated phase messages.
    /// 3. Broadcasts the phase data.
    /// 4. Transitions to the next phase.
    pub async fn run_phase(mut self) -> Option<StateMachine<T>> {
        let phase = <Self as Phase<_>>::NAME;
        let span = error_span!("run_phase", phase = %phase);

        async move {
            info!("starting {} phase", phase);
            self.shared.events.broadcast_phase(phase);
            metric!(Measurement::Phase, phase as u8);

            if let Err(err) = self.process().await {
                warn!("failed to perform the {} phase tasks", phase);
                return Some(self.into_error_state(err));
            }

            if let Err(err) = self.purge() {
                warn!("failed to purge outdated requests from the {} phase", phase);
                if let PhaseName::Error | PhaseName::Shutdown = phase {
                    debug!(
                        "already in {} phase: ignoring error while purging outdated requests",
                        phase,
                    );
                } else {
                    return Some(self.into_error_state(err));
                }
            }

            if let Err(err) = self.broadcast().await {
                warn!("failed to broadcast the {} phase data", phase);
                return Some(self.into_error_state(err));
            }

            info!("transitioning to the next phase");
            self.next()
        }
        .instrument(span)
        .await
    }
}

// Functions that are available to all states
impl<S, T> PhaseState<S, T>
where
    T: Storage,
{
    /// Receives the next [`StateMachineRequest`].
    ///
    /// # Errors
    /// Returns [`PhaseStateError::RequestChannel`] when all sender halves have been dropped.
    async fn next_request(
        &mut self,
    ) -> Result<(StateMachineRequest, Span, ResponseSender), PhaseStateError> {
        debug!("waiting for the next incoming request");
        self.shared.request_rx.next().await.ok_or_else(|| {
            error!("request receiver broken: senders have been dropped");
            PhaseStateError::RequestChannel("all message senders have been dropped!")
        })
    }

    fn try_next_request(
        &mut self,
    ) -> Result<Option<(StateMachineRequest, Span, ResponseSender)>, PhaseStateError> {
        match self.shared.request_rx.try_recv() {
            Some(Some(item)) => Ok(Some(item)),
            None => {
                debug!("no pending request");
                Ok(None)
            }
            Some(None) => {
                warn!("failed to get next pending request: channel shut down");
                Err(PhaseStateError::RequestChannel(
                    "all message senders have been dropped!",
                ))
            }
        }
    }

    fn into_error_state(self, err: PhaseStateError) -> StateMachine<T> {
        PhaseState::<PhaseStateError, _>::new(self.shared, err).into()
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::{state_machine::tests::utils, storage::tests::init_store};

    #[tokio::test]
    #[serial]
    async fn integration_update_round_id() {
        let store = init_store().await;
        let coordinator_state = utils::coordinator_state();
        let (mut shared, _, event_subscriber) = utils::init_shared(coordinator_state, store);

        let phases = event_subscriber.phase_listener();
        // When starting the round ID should be 0
        let id = phases.get_latest().round_id;
        assert_eq!(id, 0);

        shared.set_round_id(1);
        assert_eq!(shared.state.round_id, 1);

        // Old events should still have the same round ID
        let id = phases.get_latest().round_id;
        assert_eq!(id, 0);

        // But new events should have the new round ID
        shared.events.broadcast_phase(PhaseName::Sum);
        let id = phases.get_latest().round_id;
        assert_eq!(id, 1);
    }
}
