use std::{pin::Pin, task::Poll};

use derive_more::From;
use futures::{future::Future, task::Context};
use thiserror::Error;
use tokio::sync::oneshot;
use tower::Service;
use tracing::Span;

use crate::{
    message::{MessageOwned, PayloadOwned, UpdateOwned},
    state_machine::requests::{
        Request,
        RequestSender,
        Sum2Request,
        Sum2Response,
        SumRequest,
        SumResponse,
        UpdateRequest,
        UpdateResponse,
    },
    utils::trace::{Traceable, Traced},
    PetError,
};

/// The request type handled by [`StateMachineService`]
#[derive(Debug, From)]
pub struct StateMachineRequest(MessageOwned);
pub type StateMachineResponse = Result<(), StateMachineError>;

#[derive(Debug, Error)]
pub enum StateMachineError {
    #[error("PET protocol error: {0}")]
    Pet(PetError),
    #[error("Unknown internal error")]
    InternalError,
}

pub struct StateMachineService {
    handle: RequestSender<Traced<Request>>,
}

impl StateMachineService {
    pub fn new(handle: RequestSender<Traced<Request>>) -> Self {
        Self { handle }
    }

    pub fn handler(&self) -> StateMachineRequestHandler {
        trace!("creating new handler");
        StateMachineRequestHandler {
            handle: self.handle.clone(),
        }
    }
}

pub struct StateMachineRequestHandler {
    handle: RequestSender<Traced<Request>>,
}

impl StateMachineRequestHandler {
    fn send_request(&mut self, span: Span, req: Request) -> Result<(), StateMachineError> {
        let req = Traced::new(req, span);
        self.handle.send(req).map_err(|e| {
            warn!("could not send request to the state machine: {:?}", e);
            StateMachineError::InternalError
        })?;
        Ok(())
    }

    pub async fn sum_request(mut self, span: Span, req: SumRequest) -> StateMachineResponse {
        let (resp_tx, resp_rx) = oneshot::channel::<SumResponse>();
        self.send_request(span, Request::Sum((req, resp_tx)))?;
        let sum_resp = resp_rx.await.map_err(|_| {
            warn!("could not get response from state machine");
            StateMachineError::InternalError
        })?;
        sum_resp.map_err(StateMachineError::Pet)
    }

    pub async fn update_request(mut self, span: Span, req: UpdateRequest) -> StateMachineResponse {
        let (resp_tx, resp_rx) = oneshot::channel::<UpdateResponse>();
        self.send_request(span, Request::Update((req, resp_tx)))?;
        let update_resp = resp_rx.await.map_err(|_| {
            warn!("could not get response from state machine");
            StateMachineError::InternalError
        })?;
        update_resp.map_err(StateMachineError::Pet)
    }

    pub async fn sum2_request(mut self, span: Span, req: Sum2Request) -> StateMachineResponse {
        let (resp_tx, resp_rx) = oneshot::channel::<Sum2Response>();
        self.send_request(span, Request::Sum2((req, resp_tx)))?;
        let sum2_resp = resp_rx.await.map_err(|_| {
            warn!("could not get response from state machine");
            StateMachineError::InternalError
        })?;
        sum2_resp.map_err(StateMachineError::Pet)
    }
}

impl Service<Traced<StateMachineRequest>> for StateMachineService {
    type Response = StateMachineResponse;
    type Error = ::std::convert::Infallible;
    #[allow(clippy::type_complexity)]
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Traced<StateMachineRequest>) -> Self::Future {
        trace!("creating a new handler for the request");
        let handler = self.handler();
        let req_span = req.span().clone();

        let MessageOwned { header, payload } = req.into_inner().0;

        match payload {
            PayloadOwned::Sum(sum) => {
                debug!("creating a sum request to send to the state machine");
                let req = SumRequest {
                    participant_pk: header.participant_pk,
                    ephm_pk: sum.ephm_pk,
                };
                Box::pin(async move { Ok(handler.sum_request(req_span, req).await) })
            }
            PayloadOwned::Update(update) => {
                debug!("creating an update request to send to the state machine");
                let UpdateOwned {
                    local_seed_dict,
                    masked_model,
                    ..
                } = update;
                let req = UpdateRequest {
                    participant_pk: header.participant_pk,
                    local_seed_dict,
                    masked_model,
                };
                Box::pin(async move { Ok(handler.update_request(req_span, req).await) })
            }
            PayloadOwned::Sum2(sum2) => {
                debug!("creating a sum2 request to send to the state machine");
                let req = Sum2Request {
                    participant_pk: header.participant_pk,
                    mask: sum2.mask,
                };
                Box::pin(async move { Ok(handler.sum2_request(req_span, req).await) })
            }
        }
    }
}
