use std::{
    sync::Arc,
    task::{Context, Poll},
};

use futures::future::{self, Ready};
use tower::Service;

use crate::{
    mask::Model,
    state_machine::events::{EventListener, EventSubscriber, ModelUpdate},
};

pub struct ModelRequest;
pub type ModelResponse = Option<Arc<Model>>;

/// A service that serves the latest available global model
pub struct ModelService(EventListener<ModelUpdate>);

impl ModelService {
    pub fn new(events: &EventSubscriber) -> Self {
        Self(events.model_listener())
    }
}

impl Service<ModelRequest> for ModelService {
    type Response = ModelResponse;
    type Error = ::std::convert::Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: ModelRequest) -> Self::Future {
        future::ready(match self.0.get_latest().event {
            ModelUpdate::Invalidate => Ok(None),
            ModelUpdate::New(model) => Ok(Some(model)),
        })
    }
}
