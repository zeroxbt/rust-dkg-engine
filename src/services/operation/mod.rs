mod pending_requests;
mod result_store;
mod service;
mod traits;

pub(crate) use pending_requests::RequestError;
pub(crate) use service::OperationService;
pub(crate) use traits::Operation;
