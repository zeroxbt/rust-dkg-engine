use std::{sync::Arc, time::Duration};

use crate::{
    commands::{executor::CommandExecutionResult, registry::CommandHandler},
    context::Context,
    services::{PeerService, peer::PeerAddressStore},
};

const SAVE_PEER_ADDRESSES_PERIOD: Duration = Duration::from_secs(60);

pub(crate) struct SavePeerAddressesCommandHandler {
    peer_service: Arc<PeerService>,
    address_store: Arc<PeerAddressStore>,
}

impl SavePeerAddressesCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            peer_service: Arc::clone(context.peer_service()),
            address_store: Arc::clone(context.peer_address_store()),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct SavePeerAddressesCommandData;

impl CommandHandler<SavePeerAddressesCommandData> for SavePeerAddressesCommandHandler {
    #[tracing::instrument(
        name = "periodic.save_peer_addresses",
        skip(self),
        fields(
            peers = tracing::field::Empty,
        )
    )]
    async fn execute(&self, _: &SavePeerAddressesCommandData) -> CommandExecutionResult {
        let addresses = self.peer_service.get_all_addresses();

        tracing::Span::current().record("peers", addresses.len());

        if !addresses.is_empty() {
            self.address_store.save_all(&addresses);
        }

        CommandExecutionResult::Repeat {
            delay: SAVE_PEER_ADDRESSES_PERIOD,
        }
    }
}
