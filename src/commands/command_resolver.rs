use std::sync::Arc;

use crate::context::Context;

use super::{
    command::CommandName, command_handler::CommandHandler,
    dial_peers_command::DialPeersCommandHandler,
};

pub struct CommandResolver;

impl CommandResolver {
    pub fn resolve(name: &CommandName, context: Arc<Context>) -> Box<dyn CommandHandler> {
        match name {
            CommandName::DialPeers => Box::new(DialPeersCommandHandler::new(context)),
            CommandName::Default => panic!("Unable to resolve Default command!"),
        }
    }
}
