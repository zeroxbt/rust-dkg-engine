use std::{any::Any, collections::HashMap, sync::Arc};

use chrono::Utc;
use uuid::Uuid;

use super::{
    command::Command, dial_peers_command::DialPeersCommandHandler,
    sharding_table_check_command::ShardingTableCheckCommandHandler,
};
use crate::{
    commands::protocols::publish::{
        handle_publish_request_command::HandlePublishRequestCommandHandler,
        send_publish_requests_command::SendPublishRequestsCommandHandler,
    },
    context::Context,
    types::traits::command::{CommandHandler, ScheduleConfig},
};

pub struct CommandResolver {
    handlers: HashMap<&'static str, Arc<dyn CommandHandler>>,
}

impl CommandResolver {
    pub fn new(context: Arc<Context>) -> Self {
        let handlers: Vec<Arc<dyn CommandHandler>> = vec![
            Arc::new(DialPeersCommandHandler::new(Arc::clone(&context))),
            Arc::new(ShardingTableCheckCommandHandler::new(Arc::clone(&context))),
            Arc::new(SendPublishRequestsCommandHandler::new(Arc::clone(&context))),
            Arc::new(HandlePublishRequestCommandHandler::new(Arc::clone(
                &context,
            ))),
        ];

        let mut map = HashMap::new();
        for handler in handlers {
            map.insert(handler.name(), handler);
        }

        Self { handlers: map }
    }

    pub fn resolve(&self, name: &str) -> Option<Arc<dyn CommandHandler>> {
        self.handlers.get(name).cloned()
    }

    pub fn periodic_commands(&self) -> Vec<Command> {
        self.handlers
            .values()
            .filter_map(|handler| match handler.schedule_config() {
                ScheduleConfig::Periodic {
                    period_ms,
                    initial_delay_ms,
                } => {
                    let now = Utc::now();
                    Some(Command {
                        id: Uuid::new_v4(),
                        name: handler.name().to_string(),
                        data: Arc::new(()) as Arc<dyn Any + Send + Sync>,
                        period: Some(period_ms),
                        delay: initial_delay_ms,
                        retries: 0,
                        ready_at: now.timestamp_millis(),
                        deadline_at: None,
                    })
                }
                ScheduleConfig::OneShot => None,
            })
            .collect()
    }
}
