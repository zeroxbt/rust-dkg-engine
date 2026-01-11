use std::{collections::HashMap, sync::Arc};

use crate::{
    commands::publish_replication_command::PublishReplicationCommandHandler, context::Context,
};

use super::{
    command::Command,
    command_handler::{CommandHandler, ScheduleConfig},
    dial_peers_command::DialPeersCommandHandler,
};
use chrono::Utc;
use uuid::Uuid;

pub struct CommandResolver {
    handlers: HashMap<&'static str, Arc<dyn CommandHandler>>,
}

impl CommandResolver {
    pub fn new(context: Arc<Context>) -> Self {
        let handlers: Vec<Arc<dyn CommandHandler>> = vec![
            Arc::new(DialPeersCommandHandler::new(context.clone())),
            Arc::new(PublishReplicationCommandHandler::new(context.clone())),
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
                        data: serde_json::json!({}),
                        period: Some(period_ms),
                        delay: initial_delay_ms,
                        retries: 0,
                        ready_at: now.timestamp_millis(),
                        started_at: None,
                        deadline_at: None,
                        status: super::command::CommandStatus::Pending,
                        message: None,
                        parent_id: None,
                        sequence: None,
                        transactional: false,
                        created_at: now,
                        updated_at: now,
                    })
                }
                ScheduleConfig::OneShot => None,
            })
            .collect()
    }
}
