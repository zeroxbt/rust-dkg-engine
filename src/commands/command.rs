use std::{any::Any, sync::Arc};

use chrono::Utc;
use uuid::Uuid;

#[derive(Clone)]
pub struct Command {
    pub id: Uuid,
    pub name: String,
    pub ready_at: i64,
    pub delay: i64,
    pub deadline_at: Option<i64>,
    pub period: Option<i64>,
    pub retries: i32,
    pub data: Arc<dyn Any + Send + Sync>,
}

pub struct CommandBuilder {
    name: String,
    data: Option<Arc<dyn Any + Send + Sync>>,
    delay: i64,
    retries: i32,
    period: Option<i64>,
    deadline_at: Option<i64>,
}

impl CommandBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            data: None,
            delay: 0,
            retries: 0,
            period: None,
            deadline_at: None,
        }
    }

    pub fn data<T: Any + Send + Sync>(mut self, data: T) -> Self {
        self.data = Some(Arc::new(data));
        self
    }

    pub fn delay(mut self, delay: i64) -> Self {
        self.delay = delay;
        self
    }

    pub fn retries(mut self, retries: i32) -> Self {
        self.retries = retries;
        self
    }

    pub fn period(mut self, period: i64) -> Self {
        self.period = Some(period);
        self
    }

    pub fn deadline_at(mut self, deadline_at: i64) -> Self {
        self.deadline_at = Some(deadline_at);
        self
    }

    pub fn build(self) -> Command {
        let now = Utc::now();
        Command {
            id: Uuid::new_v4(),
            name: self.name,
            data: self.data.unwrap_or_else(|| Arc::new(())),
            ready_at: now.timestamp_millis(),
            delay: self.delay,
            deadline_at: self.deadline_at,
            period: self.period,
            retries: self.retries,
        }
    }
}

impl Command {
    /// Downcast the command data to the expected type.
    pub fn data<T: Any + Send + Sync>(&self) -> &T {
        self.data
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("Failed to downcast command data to expected type"))
    }
}
