/// Result of a batch send operation.
#[derive(Debug)]
pub struct BatchSendResult {
    /// Number of requests successfully sent.
    pub sent_count: usize,
    /// Number of requests that failed to send.
    pub failed_count: usize,
    /// Whether the operation was completed (signaled done) before all batches sent.
    pub early_completion: bool,
}
