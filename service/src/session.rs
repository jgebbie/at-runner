//! Per-request correlation IDs for structured logs.

pub fn new_session_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
