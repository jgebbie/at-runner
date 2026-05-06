use std::io;

use tonic::Status;

fn validate_component(value: &str, field: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{field} must not be empty"
        )));
    }

    // These values are used to form paths under the configured workspace / run dir.
    // We only allow a single "path component" to avoid traversal (`../`), absolute
    // paths, or platform-specific separators.
    if value == "." || value == ".." || value.contains('/') || value.contains('\\') {
        return Err(Status::invalid_argument(format!(
            "{field} must be a single path component"
        )));
    }

    // Extra defense-in-depth: even though we reject separators above, disallowing
    // `..` prevents surprising filenames and makes intent explicit.
    if value.contains("..") {
        return Err(Status::invalid_argument(format!(
            "{field} must not contain '..'"
        )));
    }

    Ok(())
}

pub fn validate_filename(name: &str) -> Result<(), Status> {
    validate_component(name, "filename")
}

pub fn validate_file_root(file_root: &str) -> Result<(), Status> {
    validate_component(file_root, "file_root")
}

pub fn validate_step_id(step_id: &str) -> Result<(), Status> {
    validate_component(step_id, "step id")
}

pub fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::{validate_file_root, validate_filename, validate_step_id};

    #[test]
    fn accepts_simple_components() {
        validate_filename("MunkK.env").unwrap();
        validate_file_root("MunkK").unwrap();
        validate_step_id("kraken_1").unwrap();
    }

    #[test]
    fn rejects_path_like_components() {
        for value in ["", ".", "..", "../x", "x/y", "x\\y", "x..y"] {
            assert!(validate_filename(value).is_err(), "{value:?}");
            assert!(validate_file_root(value).is_err(), "{value:?}");
            assert!(validate_step_id(value).is_err(), "{value:?}");
        }
    }
}
