#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid parameter {name:?} expected {expected:?}, found {found:?}")]
    InvalidParameter {
        name: String,
        expected: String,
        found: String,
    },
}
