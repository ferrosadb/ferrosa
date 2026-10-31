//! The smallest shape that produces a Lean file which cannot elaborate.
//!
//! `Error::InvalidData("...".into())` appears in every crate that reports parse failures with a
//! `Box<dyn Error>` behind the message. Extracted with a narrowed `--start-from`, the trait impl is
//! emitted but its trait declaration is not, and the resulting file fails with a message about a
//! `Singleton` instance that has nothing to do with the cause.

#[derive(Debug)]
pub enum Error {
    InvalidData(Box<dyn std::error::Error>),
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Error::InvalidData(e)
    }
}

pub fn check(n: usize) -> Result<usize, Error> {
    if n == 0 {
        return Err("n must be non-zero".into());
    }
    Ok(n)
}
