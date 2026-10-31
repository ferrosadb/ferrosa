//! The smallest shape that produces a Lean file which cannot elaborate.
//!
//! `Error::InvalidData("...".into())` appears in every crate that reports parse failures with a
//! `Box<dyn Error>` behind the message. The `.into()` here is `impl From<&str> for Box<dyn Error>`
//! from `alloc`, which is the trait impl Aeneas emits without the declaration it names.
//!
//! Note the `.into()` must target `Box<dyn Error>` directly: `Err("x".into())` on a `Result<_, Error>`
//! asks for `From<&str> for Error`, which is a different (and non-existent) impl.

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
        return Err(Error::InvalidData("n must be non-zero".into()));
    }
    Ok(n)
}
