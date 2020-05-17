use std::io::{self, Error, ErrorKind};
use std::marker::Sized;

/// Converts the value into a single line for use in sorting.
///
/// As `Sort` implementation keeps the temporary data in text files with
/// entries separated by newlines, the type that is about to be sorted must
/// implement this trait. Of course, the convertion must be revertible, so
/// that `T::from_line(value.into_line()) == value` holds.
pub trait IntoLine {
    /// Estimates the length of the line returned by `into_line()` method.
    /// This is required because the `Sort` needs to know how to split the
    /// input into pieces of roughly equal size.
    fn line_len(&self) -> usize;

    /// Performs the conversion from `Self` to the line. The resulting line
    /// must not contain `'\r'`, `'\n'` and `'\0'` characters.
    fn into_line(self) -> String;
}

/// Converts the line back into the original value.
///
/// As `Sort` implementation keeps the temporary data in text files with
/// entries separated by newlines, the type that is about to be sorted must
/// implement this trait. Of course, the convertion must be revertible, such
/// that `T::from_line(value.into_line()) == value` holds.
pub trait FromLine where Self: Sized {
    /// Performs the convertion from `line` to `Self`.
    fn from_line(line: &str) -> io::Result<Self>;
}

impl<T: IntoLine, E: IntoLine> IntoLine for Result<T, E> {
    fn line_len(&self) -> usize {
        match &self {
            Ok(val) => 1 + val.line_len(),
            Err(err) => 1 + err.line_len()
        }
    }

    fn into_line(self) -> String {
        match self {
            Ok(val) => "1".to_string() + &val.into_line(),
            Err(err) => "0".to_string() + &err.into_line()
        }
    }
}

impl<T: FromLine, E: FromLine> FromLine for Result<T, E> {
    fn from_line(line: &str) -> io::Result<Self> {
        match line.chars().next() {
            Some('1') => Ok(Ok(T::from_line(&line[1..])?)),
            Some('0') => Ok(Err(E::from_line(&line[1..])?)),
            _ => Err(Error::from(ErrorKind::InvalidInput))
        }
    }
}
