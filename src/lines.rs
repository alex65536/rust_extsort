use std::io;
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
