use std::io::{self, Lines, BufWriter, BufReader, BufRead, Write, Seek, SeekFrom};
use std::fs::File;
use super::lines::{FromLine, IntoLine};
use std::marker;

/// Iterator to iterate over the group of equal elements.
pub struct SameSplitIter<T> {
    /// Lines iterator from which the elements are taken
    lines: Lines<BufReader<File>>,
    _marker: marker::PhantomData<T>
}

/// Iterator to split the source iterator onto groups of equal elements.
pub struct SplitIter<Iter, T> {
    /// Source iterator
    iter: Iter,
    /// Last value taken from the source iterator
    last: Option<T>,
    _marker: marker::PhantomData<T>
}

impl<T: FromLine> Iterator for SameSplitIter<T> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lines.next() {
            Some(maybe_line) => Some(maybe_line.map(|ln| T::from_line(&ln))),
            None => None
        }
    }
}

impl<Iter, T> Iterator for SplitIter<Iter, T>
where
    Iter: Iterator<Item = T>,
    T: FromLine + IntoLine + Eq
{
    type Item = io::Result<SameSplitIter<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.last.is_none() {
            return None;
        }
        let mut file = match tempfile::tempfile() {
            Ok(file) => file,
            Err(err) => return Some(Err(err))
        };
        {
            let mut writer = BufWriter::new(&mut file);
            loop {
                let next = self.iter.next();
                let last_ref = self.last.as_ref().unwrap();
                let finish = match next.as_ref() {
                    None => true,
                    Some(val) => val != last_ref
                };
                if let Some(data) = self.last.take() {
                    let line = data.into_line() + "\n";
                    if let Err(err) = writer.write_all(line.as_bytes()) {
                        return Some(Err(err));
                    }
                }
                self.last = next;
                if finish {
                    break;
                }
            }
        }
        if let Err(err) = file.seek(SeekFrom::Start(0)) {
            return Some(Err(err));
        }
        Some(Ok(SameSplitIter {
            lines: BufReader::new(file).lines(),
            _marker: marker::PhantomData
        }))
    }
}

/// Creates an iterator that splits all the items from `iter` into the groups
/// of equal elements.
///
/// To perform the split, the iterator will use external memory if it's
/// necessary.
pub fn split<Iter, T>(iter: Iter) -> SplitIter<Iter, T>
where
    Iter: Iterator<Item = T>,
    T: FromLine + IntoLine + Eq
{
    let last = iter.next();
    SplitIter { iter, last, _marker: marker::PhantomData }
}
