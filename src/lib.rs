use threadpool::ThreadPool;
use tempdir::TempDir;
use std::io::{self, BufRead, BufReader, Write, BufWriter};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::marker;
use std::cell::{RefCell};
use std::mem;
use std::sync::{Mutex, Arc};
use std::collections::{BinaryHeap};
use std::cmp::{self, Reverse};

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
pub trait FromLine {
    /// Performs the convertion from `line` to `Self`.
    fn from_line(line: &str) -> Self;
}

/// Struct that represents configuration of the sorter.
pub struct Config {
    /// Number of files to merge at one time
    pub num_merge: usize,
    /// Number of threads to sort in parallel
    pub num_threads: usize,
    /// Maximum size of the file during the split phase
    pub max_split_size: usize
}

impl Default for Config {
    fn default() -> Config {
        let num_threads = num_cpus::get();
        Config {
            num_merge: 16,
            num_threads,
            max_split_size: 10_000_000 / num_threads
        }
    }
}

type Lines = io::Lines<BufReader<File>>;

type ResultCell = Arc<Mutex<io::Result<()>>>;

/// The sorter structure.
pub struct Sort<T> {
    /// Sorter configuration
    config: Config,
    /// Thread pool use to run the jobs
    pool: ThreadPool,
    /// Temporary directory holder
    tmpdir: TempDir,
    /// Current number of sorting stage
    stage_num: RefCell<usize>,
    /// Number of the files on the current sorting stage
    file_num: RefCell<usize>,
    /// A `RefCell` that contains the result of the operation in the thread pool
    /// It contains `Ok(())` if all the operations succeeded, and the first
    /// error otherwise.
    result_cell: ResultCell,
    _marker: marker::PhantomData<T>
}

/// The iterator over sorted data.
pub struct SortedIter<T> {
    /// The sorted structure. It's kept here because we the temporary files
    /// will be dropped when `Sort` drops, and we don't want it to happen
    /// while iterating over the results.
    _sort: Sort<T>,
    /// `Lines` iterator over the resulting file
    lines: Option<Lines>
}

/// Make a `Lines` iterator from the file
fn file_as_lines<P: AsRef<Path>>(path: P) -> io::Result<Lines> {
    Ok(BufReader::new(File::open(path)?).lines())
}

impl<T: FromLine> Iterator for SortedIter<T> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lines.as_mut()?.next() {
            Some(maybe_line) => {
                Some(maybe_line.map(|line| T::from_line(&line)))
            },
            None => None
        }
    }
}

impl<T: FromLine + IntoLine + Ord + Send + 'static> Sort<T> {
    /// Indicates that we create the next file on the current stage.
    fn next_file(&self) {
        *self.file_num.borrow_mut() += 1;
    }

    /// Indicates that the sorting stage has changed
    fn next_stage(&self) {
        *self.file_num.borrow_mut() = 0;
        *self.stage_num.borrow_mut() += 1;
    }

    /// Constucts the name of the temporary file based on the base directory,
    /// the stage number and the file number.
    fn get_dir_file_name(dir: &Path, stage: usize, num: usize) -> PathBuf {
        let filename = format!("f{}-{}.txt", stage, num);
        dir.join(filename)
    }

    /// Constucts the name of the temporary file based on the stage number and
    /// the file number. The base directory is taken from `self`.
    fn get_file_name(&self, stage: usize, num: usize) -> PathBuf {
        Self::get_dir_file_name(self.tmpdir.path(), stage, num)
    }

    /// Constructs the name of the current file to work on.
    fn get_cur_file_name(&self) -> PathBuf {
        self.get_file_name(*self.stage_num.borrow(), *self.file_num.borrow())
    }

    /// Adds a job to the thread pool, updating `result_cell` accordingly.
    fn add_to_pool<F>(&self, f: F)
    where
        F: FnOnce() -> io::Result<()> + Send + 'static
    {
        let res_cell = self.result_cell.clone();
        self.pool.execute(move || {
            let error = match f() {
                Ok(_) => return,
                Err(err) => err
            };
            let mut guard = match Mutex::try_lock(&res_cell) {
                Ok(guard) => guard,
                Err(_) => return
            };
            if let Ok(_) = *guard {
                *guard = Err(error);
            }
        });
    }

    /// This function is called from `split_invoke`. It adds one job to sort
    /// `data_vec` and write the results into a new temporary file.
    fn split_add_file(&self, mut data_vec: Vec<T>) -> io::Result<()> {
        if data_vec.is_empty() {
            return Ok(());
        }

        let out_filename = self.get_cur_file_name();
        let mut buf_write = BufWriter::new(File::create(out_filename)?);
        self.next_file();

        self.add_to_pool(move || {
            data_vec.sort();
            for data in data_vec {
                let line = data.into_line() + "\n";
                buf_write.write_all(line.as_bytes())?;
            }
            buf_write.flush()?;
            Ok(())
        });

        Ok(())
    }

    /// Adds jobs to split the data into chunks. The jobs are added into the
    /// thread pool, and `join_pool()` needs to be invoked before processing
    /// further data.
    fn split_invoke<It>(&self, iter: It) -> io::Result<()>
    where
        It: Iterator<Item = T>
    {
        let mut cur_size = 0;
        let mut cur_vec = Vec::<T>::new();
        for data in iter {
            let size = data.line_len();
            if cur_size + size > self.config.max_split_size {
                self.split_add_file(mem::replace(&mut cur_vec, vec![data]))?;
                cur_size = size;
                continue;
            }
            cur_vec.push(data);
            cur_size += size;
        }
        self.split_add_file(cur_vec)?;
        Ok(())
    }

    /// This function is called from `merge_invoke`. It adds one job to merge
    /// the files on stage `stage` that have numbers from `first` to `last`.
    fn merge_add_files(&self, stage: usize, first: usize,
                       last: usize) -> io::Result<()> {
        if first == last {
            return Ok(());
        }

        let out_filename = self.get_cur_file_name();
        let mut buf_write = BufWriter::new(File::create(out_filename)?);
        self.next_file();
        let dir = self.tmpdir.path().to_path_buf();

        self.add_to_pool(move || {
            let mut iters_vec = Vec::with_capacity(last - first + 1);
            for num in first..last {
                let filename = Self::get_dir_file_name(&dir, stage, num);
                let lines = file_as_lines(filename)?;
                iters_vec.push(lines.map(|maybe_line| {
                    maybe_line.map(|line| T::from_line(&line))
                }));
            }

            let mut heap = BinaryHeap::new();
            for (idx, iter) in iters_vec.iter_mut().enumerate() {
                match iter.next() {
                    Some(maybe_data) => heap.push(Reverse((maybe_data?, idx))),
                    None => continue
                }
            }

            while !heap.is_empty() {
                let (data, idx) = heap.pop().unwrap().0;
                let line = data.into_line() + "\n";
                buf_write.write_all(line.as_bytes())?;
                if let Some(maybe_data) = iters_vec[idx].next() {
                    heap.push(Reverse((maybe_data?, idx)));
                }
            }
            buf_write.flush()?;

            mem::drop(iters_vec);
            for num in first..last {
                let filename = Self::get_dir_file_name(&dir, stage, num);
                fs::remove_file(filename)?;
            }

            Ok(())
        });
        Ok(())
    }

    /// Adds jobs to perform one stage of file merging. The jobs are added into
    /// the thread pool, and `join_pool()` needs to be invoked before processing
    /// further data.
    fn merge_invoke(&self) -> io::Result<()> {
        let count = *self.file_num.borrow();
        let prev_stage = *self.stage_num.borrow();
        self.next_stage();
        let mut first = 0;
        let length = self.config.num_merge;
        while first != count {
            let last = cmp::min(count, first + length);
            self.merge_add_files(prev_stage, first, last)?;
            first = last;
        }
        Ok(())
    }

    /// Finishes all the currently added jobs in the thread pool.
    fn join_pool(&self) -> io::Result<()> {
        self.pool.join();
        if self.pool.panic_count() != 0 {
            panic!("Some of the threads in the pool panicked.");
        }
        let mut result = Mutex::lock(&self.result_cell).unwrap();
        mem::replace(&mut result, Ok(()))
    }

    /// Constructs a `SortedIter` after the sorting was finished.
    ///
    /// This functions panics if more than one file is present on the last
    /// stage.
    fn as_iter(self) -> io::Result<SortedIter<T>> {
        let lines = match *self.file_num.borrow() {
            0 => None,
            1 => {
                let filename = self.get_file_name(*self.stage_num.borrow(), 0);
                Some(file_as_lines(filename)?)
            },
            _ => panic!("More than one file exists on the last stage")
        };
        Ok(SortedIter {_sort: self, lines})
    }

    /// Creates a new `Sort` struct from the given configuration.
    pub fn new(config: Config) -> io::Result<Sort<T>> {
        let num_threads = config.num_threads;
        Ok(Sort {
            config,
            pool: ThreadPool::new(num_threads),
            tmpdir: TempDir::new("extsort")?,
            stage_num: RefCell::new(0),
            file_num: RefCell::new(0),
            result_cell: Arc::new(Mutex::new(Ok(()))),
            _marker: marker::PhantomData
        })
    }

    /// Performs external sorting, converting the sorter into `SortedIter`.
    pub fn sort<It>(self, iter: It) -> io::Result<SortedIter<T>>
    where
        It: Iterator<Item = T>
    {
        // First, split the data
        let result = self.split_invoke(iter);
        self.join_pool()?;
        if let Err(err) = result {
            return Err(err);
        }
        // Then, merge the files until only one remains
        while *self.file_num.borrow() > 1 {
            let result = self.merge_invoke();
            self.join_pool()?;
            if let Err(err) = result {
                return Err(err);
            }
        }
        // Finally, transform the sorter into iterator
        self.as_iter()
    }
}
