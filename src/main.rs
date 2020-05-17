use std::io::{self, BufRead, BufReader};
use extsort::{Sort, Config, FromLine, IntoLine};

#[derive(Eq, PartialEq, PartialOrd, Ord)]
struct Line(String);

impl FromLine for Line {
    fn from_line(line: &str) -> Self { Line(String::from(line)) }
}

impl IntoLine for Line {
    fn line_len(&self) -> usize { self.0.len() }
    fn into_line(self) -> String { self.0 }
}

fn main() -> io::Result<()> {
    let lines = BufReader::new(io::stdin()).lines();
    let mut config = Config::default();
    config.max_split_size = 5_000_000;
    let sort = Sort::new(config)?;
    let sorted = sort.sort(lines.map(|maybe_line| {
        match maybe_line {
            Ok(line) => Line(line),
            Err(err) => panic!("I/O error: {}", err)
        }
    }))?;
    for maybe_line in sorted {
        println!("{}", maybe_line?.0);
    }
    Ok(())
}
