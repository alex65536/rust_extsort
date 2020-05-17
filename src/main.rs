use std::io::{self, BufRead, BufReader};
use extsort::{Sort, Config};

fn main() -> io::Result<()> {
    let lines = BufReader::new(io::stdin()).lines();
    let mut config = Config::default();
    config.max_split_size = 5_000_000;
    let sort = Sort::new(config)?;
    let sorted = sort.sort(lines.map(|maybe_line| {
        match maybe_line {
            Ok(line) => line,
            Err(err) => panic!("I/O error: {}", err)
        }
    }))?;
    for maybe_line in sorted {
        println!("{}", maybe_line?);
    }
    Ok(())
}
