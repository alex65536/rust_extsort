# rust_extsort

This Rust crate implements external memory, multithreaded string sorting. You can see usage example in `src/main.rs`.

## Testing
To run the tests, use `test/test.sh` script. It compares the sorting implementation with the output of `sort` command. Be careful, as the files generated during testing may be large (about 200 MB).
