use polars::prelude::*;
use dv-rs::frame_constraints;

fn main() {
    let file: &str = "brain.csv";
    let df: DataFrame = CsvReader::from_path(file).unwrap().finish().unwrap();

    frame_constraints(&df)
}
