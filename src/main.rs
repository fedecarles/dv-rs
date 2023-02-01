use dvrs::frame_constraints;
use polars::prelude::*;

fn main() {
    let file: &str = "test_data/brain_stroke.csv";
    let df: DataFrame = CsvReader::from_path(file).unwrap().finish().unwrap();

    let x = frame_constraints(&df);
    println!("{:?}", x)
}
