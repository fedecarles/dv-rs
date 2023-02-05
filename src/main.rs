mod constraints;
mod validation;
use constraints::constraints::frame_constraints;
use constraints::constraints::*;
use polars::prelude::*;
use validation::validation::*;

fn main() {
    let file: &str = "test_data/brain_stroke.csv";
    let df: DataFrame = CsvReader::from_path(file).unwrap().finish().unwrap();

    //let x = frame_constraints(&df);
    //println!("{:?}", x)

    let c = Constraint::get_col_constraints(&df, "work_type");
    //let y = Validation::check_nullable(&df, c);
    //let y = Validation::check_duplicates(&df, "gender", c.unique.unwrap());
    let y = Validation::check_min_length(&df, c);
    println!("{:?}", y)
}
