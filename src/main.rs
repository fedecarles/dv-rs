mod constraints;
mod validation;
use clap::{App, Arg, ArgMatches, SubCommand};
use constraints::constraints::*;
use polars::prelude::*;
use std::env;
use validation::validation::*;

fn main() {

    //let file: &str = "test_data/brain_stroke.csv";
    //let df: DataFrame = CsvReader::from_path(file).unwrap().finish().unwrap();

    //let bad_file: &str = "test_data/brain_stroke_bad.csv";
    //let bad_df: DataFrame = CsvReader::from_path(bad_file).unwrap().finish().unwrap();

    ////let x = frame_constraints(&df);
    ////println!("{:?}", x)

    ////let c = Constraint::get_col_constraints(&df, "gender");
    ////println!("{}", &c);
    ////let y = Validation::check_nullable(&df, c);
    ////let y = Validation::check_duplicates(&df, "gender", c.unique.unwrap());
    ////let y = Validation::check_min_length(&df, c);
    ////let y = Validation::check_max_value(&df, c);
    ////let y = Validation::check_value_range(&bad_df, c);
    ////println!("{:?}", &c.value_range);

    ////let y = Validation::get_col_validations(&bad_df, &c);

    //let cons = get_constraint_set(&df);

    //let x = frame_validation(&bad_df, &cons);
    //println!("{:?}", x.unwrap())
}
