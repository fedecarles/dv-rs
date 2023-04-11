mod constraints;
mod validation;
use clap::{arg, Command};
use constraints::constraints::*;
use polars::prelude::*;
use validation::validation::*;

fn main() {
    let matches = Command::new("dvrs")
        .version("0.1.0")
        .author("Federico Carlés. <federico.carles@pm.me>")
        .about("Data validation for Polars DataFrame")
        .arg(arg!(--constraint <VALUE>).short('c').required(true))
        .arg(arg!(--validate <VALUE>).required(false))
        .get_matches();

    let constraint_path = matches
        .get_one::<String>("constraint")
        .expect("Constraint is required");

    let validate_path = matches
        .get_one::<String>("validate")
        .expect("Validate is required");

    let constraint_df: DataFrame = CsvReader::from_path(&constraint_path)
        .unwrap()
        .finish()
        .unwrap();

    let validate_df: DataFrame = CsvReader::from_path(&validate_path)
        .unwrap()
        .finish()
        .unwrap();

    //let x = frame_constraints(&constraint_df);
    //println!("{:?}", x);

    let mut cons = ConstraintSet::new(&constraint_df);
    cons.modify("age", "nullable", "true");
    cons.modify("gnder", "max_length", "5");
    cons.modify("Residence_type", "alue_range", "Rural,Urban, Semi");

    println!("{:?}", cons)
    //ConstraintSet::save_json(&constraint_df);
    //let y = frame_validation(&validate_df, &cons);
    //println!("{:?}", y.unwrap())

    //let json = serde_json::to_string(&cons);
    //println!("{:?}", json);
}
