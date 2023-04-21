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
        .about("Data validation for csv files.")
        .after_help(
            "To run this program provide a csv file (-f) for either validation or constraints generation"
        )
        .arg(arg!(-f --file <VALUE>).required(true).help("Csv file"))
        .arg(
            arg!(-c --constraints ...)
                .required(false)
                .help("Print Constraints"),
        )
        .arg(
            arg!(-s --save <VALUE>)
                .required(false)
                .help("Save Constraints"),
        )
        .arg(
            arg!(-v --validate <VALUE>)
                .required(false)
                .help("Print validation to json"),
        )
        .arg(
            arg!(-o --output <VALUE>)
                .required(false)
                .help("Save validation to csv"),
        )
        .get_matches();

    let file_path = matches.get_one::<String>("file").expect("File is required");
    let print_constraint = matches.get_one::<u8>("constraints").unwrap();
    let save_constraint = matches.get_one::<String>("save");
    let validate = matches.get_one::<String>("validate");
    let output = matches.get_one::<String>("output");

    let data: DataFrame = CsvReader::from_path(&file_path).unwrap().finish().unwrap();

    if print_constraint.to_owned() == 1 {
        println!("{}", ConstraintSet::new(&data))
    }

    if save_constraint.is_some() {
        let cons = ConstraintSet::new(&data);
        cons.save_json(save_constraint.unwrap()).unwrap();
        println!("Constraints saved at: {}", save_constraint.unwrap())
    }

    if validate.is_some() {
        let cons = ConstraintSet::read_constraints(&validate.unwrap());
        let val = ValidationSet::new(&data, &cons.unwrap());
        println!("{}", val)
    }

    if output.is_some() {
        let cons = ConstraintSet::read_constraints(&validate.unwrap());
        let val = ValidationSet::new(&data, &cons.unwrap());
        val.save_csv(&output.unwrap()).unwrap();
        println!("Validations saved at: {}", output.unwrap())
    }
}
