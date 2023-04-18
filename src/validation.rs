pub mod validation {
    use crate::constraints::constraints::*;
    use polars::{export::num::ToPrimitive, prelude::*};
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use std::fs::File;
    use std::path::Path;
    use csv::Writer;
    use std::borrow::Cow;

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Validation {
        pub name: String,
        pub data_type: bool,
        pub nullable: Option<u32>,
        pub unique: Option<u32>,
        pub min_length: Option<u32>,
        pub max_length: Option<u32>,
        pub min_value: Option<u32>,
        pub max_value: Option<u32>,
        pub value_range: Option<u32>,
    }

    impl Validation {
        fn _check_data_type(data: &DataFrame, constraint: &Constraint) -> bool {
            if let Ok(col) = data.column(&constraint.name) {
                col.dtype().to_string() == constraint.data_type
            } else {
                false
            }
        }

        fn _check_nullable(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            if !constraint.nullable {
                if let Ok(col) = data.column(&constraint.name) {
                    col.null_count().to_u32()
                } else {
                    return None
                }
            } else {
                return None
            }
        }

        fn _check_duplicates(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            if !constraint.unique {
                return None;
            } else {
                let col = data.column(&constraint.name);
                return match col {
                    Ok(s) => s.is_unique().iter().count().to_u32(),
                    Err(_) => None,
                };
            }
        }

        fn _check_min_length(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            let col = data.column(&constraint.name);
            let min_length = match col {
                Ok(s) => s.utf8().map(|s| s.str_n_chars()),
                Err(e) => Err(e),
            };
            match min_length {
                Ok(s) => s
                    .lt(constraint.min_length.unwrap_or_default())
                    .into_iter()
                    .filter(|b| b.unwrap_or_default() == true)
                    .count()
                    .to_u32(),
                Err(_) => None,
            }
        }
        fn _check_max_length(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            let col = data.column(&constraint.name);
            let max_length = match col {
                Ok(s) => s.utf8().map(|s| s.str_n_chars()),
                Err(e) => Err(e),
            };
            match max_length {
                Ok(s) => s
                    .gt(constraint.max_length.unwrap_or_default())
                    .into_iter()
                    .filter(|b| b.unwrap_or_default() == true)
                    .count()
                    .to_u32(),
                Err(_) => None,
            }
        }

        fn _check_min_value(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            if let Ok(col) = data.column(&constraint.name) {
                col.lt(constraint.min_value.unwrap_or_default())
                .unwrap_or_default()
                .sum()
            } else {
                return None
            }
        }

        fn _check_max_value(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            if let Ok(col) = data.column(&constraint.name) {
                col.gt(constraint.max_value.unwrap_or_default())
                .unwrap_or_default()
                .sum()
            } else {
                return None
            }
        }

        fn _check_value_range(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            let col = data.column(&constraint.name);
            let ranges_string = constraint.value_range.as_ref().map(|s| s.as_str());
            if ranges_string.is_none() {
                return None;
            }
            let ranges: Cow<'_, [String]> = match ranges_string {
                Some(s) => Cow::from(s.split(", ").map(str::to_string).collect::<Vec<_>>()),
                None => Cow::from(vec![String::from("null")]),
            };
            println!("{:?}", ranges);
            match col {
                Ok(s) => s
                    .is_in(&Series::new("ranges", &ranges))
                    .map(|b| !b)
                    .unwrap_or_default()
                    .sum(),
                Err(_) => None,
            }
        }

        pub fn new(data: &DataFrame, constraint: &Constraint) -> Validation {
            return Validation {
                name: String::from(&constraint.name),
                data_type: Self::_check_data_type(data, constraint),
                nullable: Self::_check_nullable(data, constraint),
                unique: Self::_check_duplicates(data, constraint),
                min_length: Self::_check_min_length(data, constraint),
                max_length: Self::_check_max_length(data, constraint),
                min_value: Self::_check_min_value(data, constraint),
                max_value: Self::_check_max_value(data, constraint),
                value_range: Self::_check_value_range(data, constraint),
            };
        }
    }

    impl fmt::Display for Validation {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let name_length = self.name.len().to_usize().unwrap_or_default();
            let range_string = self.value_range.clone().unwrap_or_default().to_string();

            let trimmed_range = if range_string.len() > 60 {
                &range_string[..60]
            } else {
                &range_string
            };
            write!(f, "+{:<}+\n", "-".repeat(name_length + 178)).unwrap_or_default();
            write!(
                f,
                "|{:<width1$}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<width2$}|\n",
                "Name",
                "Data Type",
                "Nullable",
                "Unique",
                "Min Length",
                "Max Length",
                "Min Value",
                "Max Value",
                "Value Range",
                width1 = name_length,
                width2 = 60
            ).unwrap_or_default();
            write!(f, "+{:<}+\n", "-".repeat(name_length + 178)).unwrap_or_default();
            write!(
                f,
                "|{:<width1$}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<60}|\n",
                self.name,
                self.data_type,
                self.nullable.unwrap_or_default(),
                self.unique.unwrap_or_default(),
                self.min_length.unwrap_or_default(),
                self.max_length.unwrap_or_default(),
                self.min_value.unwrap_or_default(),
                self.max_value.unwrap_or_default(),
                trimmed_range, 
                width1 = name_length,
                ).unwrap_or_default();
            write!(f, "+{:<}+\n", "-".repeat(name_length + 178))

        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct ValidationSet {
        pub name: String,
        pub set: Vec<Validation>,
    }

    impl ValidationSet {
        pub fn new(data: &DataFrame, constraint_set: &ConstraintSet) -> ValidationSet {
            //let columns: Vec<&str> = data.get_column_names();
            let mut validation_set: Vec<Validation> = vec![];
            for c in &constraint_set.set {
                let validation = Validation::new(&data, &c);
                validation_set.push(validation)
            }
            return ValidationSet {
                name: String::from("XXX"),
                set: validation_set,
            }
        }
        pub fn save_csv<T: AsRef<Path>>(&self, filepath: T) -> Result<(), String> {
            let file_path = filepath.as_ref();

            if let Some(ext) = file_path.extension() {
                if ext != "csv" {
                    return Err(String::from("File path must have the .csv extension"));
                }
            } else {
                return Err(String::from("File path must have the .csv extension"));
            }

            let file = File::create(file_path).map_err(|e| format!("Failed to create file: {}", e))?;
            let mut writer = Writer::from_writer(file);

            for validation in &self.set {
                writer.serialize(validation).map_err(|e| format!("Failed to serialize data: {}", e))?;
            }
            Ok(())
        }
    }
    impl fmt::Display for ValidationSet {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let mut name_length: usize = 0;
            for validation in &self.set {
                if validation.name.len().to_usize().unwrap_or_default() > name_length {
                    name_length = validation.name.len().to_usize().unwrap_or_default()
                }
            }
            write!(f, "+{:<}{:<}+\n",
                "-".repeat(name_length),
                "-".repeat(149-name_length)).unwrap_or_default();
            write!(
                f,
                "| {:<width1$}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10} |\n",
                "Name",
                "Data Type",
                "Nullable",
                "Unique",
                "Min Length",
                "Max Length",
                "Min Value",
                "Max Value",
                "Value Range",
                width1 = name_length,
            )
            .unwrap_or_default();
            write!(f, "+{:<}{:<}+\n",
                "-".repeat(name_length),
                "-".repeat(149-name_length)).unwrap_or_default();
            for validation in &self.set {
               write!(
                    f,
                    "| {:<width1$}\t| {:<11}\t| {:<11}\t| {:<11}\t| {:<11}\t| {:<11}\t| {:<11}\t| {:<11}\t| {:<11} |\n",
                    validation.name,
                    validation.data_type,
                    validation.nullable.unwrap_or_default(),
                    validation.unique.unwrap_or_default(),
                    validation.min_length.unwrap_or_default(),
                    validation.max_length.unwrap_or_default(),
                    validation.min_value.unwrap_or_default(),
                    validation.max_value.unwrap_or_default(),
                    validation.value_range.unwrap_or_default(),
                    width1 = name_length,
                ).unwrap_or_default();
                write!(f, "+{:<}{:<}+\n",
                    "-".repeat(name_length),
                    "-".repeat(149-name_length)).unwrap_or_default();
            }
            Ok(())
        }
    }
}
