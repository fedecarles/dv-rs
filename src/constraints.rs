pub mod constraints {
    use cli_table::{Cell, Style, Table};
    use polars::export::num::ToPrimitive;
    use polars::prelude::*;
    use polars::prelude::*;
    use serde::__private::ser::constrain;
    use serde::{Deserialize, Serialize};
    use serde_json::{Value};
    use std::collections::HashSet;
    use std::fmt;
    use std::str::FromStr;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;


    #[derive(Serialize, Deserialize, Debug)]
    pub struct Constraint {
        pub name: String,
        #[serde(skip_deserializing, skip_serializing)]
        pub data_type: DataType,
        pub nullable: bool,
        pub unique: bool,
        pub min_length: Option<u32>,
        pub max_length: Option<u32>,
        pub min_value: Option<f32>,
        pub max_value: Option<f32>,
        pub value_range: Option<String>,
    }

    impl Constraint {
        fn _get_value_range(data: &DataFrame, colname: &str) -> Option<String> {
            let col = data.column(colname);

            let dtype: String = match col {
                Ok(s) => s.dtype().to_string(),
                Err(_) => String::from("Not a DataType"),
            };

            if dtype != "str" {
                return None;
            } else {
                let mut unique_values = HashSet::new();
                let series = col.cloned().unwrap_or_default();
                for value in series.iter() {
                    unique_values.insert(value.to_string());
                }
                let unique_vec = unique_values.into_iter().collect::<Vec<String>>();
                let unique_str = Some(unique_vec.join(", ").replace(['\\', '"'], ""));
                unique_str
            }
        }

        fn _get_data_type(data: &DataFrame, colname: &str) -> DataType {
            data.column(colname)
                .map(|s| s.dtype())
                .unwrap_or(&DataType::Null)
                .clone()
        }

        fn _is_nullable(data: &DataFrame, colname: &str) -> bool {
            data.column(colname)
                .map(|s| s.is_null().any())
                .unwrap_or_default()
        }

        fn _is_unique(data: &DataFrame, colname: &str) -> bool {
            data.column(colname)
                .map(|s| s.is_unique().unwrap_or_default().all())
                .unwrap_or_default()
        }

        fn _get_min_length(data: &DataFrame, colname: &str) -> Option<u32> {
            data.column(colname)
                .ok()
                .and_then(|s| s.utf8().map(|s| s.str_lengths().min()).unwrap_or_default())
        }

        fn _get_max_length(data: &DataFrame, colname: &str) -> Option<u32> {
            data.column(colname)
                .ok()
                .and_then(|s| s.utf8().map(|s| s.str_lengths().max()).unwrap_or_default())
        }

        fn _get_min_value(data: &DataFrame, colname: &str) -> Option<f32> {
            data.column(colname).ok().and_then(|s| s.min())
        }

        fn _get_max_value(data: &DataFrame, colname: &str) -> Option<f32> {
            data.column(colname).ok().and_then(|s| s.max())
        }

        pub fn new(data: &DataFrame, colname: &str) -> Constraint {
            let attribute_contraints = Constraint {
                name: String::from(colname),
                data_type: Self::_get_data_type(data, colname),
                nullable: Self::_is_nullable(data, colname),
                unique: Self::_is_unique(data, colname),
                min_length: Self::_get_min_length(data, colname),
                max_length: Self::_get_max_length(data, colname),
                min_value: Self::_get_min_value(data, colname),
                max_value: Self::_get_max_value(data, colname),
                value_range: Self::_get_value_range(data, &colname),
            };
            attribute_contraints
        }
    }

    impl fmt::Display for Constraint {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
                "|{:<width1$}\t| {:<width2$}\t| {:<width3$}\t| {:<width4$}\t| {:<width5$}\t| {:<width6$}\t| {:<width7$}\t| {:<width8$}\t| {:<width9$}|\n",
                self.name,
                self.data_type.to_string(),
                self.nullable,
                self.unique,
                self.min_length.unwrap_or_default(),
                self.max_length.unwrap_or_default(),
                self.min_value.unwrap_or_default(),
                self.max_value.unwrap_or_default(),
                trimmed_range, 
                width1 = name_length,
                width2 = 10,
                width3 = 10,
                width4 = 10,
                width5 = 10,
                width6 = 10,
                width7 = 10,
                width8 = 10,
                width9 = 60
                ).unwrap_or_default();
            write!(f, "+{:<}+\n", "-".repeat(name_length + 178))
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct ConstraintSet {
        pub name: String,
        pub set: Vec<Constraint>,
    }

    impl ConstraintSet {
        pub fn new(data: &DataFrame) -> ConstraintSet {
            let columns: Vec<&str> = data.get_column_names();
            let mut constraint_set: Vec<Constraint> = vec![];
            for col in columns {
                let constraint = Constraint::new(&data, &col);
                constraint_set.push(constraint)
            }
            ConstraintSet {
                name: String::from("XXX"),
                set: constraint_set,
            }
        }

        pub fn modify(&mut self, name: &str, ctype: &str, value: &str) -> () {
            if let Some(constraint) = self.set.iter_mut().find(|c| c.name == name) {
                match ctype {
                    "data_type" => match value {
                        "str" => constraint.data_type = DataType::Utf8,
                        "int" => constraint.data_type = DataType::Int32,
                        "float" => constraint.data_type = DataType::Float64,
                        "date" => constraint.data_type = DataType::Date,
                        _ => constraint.data_type = DataType::Null,
                    },
                    "nullable" => constraint.nullable = bool::from_str(value).unwrap_or_default(),
                    "unique" => constraint.unique = bool::from_str(value).unwrap_or_default(),
                    "min_length" => constraint.min_length = u32::from_str(value).ok(),
                    "max_length" => constraint.max_length = u32::from_str(value).ok(),
                    "min_value" => constraint.min_value = f32::from_str(value).ok(),
                    "max_value" => constraint.max_value = f32::from_str(value).ok(),
                    "value_range" => constraint.value_range = String::from(value).into(),
                    _ => println!("{:?}", "Please provide a valid constraint name."),
                }
                println!("Constraint updated:");
                println!("{}", constraint)
            } else {
                println!("{:?}", "Please provide a valid column name.")
            }
        }

        pub fn save_json(&self, filepath: &str) -> Result<(), String> {

            let json = serde_json::to_string_pretty(self).map_err(|err| err.to_string())?;

            let path = Path::new(filepath);
            let mut file = File::create(&path).map_err(|err| err.to_string())?;
            file.write_all(json.as_bytes()).map_err(|err| err.to_string())?;

            Ok(())
        }
    }

    impl fmt::Display for ConstraintSet {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let mut max_length: usize = 0;
            for constraint in &self.set {
                if constraint.name.len().to_usize().unwrap_or_default() > max_length {
                    max_length = constraint.name.len().to_usize().unwrap_or_default()
                }
            }
            write!(f, "+{:<}+\n", "-".repeat(max_length + 176)).unwrap_or_default();
            write!(
                f,
                "|{:<width1$}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<}\t| {:<56}|\n",
                "Name",
                "Data Type",
                "Nullable",
                "Unique",
                "Min Length",
                "Max Length",
                "Min Value",
                "Max Value",
                "Value Range",
                width1 = max_length,
            )
            .unwrap_or_default();
            write!(f, "+{:<}+\n", "-".repeat(max_length + 176)).unwrap_or_default();
            for constraint in &self.set {
                let range_string = constraint
                    .value_range
                    .clone()
                    .unwrap_or_default()
                    .to_string();
                let trimmed_range = if range_string.len() >= 60 {
                    &range_string[..55]
                } else {
                    &range_string
                };
                write!(
                    f,
                    "|{:<width1$}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<56}|\n",
                    constraint.name,
                    constraint.data_type.to_string(),
                    constraint.nullable,
                    constraint.unique,
                    constraint.min_length.unwrap_or_default(),
                    constraint.max_length.unwrap_or_default(),
                    constraint.min_value.unwrap_or_default(),
                    constraint.max_value.unwrap_or_default(),
                    trimmed_range,
                    width1 = max_length,
                ).unwrap_or_default();
                write!(f, "+{:<}+\n", "-".repeat(max_length + 176)).unwrap_or_default();
            }
            Ok(())
        }
    }
}
