pub mod constraints {
    use polars::export::num::ToPrimitive;
    use polars::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashSet;
    use std::fmt;
    use std::str::FromStr;
    use std::io::Write;
    use std::path::Path;
    use std::fs::File;
    use std::io::prelude::*;


    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct Constraint {
        pub name: String,
        pub data_type: String,
        pub nullable: bool,
        pub unique: bool,
        pub min_length: Option<u32>,
        pub max_length: Option<u32>,
        pub min_value: Option<f64>,
        pub max_value: Option<f64>,
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

        fn _get_data_type(data: &DataFrame, colname: &str) -> String {
            data.column(colname)
                .map(|s| s.dtype())
                .unwrap_or(&DataType::Null)
                .clone().to_string()
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

        fn _get_min_value(data: &DataFrame, colname: &str) -> Option<f64> {
            data.column(colname).ok().and_then(|s| s.min())
        }

        fn _get_max_value(data: &DataFrame, colname: &str) -> Option<f64> {
            data.column(colname).ok().and_then(|s| s.max())
        }

        pub fn new(data: &DataFrame, colname: &str) -> Constraint {
            return Constraint {
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
                "|{:<width1$}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<10}\t| {:<60}|\n",
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
                ).unwrap_or_default();
            write!(f, "+{:<}+\n", "-".repeat(name_length + 178))
        }
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
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
                        "str" => constraint.data_type = DataType::Utf8.to_string(),
                        "int" => constraint.data_type = DataType::Int32.to_string(),
                        "float" => constraint.data_type = DataType::Float64.to_string(),
                        "date" => constraint.data_type = DataType::Date.to_string(),
                        _ => constraint.data_type = DataType::Null.to_string(),
                    },
                    "nullable" => constraint.nullable = bool::from_str(value).unwrap_or_default(),
                    "unique" => constraint.unique = bool::from_str(value).unwrap_or_default(),
                    "min_length" => constraint.min_length = u32::from_str(value).ok(),
                    "max_length" => constraint.max_length = u32::from_str(value).ok(),
                    "min_value" => constraint.min_value = f64::from_str(value).ok(),
                    "max_value" => constraint.max_value = f64::from_str(value).ok(),
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

        pub fn read_constraints(filepath: &str) -> Result<ConstraintSet, Box<dyn std::error::Error>> {
            let mut file = File::open(filepath)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            let set: ConstraintSet = serde_json::from_str(&contents)?;
            Ok(set)
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

mod tests {

    use super::constraints::*;
    use polars::prelude::*;

    #[test]
    fn generate_constraint() {
        let df: DataFrame = CsvReader::from_path("test_data/brain_stroke.csv")
            .unwrap()
            .finish()
            .unwrap();

        // Test constraint creation.
        let constraint = Constraint::new(&df, "age");
        assert_eq!(constraint.name, "age");
        assert_eq!(constraint.data_type, "f64");
        assert_eq!(constraint.nullable, true);
        assert_eq!(constraint.unique, false);
        assert_eq!(constraint.min_length, None);
        assert_eq!(constraint.max_length, None);
        assert_eq!(constraint.min_value, Some(0.08));
        assert_eq!(constraint.max_value, Some(82.00));
        assert_eq!(constraint.value_range, None);

        let constraint = Constraint::new(&df, "Residence_type");
        assert_eq!(constraint.name, "Residence_type");
        assert_eq!(constraint.data_type, "str");
        assert_eq!(constraint.nullable, false);
        assert_eq!(constraint.unique, false);
        assert_eq!(constraint.min_length, Some(5));
        assert_eq!(constraint.max_length, Some(5));
        assert_eq!(constraint.min_value, None);
        assert_eq!(constraint.max_value, None);
        assert_eq!(constraint.value_range, Some("Urban, Rural".to_string()));
    }

    #[test]
    fn save_and_load_set() {
        let df: DataFrame = CsvReader::from_path("test_data/brain_stroke.csv")
            .unwrap()
            .finish()
            .unwrap();

        // Test save and load constraint set
        let set = ConstraintSet::new(&df);
        set.save_json("test_data/saved_constraints.json");
        let new_set = ConstraintSet::read_constraints("test_data/saved_constraints.json");

        assert_eq!(set, new_set.unwrap());

    }

}
