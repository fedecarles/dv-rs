pub mod constraints {
    use polars::prelude::*;
    use std::collections::HashSet;
    use std::fmt;

    pub struct Constraint {
        pub name: String,
        pub data_type: String,
        pub nullable: Result<bool, PolarsError>,
        pub unique: Result<bool, PolarsError>,
        pub min_length: Option<u32>,
        pub max_length: Option<u32>,
        pub min_value: Option<f32>,
        pub max_value: Option<f32>,
        pub value_range: Option<String>,
    }

    impl Constraint {
        fn _get_value_range(data: &DataFrame, colname: &str) -> Option<String> {
            let col = data.column(&colname);

            let dtype: String = match col {
                Ok(s) => s.dtype().to_string(),
                Err(_) => String::from("Not a DataFrame"),
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
                let unique_str = Some(unique_vec.join(", "));
                unique_str
            }
        }

        fn _get_data_type(data: &DataFrame, colname: &str) -> String {
            let col = data.column(&colname);
            match col {
                Ok(s) => s.dtype().to_string(),
                Err(_) => String::from("Not a DataFrame"),
            }
        }

        fn _is_nullable(data: &DataFrame, colname: &str) -> Result<bool, PolarsError> {
            let col = data.column(&colname)?;
            let is_null = col.is_null().any();
            Ok(is_null)
        }

        fn _is_unique(data: &DataFrame, colname: &str) -> Result<bool, PolarsError> {
            let col = data.column(&colname)?;
            let is_null = col.is_unique()?.all();
            Ok(is_null)
        }

        fn _get_min_length(data: &DataFrame, colname: &str) -> Option<u32> {
            let col = data.column(&colname).ok();
            let min_length = col?.utf8().map(|s| s.str_lengths().min());
            match min_length {
                Ok(s) => s,
                Err(_) => None,
            }
        }

        fn _get_max_length(data: &DataFrame, colname: &str) -> Option<u32> {
            let col = data.column(&colname).ok();
            let min_length = col?.utf8().map(|s| s.str_lengths().max());
            match min_length {
                Ok(s) => s,
                Err(_) => None,
            }
        }

        fn _get_min_value(data: &DataFrame, colname: &str) -> Option<f32> {
            let col = data.column(&colname);
            let min_value = match col {
                Ok(s) => s.min(),
                Err(_) => None,
            };
            min_value
        }

        fn _get_max_value(data: &DataFrame, colname: &str) -> Option<f32> {
            let col = data.column(&colname);
            let min_value = match col {
                Ok(s) => s.max(),
                Err(_) => None,
            };
            min_value
        }

        pub fn get_col_constraints(data: &DataFrame, colname: &str) -> Constraint {
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
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "
                Name: {}
                Data Type: {}
                Nullable: {:?}
                Unique: {:?}
                Mix Length: {:?}
                Max Length: {:?}
                Min Value: {:?}
                Max Value: {:?}
                Value Range: {:?}
            ",
                self.name,
                self.data_type,
                self.nullable,
                self.unique,
                self.min_length,
                self.max_length,
                self.min_value,
                self.max_value,
                self.value_range
            )
        }
    }

    pub fn frame_constraints(data: &DataFrame) -> PolarsResult<DataFrame> {
        let columns: Vec<&str> = data.get_column_names();

        let mut name: Vec<String> = vec![];
        let mut dtype: Vec<String> = vec![];
        let mut nullable: Vec<bool> = vec![];
        let mut unique: Vec<bool> = vec![];
        let mut min_length: Vec<Option<u32>> = vec![];
        let mut max_length: Vec<Option<u32>> = vec![];
        let mut min_value: Vec<Option<f32>> = vec![];
        let mut max_value: Vec<Option<f32>> = vec![];
        let mut value_range: Vec<Option<String>> = vec![];

        for col in columns {
            let c = Constraint::get_col_constraints(data, col);
            name.push(c.name);
            dtype.push(c.data_type);
            nullable.push(c.nullable?);
            unique.push(c.unique?);
            min_length.push(c.min_length);
            max_length.push(c.max_length);
            min_value.push(c.min_value);
            max_value.push(c.max_value);
            value_range.push(c.value_range);
        }

        let frame: PolarsResult<DataFrame> = df![
            "Attribute" => &name,
            "Data Type" => &dtype,
            "Nullable" => &nullable,
            "Unique" => &unique,
            "Min Length" => &min_length,
            "Max Length" => &max_length,
            "Min Value" => &min_value,
            "Max Value" => &max_value,
            "Value Range" => &value_range
        ];
        println!("{:?}", frame);
        frame
    }
}
