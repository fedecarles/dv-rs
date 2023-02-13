pub mod constraints {
    use cli_table::{Cell, Style, Table};
    use polars::prelude::*;
    use std::collections::HashSet;
    use std::fmt;

    pub struct Constraint {
        pub name: String,
        pub data_type: String,
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
            let col = data.column(&colname);

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
            let col = data.column(&colname);
            match col {
                Ok(s) => s.dtype().to_string(),
                Err(_) => String::from("Not a DataFrame"),
            }
        }

        fn _is_nullable(data: &DataFrame, colname: &str) -> bool {
            let col = data.column(&colname);
            let is_null = match col {
                Ok(s) => s.is_null().any(),
                Err(_) => false,
            };
            is_null
        }

        fn _is_unique(data: &DataFrame, colname: &str) -> bool {
            let col = data.column(&colname);
            let is_unique = match col {
                Ok(s) => s.is_unique().unwrap_or_default().all(),
                Err(_) => false,
            };
            is_unique
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
            let table = vec![
                vec!["Name".cell(), self.name.as_str().cell()],
                vec!["Data Type".cell(), self.data_type.as_str().cell()],
                vec!["Duplicated".cell(), self.unique.cell()],
                vec![
                    "Min Lenght".cell(),
                    self.min_length.unwrap_or_default().cell(),
                ],
                vec![
                    "Max Lenght".cell(),
                    self.max_length.unwrap_or_default().cell(),
                ],
                vec![
                    "Min Value".cell(),
                    self.min_value.unwrap_or_default().cell(),
                ],
                vec![
                    "Max Value".cell(),
                    self.max_value.unwrap_or_default().cell(),
                ],
                vec![
                    "Value Range".cell(),
                    self.value_range.clone().unwrap_or_default().cell(),
                ],
            ]
            .table()
            .title(vec![
                "Constraint Type".cell().bold(true),
                "Constraint Value".cell().bold(true),
            ])
            .bold(true);

            let table_display = table.display().unwrap();
            write!(f, "{}", table_display)
        }
    }

    pub fn get_constraint_set(data: &DataFrame) -> Vec<Constraint> {
        let columns: Vec<&str> = data.get_column_names();
        let mut constraint_set: Vec<Constraint> = vec![];
        for col in columns {
            let constraint = Constraint::get_col_constraints(&data, &col);
            constraint_set.push(constraint)
        }
        constraint_set
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
            nullable.push(c.nullable);
            unique.push(c.unique);
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
