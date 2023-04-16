pub mod validation {
    use crate::constraints::constraints::*;
    use cli_table::{Cell, Style, Table};
    use polars::{export::num::ToPrimitive, prelude::*};
    use std::fmt;

    pub struct Validation {
        pub name: String,
        pub data_type: Option<bool>,
        pub nullable: Option<u32>,
        pub unique: Option<u32>,
        pub min_length: Option<u32>,
        pub max_length: Option<u32>,
        pub min_value: Option<u32>,
        pub max_value: Option<u32>,
        pub value_range: Option<u32>,
    }

    impl Validation {
        fn _check_data_type(data: &DataFrame, constraint: &Constraint) -> Option<bool> {
            let col = data.column(&constraint.name);
            let dtype: String = match col {
                Ok(s) => s.dtype().clone().to_string(),
                Err(_) => DataType::Null.to_string(),
            };
            if dtype == constraint.data_type {
                Some(true)
            } else {
                Some(false)
            }
        }
        fn _check_nullable(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            if constraint.nullable == true {
                return None;
            } else {
                let col = data.column(&constraint.name);
                return match col {
                    Ok(s) => s.null_count().to_u32(),
                    Err(_) => None,
                };
            }
        }
        fn _check_duplicates(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            if constraint.unique == true {
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
            let col = data.column(&constraint.name);

            match col {
                Ok(s) => s
                    .lt(constraint.min_value.unwrap_or_default())
                    .unwrap_or_default()
                    .sum(),
                Err(_) => None,
            }
        }

        fn _check_max_value(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            let col = data.column(&constraint.name);

            match col {
                Ok(s) => s
                    .gt(constraint.min_value.unwrap_or_default())
                    .unwrap_or_default()
                    .sum(),
                Err(_) => None,
            }
        }
        fn _check_value_range(data: &DataFrame, constraint: &Constraint) -> Option<u32> {
            let col = data.column(&constraint.name);
            let ranges_string = &constraint.value_range;
            let ranges: Vec<String> = match ranges_string {
                Some(s) => s.split(", ").map(str::to_string).collect(),
                None => vec![String::from("null")],
            };
            match col {
                Ok(s) => s
                    .is_in(&Series::new("ranges", &ranges))
                    .map(|b| !b)
                    .unwrap_or_default()
                    .sum(),
                Err(_) => None,
            }
        }

        pub fn get_col_validations(data: &DataFrame, constraint: &Constraint) -> Validation {
            let attribute_validations = Validation {
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
            attribute_validations
        }
    }

    impl fmt::Display for Validation {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let table = vec![
                vec!["Name".cell(), self.name.as_str().cell()],
                vec![
                    "Data Type".cell(),
                    self.data_type.unwrap_or_default().cell(),
                ],
                vec!["Duplicated".cell(), self.unique.unwrap_or_default().cell()],
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
                    self.value_range.unwrap_or_default().cell(),
                ],
            ]
            .table()
            .title(vec![
                "Constraint Type".cell().bold(true),
                "Number of Breaks".cell().bold(true),
            ])
            .bold(true);

            let table_display = table.display().unwrap();
            write!(f, "{}", table_display)
        }
    }

    pub fn frame_validation(
        data: &DataFrame,
        constraint_set: &Vec<Constraint>,
    ) -> PolarsResult<DataFrame> {
        //let columns: Vec<&str> = data.get_column_names();

        let mut name: Vec<String> = vec![];
        let mut dtype: Vec<bool> = vec![];
        let mut nullable: Vec<u32> = vec![];
        let mut unique: Vec<u32> = vec![];
        let mut min_length: Vec<Option<u32>> = vec![];
        let mut max_length: Vec<Option<u32>> = vec![];
        let mut min_value: Vec<Option<u32>> = vec![];
        let mut max_value: Vec<Option<u32>> = vec![];
        let mut value_range: Vec<Option<u32>> = vec![];

        for c in constraint_set {
            let v = Validation::get_col_validations(data, &c);
            name.push(v.name);
            dtype.push(v.data_type.unwrap_or_default());
            nullable.push(v.nullable.unwrap_or_default());
            unique.push(v.unique.unwrap_or_default());
            min_length.push(v.min_length);
            max_length.push(v.max_length);
            min_value.push(v.min_value);
            max_value.push(v.max_value);
            value_range.push(v.value_range);
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
        frame
    }
}
