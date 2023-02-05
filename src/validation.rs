pub mod validation {
    use crate::constraints::constraints::*;
    use polars::{export::num::ToPrimitive, prelude::*};

    //use std::collections::HashSet;
    //use std::fmt;

    pub struct Validation {
        pub name: String,
        pub data_type: String,
        pub nullable: Option<u32>,
        pub unique: Option<u32>,
        pub min_length: Option<u32>,
        pub max_length: Option<u32>,
        pub min_value: Option<u32>,
        pub max_value: Option<u32>,
        pub value_range: Option<u32>,
    }

    impl Validation {
        pub fn check_nullable(data: &DataFrame, constraint: Constraint) -> Option<u32> {
            if constraint.nullable.unwrap_or_default() == false {
                return None;
            } else {
                let col = data.column(&constraint.name);
                return match col {
                    Ok(s) => s.null_count().to_u32(),
                    Err(_) => None,
                };
            }
        }
        pub fn check_duplicates(data: &DataFrame, constraint: Constraint) -> Option<u32> {
            if constraint.unique.unwrap_or_default() == false {
                return None;
            } else {
                let col = data.column(&constraint.name);
                return match col {
                    Ok(s) => s.is_unique().iter().count().to_u32(),
                    Err(_) => None,
                };
            }
        }
        pub fn check_min_length(data: &DataFrame, constraint: Constraint) -> Option<u32> {
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
        pub fn check_max_length(data: &DataFrame, constraint: Constraint) -> Option<u32> {
            let col = data.column(&constraint.name);
            let max_length = match col {
                Ok(s) => s.utf8().map(|s| s.str_n_chars()),
                Err(e) => Err(e),
            };
            match max_length {
                Ok(s) => s
                    .lt(constraint.max_length.unwrap_or_default())
                    .into_iter()
                    .filter(|b| b.unwrap_or_default() == true)
                    .count()
                    .to_u32(),
                Err(_) => None,
            }
        }
    }
}
