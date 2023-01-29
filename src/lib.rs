use polars::prelude::*;
use std::fmt;

pub struct Constraint {
    name: String,
    data_type: String,
    nullable: bool,
    unique: bool,
    min_length: Option<u32>,
    max_length: Option<u32>,
    min_value: Option<f32>,
    max_value: Option<f32>,
    value_range: Option<Series>,
}

impl Constraint {
    fn _get_string_unique(data: &DataFrame, colname: &str) -> Option<Series> {
        let col = data.column(&colname);
        let dtype: String = match col {
            Ok(s) => s.dtype().to_string(),
            Err(_) => String::from("Not a DataFrame"),
        };

        if dtype != "str" {
            return None;
        } else {
            let unique = match col {
                Ok(s) => s.unique(),
                Err(e) => Err(e),
            };
            match unique {
                Ok(u) => Some(u),
                Err(_) => None,
            }
        }
    }

    fn _get_data_type(data: &DataFrame, colname: &str) -> String {
        let col = data.column(&colname);
        match col {
            Ok(s) => s.dtype().to_string(),
            Err(_) => String::from("Not a DataFrame"),
        }
    }

    fn get_col_constraints(data: &DataFrame, colname: &str) -> Constraint {
        let attribute_contraints = Constraint {
            name: String::from(colname),
            data_type: Self::_get_data_type(data, colname),
            // data_type: data.column(&colname).unwrap().dtype().to_string(),
            nullable: data.column(&colname).unwrap().is_null().any(),
            unique: data.column(&colname).unwrap().is_unique().unwrap().all(),
            min_length: data
                .column(&colname)
                .unwrap()
                .utf8()
                .map(|s| s.str_lengths().min())
                .unwrap_or(None),
            max_length: data
                .column(&colname)
                .unwrap()
                .utf8()
                .map(|s| s.str_lengths().min())
                .unwrap_or(None),
            min_value: data.column(&colname).unwrap().min(),
            max_value: data.column(&colname).unwrap().max(),
            value_range: Self::_get_string_unique(data, &colname),
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
        Nullable: {}
        Unique: {}
        Min Length: {:?}
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

pub fn frame_constraints(data: &DataFrame) {
    let columns: Vec<&str> = data.get_column_names();

    let mut name: Vec<String> = vec![];
    let mut dtype: Vec<String> = vec![];
    let mut nullable: Vec<bool> = vec![];
    let mut unique: Vec<bool> = vec![];
    let mut min_length: Vec<Option<u32>> = vec![];
    let mut max_length: Vec<Option<u32>> = vec![];
    let mut min_value: Vec<Option<f32>> = vec![];
    let mut max_value: Vec<Option<f32>> = vec![];
    let mut value_range: Vec<Option<Series>> = vec![];

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
        "Max Length" => &min_length,
        "Min Value" => &min_value,
        "Max Value" => &max_value,
        "Value Range" => &value_range
    ];
    println!("{:?}", frame.unwrap());
}
