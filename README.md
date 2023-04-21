# Csv file validation

1. [What it Does](#what-it-does)
2. [Installation](#installation)
3. [Usage](#usage)
3. [Generating Constraints](#generating-constraints)
4. [Validating Data](#validating-data)

## What it does
db-rs is a tool writen in rust to validate tabular data in csv files. 
It uses the [polars-rs](https://www.pola.rs/) crate read the files as DataFrame objects and perform validations.

The standard constraints include:

* **Null check**: Checks for null values in a DataFrame column.
* **Unique check**: Checks if a column has duplicate values.
* **Max Length**: Checks if a string value in a column exceeds the maximum number of characters.
* **Min Length**: Checks if a string value in a column exceeds the minimum number of characters.
* **Value Range**: Checks if a column has values outside the expected list of values.
* **Max Value**: Checks if a value in a column exceed the expected max value.
* **Min Value**: Checks if a value in a column exceed the expected min value.

The main use case for dv-rs is where a dataset with the same shape and attributes needs to be
validated on a recurring basis.

## Installation

This program requires the [rust](https://www.rust-lang.org/) programming language to run.

```
git clone https://github.com/fedecarles/dv-rs
cd dv-ps
cargo build // build locally
```

## Usage

```
Usage: dvrs [OPTIONS] --file <VALUE>

Options:
  -f, --file <VALUE>      Csv file
  -c, --constraints...    Print Constraints
  -s, --save <VALUE>      Save Constraints
  -v, --validate <VALUE>  Print validation to json
  -o, --output <VALUE>    Save validation to csv
  -h, --help              Print help
  -V, --version           Print version
```

## Generating constraints

A set of constraints can be generated from a csv file by passing the file with the -c (to print to output)
or -s (to save as json)

```
dvrs -f brain_stroke.csv -c
```
dv-rs will attempt to determine the constraints for each column in the file.

```
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Name                   | Data Type     | Nullable      | Unique        | Min Length    | Max Length    | Min Value     | Max Value     | Value Range                                             |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|gender                 | str           | false         | false         | 4             | 6             | 0             | 0             | Female, Male                                            |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|age                    | f64           | true          | false         | 0             | 0             | 0.08          | 82            |                                                         |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|hypertension           | f64           | false         | false         | 0             | 0             | 0             | 2             |                                                         |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|heart_disease          | f64           | false         | false         | 0             | 0             | 0             | 1             |                                                         |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|ever_married           | str           | false         | false         | 2             | 3             | 0             | 0             | Yes, No                                                 |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|work_type              | str           | true          | false         | 7             | 13            | 0             | 0             | Private, null, Govt_job, children, Self-employed        |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Residence_type         | str           | false         | false         | 5             | 5             | 0             | 0             | Urban, Rural                                            |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|avg_glucose_level      | f64           | false         | false         | 0             | 0             | -25           | 700           |                                                         |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|bmi                    | f64           | true          | false         | 0             | 0             | 0             | 100           |                                                         |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|smoking_status         | str           | false         | false         | 6             | 15            | 0             | 0             | smokes, Unknown, formerly smoked, never smoked, bad_cat |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

## Validating data

A set of constraints can be used to validate a different file. 
The validation output can be saved to a csv file with the -o option.

```
dvrs -f test_data/brain_stroke_bad.csv -v test_data/saved_constraints.json
```

```
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| Name                  | Data Type     | Nullable      | Unique        | Min Length    | Max Length    | Min Value     | Max Value     | Value Range |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| gender                | true          | 0             | 0             | 2             | 0             | 0             | 0             | 2           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| age                   | true          | 0             | 0             | 0             | 0             | 0             | 0             | 0           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| hypertension          | true          | 2             | 0             | 0             | 0             | 0             | 0             | 0           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| heart_disease         | true          | 0             | 0             | 0             | 0             | 0             | 0             | 0           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| ever_married          | true          | 3             | 0             | 1             | 0             | 0             | 0             | 4           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| work_type             | true          | 0             | 0             | 0             | 0             | 0             | 0             | 3           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| Residence_type        | true          | 0             | 0             | 0             | 3             | 0             | 0             | 3           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| avg_glucose_level     | true          | 0             | 0             | 0             | 0             | 0             | 2             | 0           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| bmi                   | true          | 0             | 0             | 0             | 0             | 1             | 0             | 0           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| smoking_status        | true          | 0             | 0             | 0             | 0             | 0             | 0             | 0           |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
```

