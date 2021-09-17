#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use thiserror::Error;

mod threaded;

/// Reads a file with given `path` where each line (separated by `0xA` byte) is an arbitrary JSON object that includes
/// a field called `type`. It outputs a [Report] containing the number of objects with each `type`, and their total size
/// in bytes.
pub fn crunch(path: &str) -> Result<Report, CruncherError> {
    let file = std::fs::File::open(path)?;
    threaded::crunch(file)
}

pub struct Report {
    errors: usize,
    histogram: HashMap<String, Statistics>,
}

impl Report {
    fn new() -> Self {
        Report {
            errors: 0,
            histogram: HashMap::new(),
        }
    }

    fn on_error(&mut self) {
        self.errors += 1;
    }

    fn update(&mut self, r#type: &str, cardinality: usize, number_of_bytes: usize) {
        match self.histogram.get_mut(r#type) {
            Some(statistics) => statistics.update(cardinality, number_of_bytes),
            None => {
                self.histogram
                    .insert(r#type.to_string(), Statistics::new(cardinality, number_of_bytes));
            }
        }
    }

    fn merge(mut self, other: Report) -> Self {
        self.errors += other.errors;
        other
            .histogram
            .into_iter()
            .for_each(|entry| self.update(&entry.0, entry.1.cardinality, entry.1.number_of_bytes));
        self
    }
}

impl Display for Report {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "{: <16} | {: >16} | {: >16}|\n",
            "Type", "Cardinality", "Bytes"
        )?;
        write!(formatter, "-------------------------------------------------------\n")?;
        for (key, statistics) in self.histogram.iter() {
            write!(
                formatter,
                "{: <16} | {: >16} | {: >16}|\n",
                key, statistics.cardinality, statistics.number_of_bytes
            )?
        }
        write!(formatter, "-------------------------------------------------------\n\n")?;
        write!(formatter, "Number of erroneous objects: {}\n", self.errors)
    }
}

pub struct Statistics {
    cardinality: usize,
    number_of_bytes: usize,
}

impl Statistics {
    fn new(cardinality: usize, number_of_bytes: usize) -> Self {
        Statistics {
            cardinality,
            number_of_bytes,
        }
    }

    fn update(&mut self, cardinality: usize, number_of_bytes: usize) {
        self.cardinality += cardinality;
        self.number_of_bytes += number_of_bytes;
    }
}

#[derive(Error, Debug)]
pub enum CruncherError {
    #[error("error")]
    GenericFailure(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("io error")]
    IOFailure(#[from] std::io::Error),
    #[error("serialization error")]
    SerializationFailure(#[from] serde_json::Error),
}
