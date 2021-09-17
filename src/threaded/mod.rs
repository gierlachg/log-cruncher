use std::borrow::Cow;
use std::convert::TryFrom;
use std::fs::File;
use std::io::Read;

use crossbeam::channel::{Receiver, Sender};
use serde::Deserialize;

use CruncherError::GenericFailure;

use crate::{CruncherError, Report};

const LINE_DELIMITER: u8 = b'\n';
const CHUNK_SIZE: usize = 8 * 1024 * 1024;
const MAX_NUMBER_IN_FLIGHT_CHUNKS: usize = 32; // effectively 32 + number of CPUs

pub(super) fn crunch(file: File) -> Result<Report, CruncherError> {
    let (chunks_sender, chunks_receiver) = crossbeam::channel::bounded(MAX_NUMBER_IN_FLIGHT_CHUNKS);
    let (response_sender, response_receiver) = crossbeam::channel::unbounded();

    spawn_crunchers(&file, chunks_receiver, response_sender)?;
    chunk_file(file, chunks_sender)?;

    Ok(response_receiver.into_iter().fold(Report::new(), Report::merge))
}

fn chunk_file(mut file: File, chunks: Sender<Vec<u8>>) -> Result<(), CruncherError> {
    let mut bytes = Vec::with_capacity(CHUNK_SIZE);
    loop {
        let bytes_read = file
            .by_ref()
            .take(u64::try_from(CHUNK_SIZE - bytes.len()).expect("Unable to covert"))
            .read_to_end(&mut bytes)?;
        if bytes_read == 0 {
            break;
        }

        match find_delimiter_index(&bytes, LINE_DELIMITER) {
            Some(delimiter_index) => {
                let mut next_bytes = Vec::with_capacity(CHUNK_SIZE);
                next_bytes.extend_from_slice(&bytes[delimiter_index..]);
                bytes.truncate(delimiter_index);
                chunks.send(bytes).map_err(|e| GenericFailure(e.into()))?;
                bytes = next_bytes;
            }
            None => {
                // with huge JSON objects, if single read is not enough, one could grow the buffer
                // however, here, the assumption is that file is corrupted
                return Err(GenericFailure("Corrupted file".into()));
            }
        }
    }
    Ok(())
}

fn find_delimiter_index(bytes: &[u8], delimiter: u8) -> Option<usize> {
    let mut index = bytes.len() - 1;
    while index > 0 {
        if bytes[index] == delimiter {
            return Some(index + 1);
        }
        index -= 1;
    }
    None
}

fn spawn_crunchers(file: &File, chunks: Receiver<Vec<u8>>, reports: Sender<Report>) -> Result<(), CruncherError> {
    let metadata = file.metadata()?;
    let number_of_crunchers = usize::min(
        num_cpus::get(),
        usize::max(
            1,
            usize::try_from(metadata.len()).expect("Unable to convert") / CHUNK_SIZE,
        ),
    );
    (0..number_of_crunchers).for_each(|_| spawn_cruncher(chunks.clone(), reports.clone()));
    Ok(())
}

fn spawn_cruncher(chunks: Receiver<Vec<u8>>, reports: Sender<Report>) {
    std::thread::spawn(move || {
        let mut report = Report::new();
        while let Ok(bytes) = chunks.recv() {
            bytes
                .split(|char| *char == LINE_DELIMITER)
                .filter(|bytes| bytes.len() > 0)
                .for_each(|bytes| match serde_json::from_slice::<'_, Object>(bytes) {
                    Ok(object) => match object.r#type.as_ref() {
                        Some(r#type) => report.update(r#type, 1, bytes.len()),
                        None => report.on_error(),
                    },
                    Err(_) => report.on_error(),
                });
        }
        // main thread has already left
        reports.send(report).unwrap_or(());
    });
}

#[derive(Deserialize)]
struct Object<'a> {
    r#type: Option<Cow<'a, str>>,
}
