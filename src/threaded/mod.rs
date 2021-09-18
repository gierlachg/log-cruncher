use std::convert::TryFrom;
use std::fs::File;
use std::io::Read;

use crossbeam::channel::{self, Receiver, Sender};
use serde::Deserialize;

use CruncherError::GenericFailure;

use crate::{CruncherError, Report};

const LINE_DELIMITER: u8 = b'\n';
const CHUNK_SIZE: usize = 1 * 1024 * 1024;
const MAX_NUMBER_IN_FLIGHT_CHUNKS: usize = 32; // effectively 32 + number of CPUs

pub(super) fn crunch(file: File) -> Result<Report, CruncherError> {
    let (chunks_sender, chunks_receiver) = channel::bounded(MAX_NUMBER_IN_FLIGHT_CHUNKS);
    let (reports_sender, reports_receiver) = channel::unbounded();

    spawn_processors(file.metadata()?.len(), chunks_receiver, reports_sender);
    chunk_file(file, chunks_sender)?;

    Ok(reports_receiver.into_iter().fold(Report::new(), Report::merge))
}

fn chunk_file(mut file: File, chunks: Sender<Vec<u8>>) -> Result<(), CruncherError> {
    let mut current_chunk = Vec::with_capacity(CHUNK_SIZE);
    loop {
        let bytes_read = file
            .by_ref()
            .take(u64::try_from(CHUNK_SIZE - current_chunk.len()).expect("Unable to covert"))
            .read_to_end(&mut current_chunk)?;
        if bytes_read == 0 {
            break;
        }
        match current_chunk.iter().rposition(|byte| byte == &LINE_DELIMITER) {
            Some(delimiter_index) => {
                let mut next_chunk = Vec::with_capacity(CHUNK_SIZE);
                next_chunk.extend_from_slice(&current_chunk[(delimiter_index + 1)..]);
                current_chunk.truncate(delimiter_index + 1);
                chunks.send(current_chunk).map_err(|e| GenericFailure(e.into()))?;
                current_chunk = next_chunk;
            }
            None => {
                // with huge JSON objects, if single chunk is not enough, one could grow the buffer
                // however, here, the assumption is that file is corrupted
                return Err(GenericFailure("Corrupted file".into()));
            }
        }
    }
    Ok(())
}

fn spawn_processors(file_size: u64, chunks: Receiver<Vec<u8>>, reports: Sender<Report>) {
    let number_of_processors = usize::min(
        num_cpus::get(),
        usize::max(1, usize::try_from(file_size).expect("Unable to convert") / CHUNK_SIZE),
    );
    (0..number_of_processors).for_each(|_| spawn_processor(SerdeChunkProcessor(), chunks.clone(), reports.clone()));
}

fn spawn_processor<T: ChunkProcessor>(processor: T, chunks: Receiver<Vec<u8>>, reports: Sender<Report>) {
    std::thread::spawn(move || {
        let mut report = Report::new();
        chunks
            .into_iter()
            .for_each(|chunk| processor.process(&chunk, &mut report));
        // main thread has already left
        reports.send(report).unwrap_or(());
    });
}

trait ChunkProcessor: Send + 'static {
    fn process(&self, chunk: &[u8], report: &mut Report);
}

struct SerdeChunkProcessor();

impl ChunkProcessor for SerdeChunkProcessor {
    fn process(&self, chunk: &[u8], report: &mut Report) {
        chunk
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
}

#[derive(Deserialize)]
struct Object<'a> {
    r#type: Option<&'a str>,
}
