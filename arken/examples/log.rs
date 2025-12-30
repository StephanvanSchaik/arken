use arken::{Arken, Error, Reader, Writer};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use jiff::Timestamp;
use mmap_rs::MmapOptions;
use std::{borrow::Cow, fs::File};

#[derive(Arken, Clone, Debug)]
struct Message<'a> {
    author: Cow<'a, str>,
    message: Cow<'a, str>,
    timestamp: Timestamp,
}

#[derive(Debug, Subcommand)]
enum Command {
    List,
    Add { author: String, message: String },
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

fn round_up(x: usize, align: usize) -> usize {
    (x + align.saturating_sub(1)) & !(align.saturating_sub(1))
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    match &args.command {
        Command::List => {
            let file = File::open("log.bin")?;

            let size = file.metadata()?.len() as usize;
            let size = round_up(size, MmapOptions::page_size());

            if size == 0 {
                return Ok(());
            }

            let map = unsafe { MmapOptions::new(size)?.with_file(&file, 0).map()? };

            let reader = Reader::try_from(&map[..])?;

            for (index, message) in reader.find::<Message>(b"msg").enumerate() {
                if index != 0 {
                    println!();
                }

                println!("Author: {}", message.author);
                println!("Date: {}", message.timestamp.strftime("%Y-%m-%d %H:%M:%S"));
                println!("  {}", message.message);
            }
        }
        Command::Add { author, message } => {
            let timestamp = Timestamp::now();

            let message = Message {
                author: author.into(),
                message: message.into(),
                timestamp,
            };

            let mut writer = match Writer::open("log.bin") {
                Ok(writer) => writer,
                _ => {
                    let writer = Writer::tempfile(Default::default())?;

                    writer.persist("log.bin")?
                }
            };

            let mut bytes = BytesMut::new();

            let _ = writer.append_with_marker(&mut bytes, b"msg", &message)?;

            writer.flush()?;
        }
    }

    Ok(())
}
