use arken::{Arken, Error, MappedFile, Writer};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use jiff::Timestamp;
use std::borrow::Cow;

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

fn main() -> Result<(), Error> {
    let args = Args::parse();

    match &args.command {
        Command::List => {
            let file = MappedFile::open("log.bin")?;
            let reader = file.reader()?;

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
