use arken::{Error, HashMap, MappedFile, Reader, Writer};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use std::borrow::Cow;

#[derive(Debug, Subcommand)]
enum Command {
    Count,
    Query { key: String },
    Add { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let mut writer = match Writer::open("trie.bin") {
        Ok(writer) => writer,
        _ => {
            let writer = Writer::tempfile(Default::default())?;

            writer.persist("trie.bin")?
        }
    };

    let mut bytes = BytesMut::new();

    match &args.command {
        Command::Count => {
            let file = MappedFile::open("trie.bin").ok();
            let reader = file
                .as_ref()
                .and_then(|file| file.reader().ok())
                .unwrap_or(Reader::default());
            let trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, b"map");

            println!("count = {}", trie.len());
        }
        Command::Query { key } => {
            let file = MappedFile::open("trie.bin").ok();
            let reader = file
                .as_ref()
                .and_then(|file| file.reader().ok())
                .unwrap_or(Reader::default());
            let trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, b"map");

            match trie.get(&key.into()) {
                Some(value) => println!("{key} = {value}"),
                _ => println!("{key} not found"),
            }
        }
        Command::Add { key, value } => {
            let file = MappedFile::open("trie.bin").ok();
            let reader = file
                .as_ref()
                .and_then(|file| file.reader().ok())
                .unwrap_or(Reader::default());
            let mut trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, b"map");

            trie.insert(key.into(), value.into());
            trie.commit(&mut bytes, &mut writer)?;
        }
        Command::Remove { key } => {
            let file = MappedFile::open("trie.bin").ok();
            let reader = file
                .as_ref()
                .and_then(|file| file.reader().ok())
                .unwrap_or(Reader::default());
            let mut trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, b"map");

            trie.remove(&key.into());
            trie.commit(&mut bytes, &mut writer)?;
        }
    }

    Ok(())
}
