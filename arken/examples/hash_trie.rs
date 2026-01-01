use arken::{Error, HashMap, HashRootRef, MappedFile, Writer};
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
            let file = MappedFile::open("trie.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<HashRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, root);

            println!("count = {}", trie.len());
        }
        Command::Query { key } => {
            let file = MappedFile::open("trie.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<HashRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, root);

            match trie.get(&key.into()) {
                Some(value) => println!("{key} = {value}"),
                _ => println!("{key} not found"),
            }
        }
        Command::Add { key, value } => {
            let file = MappedFile::open("trie.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<HashRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let mut trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, root);

            trie.insert(key.into(), value.into());
            let root_reference = trie.commit(&mut bytes, &mut writer)?;

            if let Some(root_reference) = root_reference {
                writer.append_with_marker(&mut bytes, b"map", &root_reference)?;
            }
        }
        Command::Remove { key } => {
            let file = MappedFile::open("trie.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<HashRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let mut trie: HashMap<'_, Cow<'_, str>, Cow<'_, str>> = HashMap::open(reader, root);

            trie.remove(&key.into());
            let root_reference = trie.commit(&mut bytes, &mut writer)?;

            if let Some(root_reference) = root_reference {
                writer.append_with_marker(&mut bytes, b"map", &root_reference)?;
            }
        }
    }

    Ok(())
}
