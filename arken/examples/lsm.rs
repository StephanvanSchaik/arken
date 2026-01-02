use arken::{Error, MappedFile, MergeMap, MergeRootRef, Writer};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use std::borrow::Cow;

#[derive(Debug, Subcommand)]
enum Command {
    Count,
    List,
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

    let mut writer = match Writer::open("lsm.bin") {
        Ok(writer) => writer,
        _ => {
            let writer = Writer::tempfile(Default::default())?;

            writer.persist("lsm.bin")?
        }
    };

    let mut bytes = BytesMut::new();

    match &args.command {
        Command::Count => {
            let file = MappedFile::open("lsm.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<MergeRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let map: MergeMap<'_, Cow<'_, str>, Cow<'_, str>> = MergeMap::open(reader, root);

            println!("count = {}", map.len());
        }
        Command::List => {
            let file = MappedFile::open("lsm.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<MergeRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let map: MergeMap<'_, Cow<'_, str>, Cow<'_, str>> = MergeMap::open(reader, root);

            for key in map.keys() {
                println!("{key}");
            }
        }
        Command::Query { key } => {
            let file = MappedFile::open("lsm.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<MergeRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let map: MergeMap<'_, Cow<'_, str>, Cow<'_, str>> = MergeMap::open(reader, root);

            match map.get(&key.into()) {
                Some(value) => println!("{key} = {value}"),
                _ => println!("{key} not found"),
            }
        }
        Command::Add { key, value } => {
            let file = MappedFile::open("lsm.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<MergeRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let mut map: MergeMap<'_, Cow<'_, str>, Cow<'_, str>> = MergeMap::open(reader, root);

            map.insert(key.into(), value.into());
            let root_reference = map.commit(&mut bytes, &mut writer)?;

            if let Some(root_reference) = root_reference {
                writer.append_with_marker(&mut bytes, b"map", &root_reference)?;
            }
        }
        Command::Remove { key } => {
            let file = MappedFile::open("lsm.bin")?;
            let reader = file.reader();
            let root = reader
                .find::<MergeRootRef<'_, Cow<'_, str>, Cow<'_, str>>>(b"map")
                .next();
            let mut map: MergeMap<'_, Cow<'_, str>, Cow<'_, str>> = MergeMap::open(reader, root);

            map.remove(&key.into());
            let root_reference = map.commit(&mut bytes, &mut writer)?;

            if let Some(root_reference) = root_reference {
                writer.append_with_marker(&mut bytes, b"map", &root_reference)?;
            }
        }
    }

    Ok(())
}
