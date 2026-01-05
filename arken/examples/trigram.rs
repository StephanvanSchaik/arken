use arken::{Error, MappedFile, StringTrigramIter, TrigramRootRef, TrigramSet, Writer};
use bytes::BytesMut;
use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Command {
    Query { key: String },
    Add { key: String },
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

    let mut writer = match Writer::open("trigram.bin") {
        Ok(writer) => writer,
        _ => {
            let writer = Writer::tempfile(Default::default())?;

            writer.persist("trigram.bin")?
        }
    };

    let mut bytes = BytesMut::new();

    match &args.command {
        Command::Query { key } => {
            let file = MappedFile::open("trigram.bin")?;
            let reader = file.reader();
            let root = reader.find::<TrigramRootRef<'_, ()>>(b"map").next();
            let map: TrigramSet<'_, StringTrigramIter> = TrigramSet::open(reader, root);

            let results = map.query(key.as_bytes());

            for (score, key) in results.iter().rev() {
                let Ok(key) = std::str::from_utf8(key) else {
                    continue;
                };

                println!("{key} ({score})");
            }
        }
        Command::Add { key } => {
            let file = MappedFile::open("trigram.bin")?;
            let reader = file.reader();
            let root = reader.find::<TrigramRootRef<'_, ()>>(b"map").next();
            let mut map: TrigramSet<'_, StringTrigramIter> = TrigramSet::open(reader, root);

            map.insert(key.as_bytes());
            let root_reference = map.commit(&mut bytes, &mut writer)?;

            if let Some(root_reference) = root_reference {
                writer.append_with_marker(&mut bytes, b"map", &root_reference)?;
            }
        }
        Command::Remove { key } => {
            let file = MappedFile::open("trigram.bin")?;
            let reader = file.reader();
            let root = reader.find::<TrigramRootRef<'_, ()>>(b"map").next();
            let mut map: TrigramSet<'_, StringTrigramIter> = TrigramSet::open(reader, root);

            map.remove(&key.as_bytes());
            let root_reference = map.commit(&mut bytes, &mut writer)?;

            if let Some(root_reference) = root_reference {
                writer.append_with_marker(&mut bytes, b"map", &root_reference)?;
            }
        }
    }

    Ok(())
}
