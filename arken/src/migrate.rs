use crate::{Error, MappedFile, Reader, Writer};
use bytes::BytesMut;
use std::{
    io::{Seek, Write},
    path::Path,
};

pub trait MigrationStrategy {
    fn migrate<'a, W: Seek + Write>(
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
        reader: &Reader<'a>,
    ) -> Result<(), Error>;
}

pub fn migrate<P: AsRef<Path>, S: MigrationStrategy>(
    bytes: &mut BytesMut,
    path: P,
) -> Result<(), Error> {
    migrate_to::<_, _, S>(bytes, &path, &path)
}

pub fn migrate_to<D: AsRef<Path>, P: AsRef<Path>, S: MigrationStrategy>(
    bytes: &mut BytesMut,
    dst_path: D,
    path: P,
) -> Result<(), Error> {
    let file = MappedFile::open(&path)?;
    let reader = file.reader();

    let mut writer = Writer::tempfile(Default::default())?;

    S::migrate(bytes, &mut writer, &reader)?;

    writer.flush()?;
    writer.persist(dst_path)?;

    Ok(())
}
