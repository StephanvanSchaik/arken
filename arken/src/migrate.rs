use crate::{Error, Reader, Writer};
use bytes::BytesMut;
use mmap_rs::MmapOptions;
use std::{
    fs::File,
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

fn round_up(x: usize, align: usize) -> usize {
    (x + align.saturating_sub(1)) & !(align.saturating_sub(1))
}

pub fn migrate<P: AsRef<Path>, S: MigrationStrategy>(
    bytes: &mut BytesMut,
    path: P,
) -> Result<(), Error> {
    let file = File::open(&path)?;

    let size = file.metadata()?.len() as usize;
    let size = round_up(size, MmapOptions::page_size());

    if size == 0 {
        return Ok(());
    }

    let map = unsafe { MmapOptions::new(size)?.with_file(&file, 0).map()? };

    let reader = Reader::try_from(&map[..])?;

    let mut writer = Writer::tempfile(Default::default())?;

    S::migrate(bytes, &mut writer, &reader)?;

    writer.flush()?;
    writer.persist(path)?;

    Ok(())
}

pub fn migrate_to<D: AsRef<Path>, P: AsRef<Path>, S: MigrationStrategy>(
    bytes: &mut BytesMut,
    dst_path: D,
    path: P,
) -> Result<(), Error> {
    let file = File::open(&path)?;

    let size = file.metadata()?.len() as usize;
    let size = round_up(size, MmapOptions::page_size());

    if size == 0 {
        return Ok(());
    }

    let map = unsafe { MmapOptions::new(size)?.with_file(&file, 0).map()? };

    let reader = Reader::try_from(&map[..])?;

    let mut writer = Writer::tempfile(Default::default())?;

    S::migrate(bytes, &mut writer, &reader)?;

    writer.flush()?;
    writer.persist(dst_path)?;

    Ok(())
}
