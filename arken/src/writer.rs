use crate::{Config, Error, Field, Reader, Ref};
use bytes::{BufMut as _, BytesMut};
use mmap_rs::MmapOptions;
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    marker::PhantomData,
    path::Path,
};
use tempfile::NamedTempFile;

#[derive(Debug)]
pub struct Writer<W: Seek + Write> {
    file: W,
    config: Config,
}

impl Writer<NamedTempFile> {
    pub fn tempfile(config: Config) -> Result<Self, Error> {
        let mut file = tempfile::Builder::new().append(true).tempfile()?;

        let mut bytes = BytesMut::with_capacity(4);
        config.put_bytes(&mut bytes, Default::default())?;
        file.write_all(&bytes[..])?;

        Ok(Self { file, config })
    }

    pub fn persist<P: AsRef<Path>>(self, new_path: P) -> Result<Writer<File>, Error> {
        let file = self.file.persist(new_path)?;
        let config = self.config;

        Ok(Writer { file, config })
    }
}

impl Writer<File> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;

        let map = unsafe {
            MmapOptions::new(MmapOptions::page_size())?
                .with_file(&file, 0)
                .map()?
        };
        let (config, _) = Config::from_slice(&map[..], Default::default())?;

        Ok(Self { file, config })
    }
}

impl<W: Seek + Write> Writer<W> {
    pub fn config(&self) -> Config {
        self.config
    }

    pub fn append<'a, T: Field<'a>>(
        &mut self,
        bytes: &mut BytesMut,
        data: &T,
    ) -> Result<Ref<'a, T>, Error> {
        let reference = Ref {
            offset: self.file.seek(SeekFrom::End(0))? as usize,
            _marker: &PhantomData,
        };

        bytes.clear();
        data.put_bytes(bytes, self.config)?;

        self.file.write_all(&bytes[..])?;

        Ok(reference)
    }

    pub fn append_with_marker<'a, T: Field<'a>>(
        &mut self,
        bytes: &mut BytesMut,
        marker: &'a [u8],
        data: &T,
    ) -> Result<Ref<'a, T>, Error> {
        let reference = Ref {
            offset: self.file.seek(SeekFrom::End(0))? as usize,
            _marker: &PhantomData,
        };

        bytes.clear();
        data.put_bytes(bytes, self.config)?;

        let size = bytes.len();
        let checksum = crc32fast::hash(&bytes[..size]);

        bytes.put_slice(marker);
        size.put_bytes(bytes, self.config)?;
        checksum.put_bytes(bytes, self.config)?;

        self.file.write_all(&bytes[..])?;

        Ok(reference)
    }

    pub fn migrate_with_marker<'a, T: Field<'a>>(
        &mut self,
        bytes: &mut BytesMut,
        marker: &'a [u8],
        reader: &Reader<'a>,
        mut data: T,
    ) -> Result<Ref<'a, T>, Error> {
        let reference = Ref {
            offset: self.file.seek(SeekFrom::End(0))? as usize,
            _marker: &PhantomData,
        };

        bytes.clear();
        data.migrate(bytes, self, reader)?;
        self.file.write_all(&bytes[..])?;

        self.append_with_marker(bytes, marker, &data)?;

        Ok(reference)
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        self.file.flush()?;

        Ok(())
    }
}
