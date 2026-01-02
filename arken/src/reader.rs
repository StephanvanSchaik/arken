use crate::{Config, Error, Field, Ref};
use memchr::memmem::FinderRev;
use mmap_rs::{Mmap, MmapOptions};
use std::{fs::File, marker::PhantomData, path::Path};

#[derive(Clone, Debug)]
pub struct MarkerIter<'a, T: Field<'a>> {
    bytes: &'a [u8],
    config: Config,
    marker: &'a [u8],
    limit: usize,
    _marker: PhantomData<T>,
}

impl<'a, T: Field<'a>> Iterator for MarkerIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let limit = self.limit.min(self.bytes.len());

        let offset = FinderRev::new(self.marker).rfind(&self.bytes[..limit])?;

        let slice = &self.bytes[offset + self.marker.len()..];
        let (size, rest) = usize::from_slice(slice, self.config).ok()?;
        let (checksum, _) = u32::from_slice(rest, self.config).ok()?;

        let slice = &self.bytes[offset - size..][..size];

        if crc32fast::hash(slice) != checksum {
            return None;
        }

        let (value, _) = T::from_slice(slice, self.config).ok()?;

        self.limit = offset;

        Some(value)
    }
}

#[derive(Debug, Default)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    config: Config,
}

impl<'a> TryFrom<&'a [u8]> for Reader<'a> {
    type Error = Error;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Error> {
        let (config, _) = Config::from_slice(bytes, Default::default())?;

        Ok(Self { bytes, config })
    }
}

impl<'a> Reader<'a> {
    pub fn read<T: Field<'a>>(&self, reference: &Ref<'a, T>) -> Result<T, Error> {
        if self.bytes.len() < reference.offset {
            return Err(Error::InvalidOffset);
        }

        let (value, _) = T::from_slice(&self.bytes[reference.offset..], self.config)?;

        Ok(value)
    }

    pub fn find<T: Field<'a>>(&self, marker: &'a [u8]) -> MarkerIter<'a, T> {
        MarkerIter {
            bytes: self.bytes,
            config: self.config,
            marker,
            limit: usize::MAX,
            _marker: PhantomData,
        }
    }
}

fn round_up(x: usize, align: usize) -> usize {
    (x + align.saturating_sub(1)) & !(align.saturating_sub(1))
}

#[derive(Debug)]
pub struct MappedFile {
    file: Option<File>,
    map: Option<Mmap>,
    size: usize,
}

impl MappedFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let Ok(file) = File::open(&path) else {
            return Ok(Self {
                file: None,
                map: None,
                size: 0,
            });
        };

        let size = file
            .metadata()
            .map(|metadata| metadata.len() as usize)
            .unwrap_or(0);

        let map_size = round_up(size, MmapOptions::page_size());

        if map_size == 0 {
            return Ok(Self {
                file: Some(file),
                map: None,
                size,
            });
        }

        let map = unsafe { MmapOptions::new(map_size)?.with_file(&file, 0).map()? };

        Ok(Self {
            file: Some(file),
            map: Some(map),
            size,
        })
    }

    pub fn resize(&mut self) -> Result<(), Error> {
        let Some(file) = self.file.as_ref() else {
            return Ok(());
        };

        let size = file
            .metadata()
            .map(|metadata| metadata.len() as usize)
            .unwrap_or(0);

        if self.size == size {
            return Ok(());
        }

        let map_size = round_up(size, MmapOptions::page_size());

        if map_size == 0 {
            self.map = None;
            self.size = 0;
        }

        let map = unsafe { MmapOptions::new(map_size)?.with_file(file, 0).map()? };

        self.map = Some(map);
        self.size = size;

        Ok(())
    }

    pub fn reader(&self) -> Reader<'_> {
        Reader::try_from(
            self.map
                .as_ref()
                .map(|map| &map[..self.size])
                .unwrap_or(&[]),
        )
        .unwrap_or_default()
    }
}
