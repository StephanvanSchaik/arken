#[cfg(feature = "rust_decimal")]
mod decimal;
mod float;
#[cfg(feature = "jiff")]
mod jiff;
mod migrate;
mod reader;
mod signed;
mod unsigned;
#[cfg(feature = "uuid")]
mod uuid;
mod writer;

use bytes::{BufMut as _, BytesMut};
use num_enum::TryFromPrimitive;
use std::{
    borrow::Cow,
    io::{Seek, Write},
    marker::PhantomData,
};
use thiserror::Error;

#[cfg(feature = "rust_decimal")]
pub use crate::decimal::FixedDecimal;
pub use crate::migrate::{MigrationStrategy, migrate, migrate_to};
pub use crate::reader::{MappedFile, Reader};
pub use crate::writer::Writer;
pub use arken_impl::Arken;

#[derive(Debug, Error)]
pub enum Error {
    #[error("incomplete")]
    Incomplete,
    #[error("invalid header")]
    InvalidHeader,
    #[error("overflow")]
    Overflow,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[cfg(feature = "jiff")]
    #[error(transparent)]
    Jiff(#[from] ::jiff::Error),
    #[error(transparent)]
    Mmap(#[from] mmap_rs::Error),
    #[error(transparent)]
    Persist(#[from] tempfile::PersistError),
    #[cfg(feature = "uuid")]
    #[error(transparent)]
    Uuid(#[from] ::uuid::Error),
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, TryFromPrimitive)]
#[repr(u8)]
pub enum Endian {
    Big,
    #[default]
    Little,
    Native,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Config {
    fixed: bool,
    endian: Endian,
}

impl Config {
    pub fn variable_width(&mut self) -> &mut Self {
        self.fixed = false;
        self
    }

    pub fn fixed_width(&mut self) -> &mut Self {
        self.fixed = true;
        self
    }

    pub fn with_endian(&mut self, mut endian: Endian) -> &mut Self {
        if endian == Endian::Native {
            if cfg!(target_endian = "big") {
                endian = Endian::Big;
            } else {
                endian = Endian::Little;
            }
        }

        self.endian = endian;
        self
    }
}

impl<'a> Field<'a> for Config {
    fn from_slice(mut slice: &'a [u8], _: Config) -> Result<(Self, &'a [u8]), Error> {
        if slice.len() < 4 {
            return Err(Error::InvalidHeader);
        }

        if &slice[..3] != b"ARK" {
            return Err(Error::InvalidHeader);
        }

        let value = slice[3];
        slice = &slice[4..];

        let endian = Endian::try_from(value).map_err(|_| Error::InvalidHeader)?;
        let fixed = (value >> 7) & 1 == 1;

        Ok((Self { fixed, endian }, slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, _: Config) -> Result<(), Error> {
        bytes.put_slice(b"ARK");

        let value = self.endian as u8 | (self.fixed as u8) << 7;
        bytes.put_u8(value);

        Ok(())
    }
}

pub trait Field<'a> {
    fn from_slice(slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error>
    where
        Self: Sized;

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error>;

    fn migrate<W: Seek + Write>(
        &mut self,
        _bytes: &mut BytesMut,
        _writer: &mut Writer<W>,
        _reader: &Reader<'a>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Field<'a> for () {
    fn from_slice(slice: &'a [u8], _: Config) -> Result<(Self, &'a [u8]), Error> {
        Ok(((), slice))
    }

    fn put_bytes(&self, _: &mut BytesMut, _: Config) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a, T: Field<'a>> Field<'a> for Option<T> {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let (value, rest) = u8::from_slice(slice, config)?;
        slice = rest;

        match value {
            0 => Ok((None, rest)),
            1 => {
                let (value, rest) = T::from_slice(slice, config)?;
                slice = rest;

                Ok((Some(value), slice))
            }
            _ => return Err(Error::Incomplete),
        }
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        match self {
            None => {
                bytes.put_u8(0);
            }
            Some(value) => {
                bytes.put_u8(1);
                value.put_bytes(bytes, config)?;
            }
        }

        Ok(())
    }

    fn migrate<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
        reader: &Reader<'a>,
    ) -> Result<(), Error> {
        let Some(value) = self else {
            return Ok(());
        };

        value.migrate(bytes, writer, reader)?;

        Ok(())
    }
}

impl<'a> Field<'a> for Cow<'a, str> {
    fn from_slice(mut slice: &'a [u8], _: Config) -> Result<(Self, &'a [u8]), Error> {
        let n = slice
            .iter()
            .position(|&b| b == 0)
            .ok_or(Error::Incomplete)?;
        let value = std::str::from_utf8(&slice[..n]).unwrap_or_default();
        slice = &slice[n + 1..];

        Ok((value.into(), slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, _: Config) -> Result<(), Error> {
        bytes.put_slice(self.as_bytes());
        bytes.put_u8(0);

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum Array<'a, T> {
    Ref(&'a [u8]),
    Owned(Vec<T>),
}

impl<'a, T> Array<'a, T> {
    pub fn iter(&'a self, config: Config) -> Iter<'a, T> {
        match self {
            Self::Ref(bytes) => Iter::Ref(bytes, config),
            Self::Owned(vec) => Iter::Owned(vec.iter()),
        }
    }
}

impl<'a, T: Field<'a>> Field<'a> for Array<'a, T> {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let (n, rest) = usize::from_slice(slice, config)?;
        slice = rest;

        let value = &slice[..n];
        slice = &slice[n..];

        Ok((Self::Ref(value), slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        match self {
            Self::Ref(value) => {
                value.len().put_bytes(bytes, config)?;
                bytes.put_slice(value);
            }
            Self::Owned(items) => {
                let mut subbytes = BytesMut::new();

                for item in items {
                    item.put_bytes(&mut subbytes, config)?;
                }

                subbytes.len().put_bytes(bytes, config)?;
                bytes.put_slice(&subbytes[..]);
            }
        }

        Ok(())
    }
}

pub enum Iter<'a, T> {
    Ref(&'a [u8], Config),
    Owned(std::slice::Iter<'a, T>),
}

impl<'a, T: Clone + Field<'a>> Iterator for Iter<'a, T> {
    type Item = Cow<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Ref(bytes, config) => {
                let (item, rest) = T::from_slice(bytes, *config).ok()?;
                *self = Self::Ref(rest, *config);

                Some(Cow::Owned(item))
            }
            Self::Owned(iter) => iter.next().map(Cow::Borrowed),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Ref<'a, T: Field<'a>> {
    pub(crate) offset: usize,
    pub(crate) _marker: &'a PhantomData<T>,
}

impl<'a, T: Field<'a>> Field<'a> for Ref<'a, T> {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let (offset, rest) = usize::from_slice(slice, config)?;
        slice = rest;

        let value = Ref {
            offset,
            _marker: &PhantomData,
        };

        Ok((value, slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        self.offset.put_bytes(bytes, config)?;

        Ok(())
    }

    fn migrate<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
        reader: &Reader<'a>,
    ) -> Result<(), Error> {
        let mut value = reader.read(self)?;

        value.migrate(bytes, writer, reader)?;

        let reference = writer.append(bytes, &value)?;

        self.offset = reference.offset;

        Ok(())
    }
}

impl<'a, T: Clone + Field<'a>, const N: usize> Field<'a> for Cow<'a, [T; N]> {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let values = std::array::from_fn(|_| {
            let (value, rest) = T::from_slice(slice, config).unwrap();
            slice = rest;
            value
        });

        Ok((Cow::Owned(values), slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        for value in self.as_ref() {
            value.put_bytes(bytes, config)?;
        }

        Ok(())
    }

    fn migrate<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
        reader: &Reader<'a>,
    ) -> Result<(), Error> {
        match self {
            Cow::Borrowed(values) => {
                let mut values = values.clone();

                for value in &mut values {
                    value.migrate(bytes, writer, reader)?;
                }

                *self = Cow::Owned(values);
            }
            Cow::Owned(values) => {
                for value in values {
                    value.migrate(bytes, writer, reader)?;
                }
            }
        };

        Ok(())
    }
}

impl<'a, T: Clone + Field<'a>> Field<'a> for Cow<'a, [T]> {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let (n, rest) = usize::from_slice(slice, config)?;
        slice = rest;

        let mut values = Vec::with_capacity(n);

        for _ in 0..n {
            let (value, rest) = T::from_slice(slice, config).unwrap();
            slice = rest;
            values.push(value);
        }

        Ok((Cow::Owned(values), slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        self.as_ref().len().put_bytes(bytes, config)?;

        for value in self.as_ref() {
            value.put_bytes(bytes, config)?;
        }

        Ok(())
    }

    fn migrate<W: Seek + Write>(
        &mut self,
        bytes: &mut BytesMut,
        writer: &mut Writer<W>,
        reader: &Reader<'a>,
    ) -> Result<(), Error> {
        let mut values = std::mem::take(self).into_owned();

        for value in &mut values {
            value.migrate(bytes, writer, reader)?;
        }

        *self = Cow::Owned(values);

        Ok(())
    }
}
