use crate::{Config, Endian, Error, Field};
use ::uuid::Uuid;
use bytes::{BufMut as _, BytesMut};

impl<'a> Field<'a> for Uuid {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        if slice.len() < 16 {
            return Err(Error::Incomplete);
        }

        let value = match config.endian {
            Endian::Big => Uuid::from_slice(&slice[..16])?,
            Endian::Little => Uuid::from_slice_le(&slice[..16])?,
            #[cfg(target_endian = "big")]
            Endian::Native => Uuid::from_slice(&slice[..16])?,
            #[cfg(target_endian = "little")]
            Endian::Native => Uuid::from_slice_le(&slice[..16])?,
        };

        slice = &slice[16..];

        Ok((value, slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        match config.endian {
            Endian::Big => bytes.put_slice(&self.as_bytes()[..]),
            Endian::Little => bytes.put_slice(&self.to_bytes_le()[..]),
            #[cfg(target_endian = "big")]
            Endian::Native => bytes.put_slice(&self.as_bytes()[..]),
            #[cfg(target_endian = "little")]
            Endian::Native => bytes.put_slice(&self.to_bytes_le()[..]),
        }

        Ok(())
    }
}
