use crate::{Config, Error, Field};
use ::jiff::Timestamp;
use bytes::BytesMut;

impl<'a> Field<'a> for ::jiff::Timestamp {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let (value, rest) = i128::from_slice(slice, config)?;
        slice = rest;

        let value = Timestamp::from_nanosecond(value)?;

        Ok((value, slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        self.as_nanosecond().put_bytes(bytes, config)?;

        Ok(())
    }
}
