use crate::{Config, Endian, Error, Field};
use bytes::{BufMut as _, BytesMut};
use pastey::paste;

impl<'a> Field<'a> for u8 {
    fn from_slice(mut slice: &'a [u8], _: Config) -> Result<(Self, &'a [u8]), Error> {
        if slice.is_empty() {
            return Err(Error::Incomplete);
        }

        let value = slice[0];
        slice = &slice[1..];

        Ok((value, slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, _: Config) -> Result<(), Error> {
        bytes.put_u8(*self);

        Ok(())
    }
}

macro_rules! impl_unsigned_primitive {
    ($ty:ty) => {
        paste! {
            impl<'a> Field<'a> for $ty {
                fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
                    let value = if config.fixed {
                        const N: usize = std::mem::size_of::<$ty>();

                        if slice.len() < N {
                            return Err(Error::Incomplete);
                        }

                        let mut bytes = [0u8; N];
                        bytes.copy_from_slice(&slice[..N]);
                        slice = &slice[N..];

                        match config.endian {
                            Endian::Big => $ty::from_be_bytes(bytes),
                            Endian::Little => $ty::from_le_bytes(bytes),
                            Endian::Native => $ty::from_ne_bytes(bytes),
                        }
                    } else {
                        let mut value = 0;
                        let mut shift = 0;

                        loop {
                            let (byte, rest) = u8::from_slice(slice, config)?;
                            slice = rest;

                            let next = byte as $ty;
                            value += (next & 0x7f) << shift;

                            if byte & 0x80 == 0 {
                                break;
                            }

                            shift += 7;

                            if shift > std::mem::size_of::<$ty>() * 8 {
                                return Err(Error::Overflow);
                            }
                        }

                        value
                    };

                    Ok((value, slice))
                }

                fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
                    if config.fixed {
                        let slice = match config.endian {
                            Endian::Big => $ty::to_be_bytes(*self),
                            Endian::Little => $ty::to_le_bytes(*self),
                            Endian::Native => $ty::to_ne_bytes(*self),
                        };

                        bytes.put_slice(&slice);
                    } else {
                        let mut value = *self;

                        while value >= 0x80 {
                            bytes.put_u8(((value & 0x7f) | 0x80) as u8);
                            value >>= 7;
                        }

                        bytes.put_u8(value as u8);
                    }

                    Ok(())
                }
            }
        }
    };
}

impl_unsigned_primitive!(u16);
impl_unsigned_primitive!(u32);
impl_unsigned_primitive!(u64);
impl_unsigned_primitive!(u128);
impl_unsigned_primitive!(usize);
