use crate::{Config, Endian, Error, Field};
use bytes::{BufMut as _, BytesMut};

macro_rules! impl_float_primitive {
    ($ty:ty) => {
        pastey::paste! {
            impl<'a> Field<'a> for $ty {
                fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
                    const N: usize = std::mem::size_of::<$ty>();

                    if slice.len() < N {
                        return Err(Error::Incomplete);
                    }

                    let mut bytes = [0u8; N];
                    bytes.copy_from_slice(&slice[..N]);
                    slice = &slice[N..];

                    let value = match config.endian {
                        Endian::Big => $ty::from_be_bytes(bytes),
                        Endian::Little => $ty::from_le_bytes(bytes),
                        Endian::Native => $ty::from_ne_bytes(bytes),
                    };

                    Ok((value, slice))
                }

                fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
                    let slice = match config.endian {
                        Endian::Big => $ty::to_be_bytes(*self),
                        Endian::Little => $ty::to_le_bytes(*self),
                        Endian::Native => $ty::to_ne_bytes(*self),
                    };

                    bytes.put_slice(&slice);

                    Ok(())
                }
            }
        }
    };
}

impl_float_primitive!(f32);
impl_float_primitive!(f64);
