use crate::{Config, Error, Field};
use bytes::BytesMut;
use rust_decimal::Decimal;

impl<'a> Field<'a> for Decimal {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let (mantissa, rest) = i128::from_slice(slice, config)?;
        slice = rest;

        let (scale, rest) = u32::from_slice(slice, config)?;
        slice = rest;

        let value = Decimal::from_i128_with_scale(mantissa, scale);

        Ok((value, slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        self.mantissa().put_bytes(bytes, config)?;
        self.scale().put_bytes(bytes, config)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FixedDecimal<const N: u32>(Decimal);

impl<const N: u32> From<Decimal> for FixedDecimal<N> {
    fn from(value: Decimal) -> FixedDecimal<N> {
        Self(value)
    }
}

impl<const N: u32> From<FixedDecimal<N>> for Decimal {
    fn from(value: FixedDecimal<N>) -> Decimal {
        value.0
    }
}

impl<'a, const N: u32> Field<'a> for FixedDecimal<N> {
    fn from_slice(mut slice: &'a [u8], config: Config) -> Result<(Self, &'a [u8]), Error> {
        let (mantissa, rest) = i128::from_slice(slice, config)?;
        slice = rest;

        let value = Decimal::from_i128_with_scale(mantissa, N);

        Ok((Self(value), slice))
    }

    fn put_bytes(&self, bytes: &mut BytesMut, config: Config) -> Result<(), Error> {
        let mut value = self.0;

        value.rescale(N);

        value.mantissa().put_bytes(bytes, config)?;

        Ok(())
    }
}
