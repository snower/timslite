use napi::bindgen_prelude::{BigInt, Buffer};

use crate::errors;

pub fn bigint_to_i64(v: &BigInt) -> napi::Result<i64> {
    let (val, lossless) = v.get_i64();
    if !lossless {
        return Err(errors::invalid_data(
            "BigInt value exceeds i64 range",
        ));
    }
    Ok(val)
}

pub fn i64_to_bigint(v: i64) -> BigInt {
    BigInt::from(v)
}

pub fn u64_to_bigint(v: u64) -> BigInt {
    BigInt::from(v)
}

pub fn vec_to_buffer(data: Vec<u8>) -> Buffer {
    Buffer::from(data)
}

pub fn duration_to_ms(d: std::time::Duration) -> u32 {
    d.as_millis().min(u32::MAX as u128) as u32
}
