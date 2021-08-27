use integer_encoding::{self, FixedInt, VarInt};

pub fn get_varint_32_prefix_ptr(p: usize, limit: usize, key: &[u8]) -> Option<(u32, usize)> {
    if key.len() == 0 {
        return None;
    }
    for (i, &b) in key.iter().enumerate() {
        if b < 128u8 {
            return u32::decode_var(&key[..i]);
        }
    }
    None
}

pub fn get_varint_32_prefix_slice(p: usize, limit: usize, key: &[u8]) -> Option<&[u8]> {
    match get_varint_32_prefix_ptr(p, limit, key) {
        Some((len, n)) => {
            let read_len = len as usize + n;
            if read_len > key.len() {
                return None;
            }
        }
        None => return None,
    };

    None
}
// fn get_varint_32_ptr_fallback(p: usize, limit: usize, key: &[u8]) -> Option<usize> {
//     None
// }
pub fn put_fixed_64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(value.encode_fixed_light())
}

pub fn put_fixed_32(dst: &mut Vec<u8>, value: u32) {
    let mut i = value.encode_fixed_light();
    dst.extend_from_slice(i)
}

pub fn encode_fixed_32(dst: &mut [u8], value: u32) {
    let mut v = value.encode_fixed_light();
    for i in 0..4 {
        dst[i] = v[i];
    }
}
pub fn encode_fixed_64(dst: &mut [u8], value: u64) {
    let mut v = value.encode_fixed_light();
    for i in 0..8 {
        dst[i] = v[i];
    }
}
pub fn put_varint_32(dst: &mut Vec<u8>, value: u32) {
    dst.append(&mut value.encode_var_vec());
}
pub fn put_varint_64(dst: &mut Vec<u8>, value: u64) {
    dst.append(&mut value.encode_var_vec());
}

pub fn varint_length(value: usize) -> usize {
    let mut len = 1;

    let mut value = value;
    while (value >= 128) {
        value >>= 7;
        len += 1;
    }
    len
}
pub fn put_length_prefixed_slice(dst: &mut Vec<u8>, value: &[u8]) {
    put_varint_32(dst, value.len() as u32);
    dst.extend_from_slice(value);
}

pub fn get_varint_32(input: &[u8]) -> Option<(u32, usize)> {
    u32::decode_var(input)
}

pub fn get_varint_64(input: &[u8]) -> Option<(u64, usize)> {
    u64::decode_var(input)
}
pub fn get_fixed_32(input: &[u8]) -> u32 {
    u32::decode_fixed(input)
}
pub fn get_fixed_64(input: &[u8]) -> u64 {
    u64::decode_fixed(input)
}
// TODO decode inplace to avoid clone
pub fn decode_fixed_32(input: &[u8]) -> u32 {
    match input.len().cmp(&4) {
        std::cmp::Ordering::Less => {
            let mut input = Vec::from(input);
            input.resize(4, 0);
            u32::decode_fixed_vec(&input)
        }
        _ => u32::decode_fixed(&input[0..4]),
    }
}

pub fn decode_fixed_64(input: &[u8]) -> u64 {
    match input.len().cmp(&8) {
        std::cmp::Ordering::Less => {
            let mut input = Vec::from(input);
            input.resize(8, 0);
            u64::decode_fixed_vec(&input)
        }
        _ => u64::decode_fixed(&input[0..8]),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use integer_encoding::{self, FixedInt, VarInt};
    #[test]
    fn test_encode() {
        println!("{:?}", 226u64.encode_fixed_light());
        println!("{:?}", 0u32.encode_fixed_vec());
        println!("{:?}", 123456u32.encode_var_vec());
        println!("{:?}", 123456u64.encode_var_vec());
    }

    #[test]
    fn test_decode() {
        let a = u32::decode_fixed_vec(&vec![0, 0, 0, 0]);
        println!("{}", a);
    }
    #[test]
    fn test_encode_fixed_32() {
        let mut tests: Vec<(u32, Vec<u8>, Vec<u8>)> = vec![
            (0u32, vec![0; 4], vec![0, 0, 0, 0]),
            (1u32, vec![0; 4], vec![1, 0, 0, 0]),
            (255u32, vec![0; 4], vec![255, 0, 0, 0]),
            (256u32, vec![0; 4], vec![0, 1, 0, 0]),
            (512u32, vec![0; 4], vec![0, 2, 0, 0]),
            (u32::max_value(), vec![0; 4], vec![255, 255, 255, 255]),
            (u32::max_value(), vec![0; 6], vec![255, 255, 255, 255, 0, 0]),
        ];
        for (input, mut dst, expect) in tests.drain(..) {
            encode_fixed_32(dst.as_mut_slice(), input);
            for (n, m) in dst.iter().zip(expect) {
                assert_eq!(*n, m);
            }
        }
    }
    #[test]
    fn test_decode_fixed_32() {
        let mut tests: Vec<(Vec<u8>, u32)> = vec![
            // (vec![], 0u32),
            (vec![0], 0u32),
            (vec![1, 0], 1u32),
            (vec![1, 1, 0, 0], 257u32),
            (vec![0, 0, 0, 0], 0u32),
            (vec![1, 0, 0, 0], 1u32),
            (vec![255, 0, 0, 0], 255u32),
            (vec![0, 1, 0, 0], 256u32),
            (vec![0, 1], 256u32),
            (vec![0, 2, 0, 0], 512u32),
            (vec![255, 255, 255, 255], u32::max_value()),
            (vec![255, 255, 255, 255, 0, 0], u32::max_value()),
            (vec![255, 255, 255, 255, 1, 0], u32::max_value()),
        ];
        let mut len = 0;
        for (src, expect) in tests.drain(..) {
            let result = decode_fixed_32(src.as_slice());
            assert_eq!(result, expect);
        }
    }
    #[test]
    fn test_encode_fixed_64() {
        let mut tests: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (0u64, vec![0; 8], vec![0; 8]),
            (1u64, vec![0; 8], vec![1, 0, 0, 0, 0, 0, 0, 0]),
            (255u64, vec![0; 8], vec![255, 0, 0, 0, 0, 0, 0, 0]),
            (256u64, vec![0; 8], vec![0, 1, 0, 0, 0, 0, 0, 0]),
            (512u64, vec![0; 8], vec![0, 2, 0, 0, 0, 0, 0, 0]),
            (
                u64::max_value(),
                vec![0; 8],
                vec![255, 255, 255, 255, 255, 255, 255, 255],
            ),
            (
                u64::max_value(),
                vec![0; 10],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0],
            ),
        ];
        for (input, mut dst, expect) in tests.drain(..) {
            encode_fixed_64(dst.as_mut_slice(), input);
            for (n, m) in dst.iter().zip(expect) {
                assert_eq!(*n, m);
            }
        }
    }
    #[test]
    fn test_decode_fixed_64() {
        let mut tests: Vec<(Vec<u8>, u64)> = vec![
            (vec![], 0u64),
            (vec![0], 0u64),
            (vec![0; 8], 0u64),
            (vec![1, 0], 1u64),
            (vec![1, 1], 257u64),
            (vec![1, 0, 0, 0, 0, 0, 0, 0], 1u64),
            (vec![255, 0, 0, 0, 0, 0, 0, 0], 255u64),
            (vec![0, 1, 0, 0, 0, 0, 0, 0], 256u64),
            (vec![0, 1], 256u64),
            (vec![0, 2, 0, 0, 0, 0, 0, 0], 512u64),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255],
                u64::max_value(),
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 0],
                u64::max_value(),
            ),
            (
                vec![255, 255, 255, 255, 255, 255, 255, 255, 1, 0],
                u64::max_value(),
            ),
        ];
        for (src, expect) in tests.drain(..) {
            let result = decode_fixed_64(src.as_slice());
            assert_eq!(result, expect);
        }
    }
}
