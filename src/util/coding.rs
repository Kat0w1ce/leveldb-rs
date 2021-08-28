use integer_encoding::{self, FixedInt, VarInt};

pub const MAX_VARINT_LEN_U32: usize = 5;
pub const MAX_VARINT_LEN_U64: usize = 10;
pub fn get_varint_32_prefix_ptr(p: usize, limit: usize, key: &[u8]) -> Option<(u32, usize)> {
    if key.len() == 0 {
        return None;
    }
    for (i, &b) in key.iter().enumerate() {
        if b < 128u8 {
            return u32::decode_var(&key[..i + 1]);
        }
    }
    None
}

// sliceFormat:
// len: varint32
// data: &[u8]

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

pub fn get_length_prefixed_slice(data: &[u8]) -> Option<Vec<u8>> {
    match get_varint_32_prefix_ptr(0, data.len(), &data) {
        Some((len, size)) => return Some(data[size..size + len as usize].to_vec()),
        None => (),
    };
    None
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

    #[test]
    fn test_put_varint_64() {
        let tests = vec![
            (0u64, vec![0]),
            (100u64, vec![0b110_0100]),
            (129u64, vec![0b1000_0001, 0b1]),
            (258u64, vec![0b1000_0010, 0b10]),
            (
                58962304u64,
                vec![0b1000_0000, 0b1110_0011, 0b1000_1110, 0b1_1100],
            ),
        ];
        for (input, results) in tests {
            let mut buf: Vec<u8> = Vec::with_capacity(MAX_VARINT_LEN_U64);
            put_varint_64(&mut buf, input);
            assert_eq!(buf.len(), results.len());
            for (i, b) in buf.iter().enumerate() {
                assert_eq!(*b, results[i]);
            }
        }
    }

    #[test]
    fn test_read_varint_64() {
        let mut test_data = vec![
            0,
            0b110_0100,
            0b1000_0001,
            0b1,
            0b1000_0010,
            0b10,
            0b1000_0000,
            0b1110_0011,
            0b1000_1110,
            0b1_1100,
            0b1100_1110,
            0b1000_0001,
            0b1011_0101,
            0b1101_1001,
            0b1111_0110,
            0b1010_1100,
            0b1100_1110,
            0b1000_0001,
            0b1011_0101,
            0b1101_1001,
            0b1111_0110,
            0b1010_1100,
        ];
        let expects = vec![
            Some((0u64, 1)),
            Some((100u64, 1)),
            Some((129u64, 2)),
            Some((258u64, 2)),
            Some((58962304u64, 4)),
            None,
        ];

        let mut idx = 0;
        while !test_data.is_empty() {
            match u64::decode_var(&test_data.as_slice()) {
                Some((i, n)) => {
                    assert_eq!(Some((i, n)), expects[idx]);
                    test_data.drain(0..n);
                }
                None => {
                    assert_eq!(None, expects[idx]);
                    test_data.drain(..);
                }
            }
            idx += 1;
        }
    }

    #[test]
    fn test_put_and_get_varint() {
        let mut buf = vec![];
        let mut numbers = vec![];
        let n = 100;
        for _ in 0..n {
            let r = rand::random::<u64>();
            put_varint_64(&mut buf, r);
            numbers.push(r);
        }
        println!("{:?}", numbers);
        let mut start = 0;
        for i in 0..n {
            if let Some((res, n)) = u64::decode_var(&buf.as_slice()[start..]) {
                assert_eq!(numbers[i], res);
                start += n
            }
        }
    }
    #[test]
    fn test_get_varint_32_prefix_ptr() {
        let tests: Vec<Vec<u8>> = vec![vec![1], vec![1, 2, 3, 4, 5], vec![0; 100], vec![0; 256]];
        let mut encoded = vec![];
        for v in tests {
            let mut buf = vec![];
            put_length_prefixed_slice(&mut buf, v.as_slice());
            encoded.push(buf.to_owned());
        }
        let results = vec![
            Some((1u32, 1usize)),
            Some((5, 1)),
            Some((100, 1)),
            Some((256, 2)),
        ];
        for (i, v) in encoded.iter().enumerate() {
            let a = get_varint_32_prefix_ptr(0, 0, v);
            assert_eq!(a, results[i]);
        }
    }

    #[test]
    fn test_put_and_get_prefixed_slice() {
        let mut encoded: Vec<Vec<u8>> = vec![];
        let tests: Vec<Vec<u8>> = vec![vec![1], vec![1, 2, 3, 4, 5], vec![0; 100]];
        for input in tests.clone() {
            let mut buf = vec![];
            put_length_prefixed_slice(&mut buf, &input);

            encoded.push(buf.to_owned())
        }

        let mut decoded = vec![];
        for s in encoded {
            if let Some(res) = get_length_prefixed_slice(&s) {
                println!("res: {:?}", res);
                decoded.push(res.to_owned());
            } else {
                break;
            }
        }
        assert_eq!(tests.len(), decoded.len());
        for (get, want) in decoded.into_iter().zip(tests.into_iter()) {
            assert_eq!(get.len(), want.len());
            for (getv, wantv) in get.iter().zip(want.iter()) {
                assert_eq!(*getv, *wantv)
            }
        }
    }
}
