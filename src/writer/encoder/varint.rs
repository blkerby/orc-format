use crate::writer::compression::{CompressionStream};

pub trait VarInt: Copy + Default {
    fn write_varint(self, out: &mut CompressionStream);
    fn wrapping_sub_i64(self, rhs: Self) -> i64;
}

impl VarInt for u64 {
    #[inline(always)]
    fn write_varint(mut self, out: &mut CompressionStream) {
        let mut buf = [0; 10];
        let mut len: usize = 0;

        for i in 0..10 {
            if self < 0x80 { buf[i] = self as u8; len = i + 1; break }
            buf[i] = 0x80 | (self as u8);
            self >>= 7;
        }
        out.write_bytes(&buf[..len]);
    }

    #[inline(always)]
    fn wrapping_sub_i64(self, rhs: Self) -> i64 {
        self.wrapping_sub(rhs) as i64
    }
}

impl VarInt for i64 {
    #[inline(always)]
    fn write_varint(self, out: &mut CompressionStream) {
        let zigzag_encoding = (self << 1) ^ (self >> 63);
        (zigzag_encoding as u64).write_varint(out);
    }

    #[inline(always)]
    fn wrapping_sub_i64(self, rhs: Self) -> i64 {
        self.wrapping_sub(rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::compression::{CompressionStream, NoCompression};

    #[test]
    fn test_write_varint_u64() {
        let cases: Vec<(u64, Vec<u8>)> = vec![
            (0, vec![0x00]),
            (1, vec![0x01]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x01]),
            (129, vec![0x81, 0x01]),
            (16383, vec![0xff, 0x7f]),
            (16384, vec![0x80, 0x80, 0x01]),
            (16385, vec![0x81, 0x80, 0x01]),
        ];
        let mut stream = CompressionStream::new(&NoCompression::new().build());
        for (input, expected_output) in cases {
            input.write_varint(&mut stream);
            let mut out: Vec<u8> = Vec::new();
            stream.finish(&mut out).unwrap();
            assert_eq!(out, expected_output);
        }
    }

    #[test]
    fn test_write_varint_i64() {
        let cases: Vec<(i64, Vec<u8>)> = vec![
            (0, vec![0]),
            (-1, vec![1]),
            (1, vec![2]),
            (-2, vec![3]),
            (2, vec![4]),
        ];
        let mut stream = CompressionStream::new(&NoCompression::new().build());
        for (input, expected_output) in cases {
            input.write_varint(&mut stream);
            let mut out: Vec<u8> = Vec::new();
            stream.finish(&mut out).unwrap();
            assert_eq!(out, expected_output);
        }
    }


}