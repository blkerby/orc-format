use std::io::Write;
use std::slice;
use std::iter;

struct Buffer(Vec<u8>);

// impl Write for Buffer {
//     fn write(&mut self, buf: &[u8]) -> Result<usize> {
//         self.0.write(buf)
//     }

//     fn flush(&mut self) -> Result<()> {
//         Ok(())
//     }
// }

impl Buffer {
    pub fn new() -> Self {
        Buffer(Vec::new())
    }

    pub fn finish<W: Write>(&mut self, out: &mut W) -> usize {
        let len = self.0.len();
        out.write_all(&self.0);
        self.0.clear();
        len
    }

    pub fn write_u8(&mut self, b: u8) {
        self.0.push(b);
    }

    pub fn write_bytes(&mut self, buf: &[u8]) {
        self.0.extend(buf);
    }
}



pub struct ByteRLE {
    sink: Buffer,
    buf: [u8; 128],
    buf_len: usize,
    run_len: usize,
}

impl ByteRLE {
    pub fn new() -> Self {
        ByteRLE {
            sink: Buffer::new(),
            buf: [0; 128],
            run_len: 0,
            buf_len: 0,
        }
    }

    pub fn write(&mut self, x: u8) {
        if self.buf_len == 128 || self.run_len == 130 {
            self.finish_group();
        }
        if self.run_len > 0 {
            if x == self.buf[0] {
                self.run_len += 1
            } else {
                self.finish_group();
                self.buf[self.buf_len] = x;
                self.buf_len += 1;
            }
        } else {
            if self.buf_len >= 2 && x == self.buf[self.buf_len - 1] && x == self.buf[self.buf_len - 2] {
                self.buf_len -= 2;
                self.finish_group();
                self.run_len = 3;
                self.buf_len = 1;
                self.buf[0] = x;
            } else {
                self.buf[self.buf_len] = x;
                self.buf_len += 1;
            }
        }
    }

    fn finish_group(&mut self) {
        if self.run_len > 0 {
            self.sink.write_u8((self.run_len - 3) as u8);
            self.sink.write_u8(self.buf[0]);
            self.buf_len = 0;
            self.run_len = 0;
        } else if self.buf_len > 0 {
            self.sink.write_u8(-(self.buf_len as isize) as u8);
            self.sink.write_bytes(&self.buf[..self.buf_len as usize]);
            self.buf_len = 0;
        }
    }

    pub fn finish<W: Write>(&mut self, w: &mut W) -> usize {
        self.finish_group();
        self.sink.finish(w)
    }
}

pub struct BooleanRLE {
    byte_rle: ByteRLE,
    buf: u8,
    cnt: u8
}

impl BooleanRLE {
    pub fn new() -> Self {
        BooleanRLE {
            byte_rle: ByteRLE::new(),
            buf: 0,
            cnt: 0,
        }
    }

    pub fn write(&mut self, x: bool) {
        self.buf = self.buf << 1 | (x as u8);
        if self.cnt == 7 {
            self.cnt = 0;
            self.byte_rle.write(self.buf);
        } else {
            self.cnt += 1;
        }
    }

    pub fn finish<W: Write>(&mut self, out: &mut W) -> usize {
        if self.cnt > 0 {
            self.byte_rle.write(self.buf << (8 - self.cnt));
        }
        self.byte_rle.finish(out)
    }
}

trait VarInt: Copy + Default {
    fn write_varint(self, out: &mut Buffer);
    fn wrapping_sub_i64(self, rhs: Self) -> i64;
}

impl VarInt for u64 {
    fn write_varint(mut self, out: &mut Buffer) {
        while self >= 0x80 {
            out.write_u8((0x80 | (self & 0x7f)) as u8);
            self >>= 7;
        }
        out.write_u8(self as u8);
    }

    fn wrapping_sub_i64(self, rhs: Self) -> i64 {
        self.wrapping_sub(rhs) as i64
    }
}

impl VarInt for i64 {        
    fn write_varint(mut self, out: &mut Buffer) {
        let zigzag_encoding = (self << 1) ^ (self >> 63);
        (zigzag_encoding as u64).write_varint(out);
    }

    fn wrapping_sub_i64(self, rhs: Self) -> i64 {
        self.wrapping_sub(rhs)
    }
}

struct IntRLEv1<T: VarInt> {
    sink: Buffer,
    buf: Vec<T>,
    run_len: u8,
    last_val: T,
    delta: i64,
}

impl<T: VarInt> IntRLEv1<T> {
    pub fn new() -> Self {
        IntRLEv1 {
            sink: Buffer::new(),
            buf: Vec::new(),
            run_len: 0,
            last_val: T::default(),
            delta: 0,
        }
    }

    pub fn write(&mut self, x: T) {
        println!("buf len {}, run_len {}, delta {}", self.buf.len(), self.run_len, self.delta);
        let len = self.buf.len();
        if len == 128 || self.run_len == 130 {
            self.finish_group();
        }
        if self.run_len > 0 {
            if x.wrapping_sub_i64(self.last_val) == self.delta {
                self.run_len += 1
            } else {
                self.finish_group();
                self.buf.push(x);
            }
        } else {
            let len = self.buf.len();
            if len == 0 {
                self.buf.push(x);
                return;
            }
            
            let delta = x.wrapping_sub_i64(*self.buf.last().unwrap());
            if len >= 2 && delta == self.delta && delta >= -128 && delta <= 127 {
                self.buf.pop().unwrap();
                let y = self.buf.pop().unwrap();
                self.finish_group();
                self.run_len = 3;
                self.buf.push(y);
            } else {
                self.buf.push(x);
                self.delta = delta;
            }
        }
        self.last_val = x;
    }

    pub fn finish_group(&mut self) {
        println!("finish_group: buf len {}, run_len {}, delta {}", self.buf.len(), self.run_len, self.delta);
        if self.run_len > 0 {
            self.sink.write_u8(self.run_len - 3);
            self.sink.write_u8(self.delta as u8);
            self.buf[0].write_varint(&mut self.sink);
            self.buf.clear();
            self.run_len = 0;
        } else if self.buf.len() > 0 {
            self.sink.write_u8(-(self.buf.len() as isize) as u8);
            for x in &self.buf {
                x.write_varint(&mut self.sink);
            }
            self.buf.clear();
        }
    }

    pub fn finish<W: Write>(&mut self, w: &mut W) -> usize {
        self.finish_group();
        self.sink.finish(w)
    }
}

pub struct SignedIntRLEv1(IntRLEv1<i64>);

impl SignedIntRLEv1 {
    pub fn new() -> Self { SignedIntRLEv1(IntRLEv1::new()) }
    pub fn write(&mut self, x: i64) { self.0.write(x); }
    pub fn finish<W: Write>(&mut self, w: &mut W) -> usize { self.0.finish(w) }
}

pub struct UnsignedIntRLEv1(IntRLEv1<u64>);

impl UnsignedIntRLEv1 {
    pub fn new() -> Self { UnsignedIntRLEv1(IntRLEv1::new()) }
    pub fn write(&mut self, x: u64) { self.0.write(x); }
    pub fn finish<W: Write>(&mut self, w: &mut W) -> usize { self.0.finish(w) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case_derive::test_case;

    #[test_case(vec![], vec![] :: "empty")]
    #[test_case(vec![10], vec![255, 10] :: "single")]
    #[test_case(vec![10, 20, 30], vec![253, 10, 20, 30] :: "literal")]
    #[test_case(vec![10, 10, 10], vec![0, 10] :: "run")]
    #[test_case(vec![10, 20, 20, 20, 20], vec![255, 10, 1, 20] :: "literal then run")]
    #[test_case(vec![10, 10, 10, 10, 10, 20, 30], vec![2, 10, 254, 20, 30] :: "run then literal")]
    #[test_case(vec![10, 20, 20, 30], vec![252, 10, 20, 20, 30] :: "literal including false run")]
    #[test_case(iter::repeat(10).take(131).collect(), vec![127, 10, 255, 10] :: "long run")]
    #[test_case((0..140).collect(), [vec![128], (0..128).collect(), vec![244], (128..140).collect()].concat() :: "long literal")]
    fn test_byte_rle(input: Vec<u8>, expected_output: Vec<u8>) {
        let mut rle = ByteRLE::new();
        for x in input {
            rle.write(x);
        }
        let mut out: Vec<u8> = Vec::new();
        rle.finish(&mut out);
        assert_eq!(out, expected_output);
    }

    #[test_case(vec![], vec![] :: "empty")]
    #[test_case(vec![true, false, true, false, true, false, false, false, true], vec![254, 0b10101000, 0b10000000] :: "literal")]
    #[test_case(vec![false; 80], vec![7, 0] :: "run")]
    fn test_boolean_rle(input: Vec<bool>, expected_output: Vec<u8>) {
        let mut rle = BooleanRLE::new();
        for x in input {
            rle.write(x);
        }
        let mut out: Vec<u8> = Vec::new();
        rle.finish(&mut out);
        assert_eq!(out, expected_output);
    }

    #[test_case(0, vec![0x00])]
    #[test_case(1, vec![0x01])]
    #[test_case(127, vec![0x7f])]
    #[test_case(128, vec![0x80, 0x01])]
    #[test_case(129, vec![0x81, 0x01])]
    #[test_case(16383, vec![0xff, 0x7f])]
    #[test_case(16384, vec![0x80, 0x80, 0x01])]
    #[test_case(16385, vec![0x81, 0x80, 0x01])]
    fn test_write_varint_u64(input: u64, expected_output: Vec<u8>) {
        let mut buf = Buffer::new();
        input.write_varint(&mut buf);
        assert_eq!(buf.0, expected_output);
    }

    #[test_case(0, vec![0])]
    #[test_case(-1, vec![1])]
    #[test_case(1, vec![2])]
    #[test_case(-2, vec![3])]
    #[test_case(2, vec![4])]
    fn test_write_varint_i64(input: i64, expected_output: Vec<u8>) {
        let mut buf = Buffer::new();
        input.write_varint(&mut buf);
        assert_eq!(buf.0, expected_output);
    }

    #[test_case(vec![], vec![] :: "empty")]
    #[test_case(vec![10], vec![255, 20] :: "single")]
    #[test_case(vec![0, -1, 1, -2, 2], vec![251, 0, 1, 2, 3, 4] :: "literal")]
    #[test_case(vec![10, 10, 10, 10], vec![1, 0, 20] :: "run_zero")]
    #[test_case(vec![10, 15, 20, 25], vec![1, 5, 20] :: "run")]
    #[test_case(vec![10, 15, 20, 25, 0], vec![1, 5, 20, 255, 0] :: "run then literal")]
    fn test_signed_int_rle_v1(input: Vec<i64>, expected_output: Vec<u8>) {
        let mut rle = SignedIntRLEv1::new();
        for x in input {
            rle.write(x);
        }
        let mut out: Vec<u8> = Vec::new();
        rle.finish(&mut out);
        assert_eq!(out, expected_output);
    }

    #[test_case(vec![7; 100], vec![97, 0, 7] :: "run")]
    fn test_unsigned_int_rle_v1(input: Vec<u64>, expected_output: Vec<u8>) {
        let mut rle = UnsignedIntRLEv1::new();
        for x in input {
            rle.write(x);
        }
        let mut out: Vec<u8> = Vec::new();
        rle.finish(&mut out);
        assert_eq!(out, expected_output);
    }

}