use std::io::Write;
use std::slice;

use super::Buffer;

pub struct ByteRLE {
    sink: Buffer,
    buf: Vec<u8>,
    run_len: u8,
}

impl ByteRLE {
    pub fn new() -> Self {
        ByteRLE {
            sink: Buffer::new(),
            buf: Vec::with_capacity(128),
            run_len: 0,
        }
    }

    pub fn write(&mut self, x: u8) {
        let len = self.buf.len();
        if len == 128 || self.run_len == 127 {
            self.finish_group();
        }
        if self.run_len > 0 {
            if x == self.buf[0] {
                self.run_len += 1
            } else {
                self.finish_group();
                self.buf.push(x);
            }
        } else {
            if len >= 2 && x == self.buf[len - 1] && x == self.buf[len - 2] {
                self.buf.pop();
                self.buf.pop();
                self.finish_group();
                self.run_len = 3;
                self.buf.push(x);
            } else {
                self.buf.push(x);
            }
        }
    }

    pub fn finish_group(&mut self) {
        if self.run_len > 0 {
            self.sink.write_u8(self.run_len - 3);
            self.sink.write_u8(self.buf[0]);
            self.buf.clear();
            self.run_len = 0;
        } else if self.buf.len() > 0 {
            self.sink.write_u8(-(self.buf.len() as isize) as u8);
            self.sink.write_bytes(&self.buf);
            self.buf.clear();
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

fn write_varint_u64(mut x: u64, out: &mut Buffer) {
    while (x >= 0x80) {
        out.write_u8((0x80 | (x & 0x7f)) as u8);
        x >>= 7;
    }
    out.write_u8(x as u8);
}

fn write_varint_i64(x: i64, out: &mut Buffer) {
    let u = x as u64;
    let zigzag_encoding = (u << 1) ^ (u >> 63);
    write_varint_u64(zigzag_encoding, out);
}

pub struct SignedIntRLEv1 {
    sink: Buffer,
    buf: Vec<i64>,
    run_len: u8,
    delta: i64,
}

impl SignedIntRLEv1 {
    pub fn new() -> Self {
        SignedIntRLEv1 {
            sink: Buffer::new(),
            buf: Vec::new(),
            run_len: 0,
            delta: 0,
        }
    }

    pub fn write(&mut self, x: i64) {
        let len = self.buf.len();
        if len == 128 || self.run_len == 127 {
            self.finish_group();
        }
        if self.run_len > 0 {
            if x - self.buf.last().unwrap() == self.delta {
                self.run_len += 1
            } else {
                self.finish_group();
                self.buf.push(x);
            }
        } else {
            let len = self.buf.len();
            let delta = x - self.buf.last().unwrap();
            if len >= 2 && delta == self.delta && delta >= -128 && delta <= 127 {
                self.buf.pop();
                self.buf.pop();
                self.finish_group();
                self.run_len = 3;
                self.buf.push(x);
            } else {
                self.buf.push(x);
            }
            self.delta = delta;
        }
    }

    pub fn finish_group(&mut self) {
        if self.run_len > 0 {
            self.sink.write_u8(self.run_len - 3);
            self.sink.write_u8(self.delta as u8);
            write_varint_i64(self.buf[0], &mut self.sink);
            self.buf.clear();
            self.run_len = 0;
        } else if self.buf.len() > 0 {
            self.sink.write_u8(-(self.buf.len() as isize) as u8);
            for x in &self.buf {
                write_varint_i64(*x, &mut self.sink);
            }
            self.buf.clear();
        }
    }

    pub fn finish<W: Write>(&mut self, w: &mut W) -> usize {
        self.finish_group();
        self.sink.finish(w)
    }
}

// pub struct UnsignedIntRLEv1 {

// }

#[cfg(test)]
mod tests {
    use super::*;
    use test_case_derive::test_case;

    #[test_case(vec![10, 20, 30], vec![253, 10, 20, 30] :: "literal")]
    #[test_case(vec![10, 10, 10], vec![0, 10] :: "run")]
    #[test_case(vec![10, 20, 20, 20, 20], vec![255, 10, 1, 20] :: "literal then run")]
    #[test_case(vec![10, 10, 10, 10, 10, 20, 30], vec![2, 10, 254, 20, 30] :: "run then literal")]
    #[test_case(vec![10, 20, 20, 30, 30], vec![251, 10, 20, 20, 30, 30] :: "literal including false runs")]
    fn test_byte_rle(input: Vec<u8>, expected_output: Vec<u8>) {
        let mut byte_rle = ByteRLE::new();
        for x in input {
            byte_rle.write(x);
        }
        let mut out: Vec<u8> = Vec::new();
        byte_rle.finish(&mut out);
        assert_eq!(out, expected_output);
    }

    
}