use std::io::{Write, Result};
use crate::writer::compression::{Compression, CompressionStream, CompressionStreamPosition};
use super::varint::VarInt;

struct IntRLEv1<T: VarInt> {
    sink: CompressionStream,
    buf: Vec<T>,
    run_len: u8,
    last_val: T,
    delta: i64,
}

#[derive(Copy, Clone)]
pub struct IntRLEv1Position {
    inner: CompressionStreamPosition,
    rle_offset: u64,
}

impl IntRLEv1Position {
    pub fn record(&self, out: &mut Vec<u64>) {
        self.inner.record(out);
        out.push(self.rle_offset);
    }
}

impl<T: VarInt> IntRLEv1<T> {
    pub fn new(compression: &Compression) -> Self {
        IntRLEv1 {
            sink: CompressionStream::new(compression),
            buf: Vec::new(),
            run_len: 0,
            last_val: T::default(),
            delta: 0,
        }
    }

    pub fn position(&self) -> IntRLEv1Position {
        IntRLEv1Position {
            inner: self.sink.position(),
            rle_offset: if self.run_len > 0 {
                    self.run_len as u64
                } else {
                    self.buf.len() as u64
                }
        }
    }

    #[inline(always)]
    pub fn write(&mut self, x: T) {
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

    pub fn finish<W: Write>(&mut self, w: &mut W) -> Result<()> {
        self.finish_group();
        self.sink.finish(w)
    }

    pub fn estimated_size(&self) -> usize {
        self.sink.estimated_size()
    }
}

pub struct SignedIntRLEv1(IntRLEv1<i64>);

// When it arrives to Rust, "delegation" could be used to eliminate this boilerplate:
impl SignedIntRLEv1 {
    pub fn new(compression: &Compression) -> Self {
        SignedIntRLEv1(IntRLEv1::new(compression))
    }

    #[inline(always)]
    pub fn write(&mut self, x: i64) {
        self.0.write(x);
    }

    pub fn finish<W: Write>(&mut self, w: &mut W) -> Result<()> {
        self.0.finish(w)
    }

    pub fn position(&self) -> IntRLEv1Position {
        self.0.position()
    }

    pub fn estimated_size(&self) -> usize {
        self.0.estimated_size()
    }
}

pub struct UnsignedIntRLEv1(IntRLEv1<u64>);

impl UnsignedIntRLEv1 {
    pub fn new(compression: &Compression) -> Self {
        UnsignedIntRLEv1(IntRLEv1::new(compression))
    }

    #[inline(always)]
    pub fn write(&mut self, x: u64) {
        self.0.write(x);
    }

    pub fn position(&self) -> IntRLEv1Position {
        self.0.position()
    }

    pub fn finish<W: Write>(&mut self, w: &mut W) -> Result<()> {
        self.0.finish(w)
    }

    pub fn estimated_size(&self) -> usize {
        self.0.estimated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::compression::NoCompression;

    #[test]
    fn test_signed_int_rle_v1() {
        let cases = vec![
            (vec![], vec![]),
            (vec![10], vec![255, 20]),
            (vec![0, -1, 1, -2, 2], vec![251, 0, 1, 2, 3, 4]),
            (vec![10, 10, 10, 10], vec![1, 0, 20]),
            (vec![10, 15, 20, 25], vec![1, 5, 20]),
            (vec![10, 15, 20, 25, 0], vec![1, 5, 20, 255, 0]),
        ];
        let mut rle = SignedIntRLEv1::new(&NoCompression::new().build());
        for (input, expected_output) in cases {
            for x in input {
                rle.write(x);
            }
            let mut out: Vec<u8> = Vec::new();
            rle.finish(&mut out).unwrap();
            assert_eq!(out, expected_output);
        }
    }

    #[test]
    fn test_unsigned_int_rle_v1() {
        let cases = vec![
            (vec![7; 100], vec![97, 0, 7])
        ];
        let mut rle = UnsignedIntRLEv1::new(&NoCompression::new().build());
        for (input, expected_output) in cases {
            for x in input {
                rle.write(x);
            }
            let mut out: Vec<u8> = Vec::new();
            rle.finish(&mut out).unwrap();
            assert_eq!(out, expected_output);
        }
    }
}