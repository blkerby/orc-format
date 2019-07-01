use crate::writer::compression::{Compression, CompressionStream, CompressionStreamPosition};
use std::io::{Write, Result};


pub struct ByteRLE {
    sink: CompressionStream,
    buf: [u8; 128],
    buf_len: usize,
    run_len: usize,
}

#[derive(Copy, Clone)]
pub struct ByteRLEPosition {
    inner: CompressionStreamPosition,
    rle_offset: u64,
}

impl ByteRLEPosition {
    pub fn record(&self, out: &mut Vec<u64>) {
        self.inner.record(out);
        out.push(self.rle_offset);
    }
}

impl ByteRLE {
    pub fn new(compression: &Compression) -> Self {
        ByteRLE {
            sink: CompressionStream::new(compression),
            buf: [0; 128],
            run_len: 0,
            buf_len: 0,
        }
    }

    pub fn position(&self) -> ByteRLEPosition {
        ByteRLEPosition {
            inner: self.sink.position(),
            rle_offset: if self.run_len > 0 {
                    self.run_len as u64
                } else {
                    self.buf_len as u64
                }
        }
    }

    #[inline(always)]
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
            if self.buf_len >= 2
                && x == self.buf[self.buf_len - 1]
                && x == self.buf[self.buf_len - 2]
            {
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

    #[inline(always)]
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

    pub fn finish<W: Write>(&mut self, w: &mut W) -> Result<()> {
        self.finish_group();
        self.sink.finish(w)
    }

    pub fn estimated_size(&self) -> usize {
        self.sink.estimated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;
    use crate::writer::NoCompression;

    #[test]
    fn test_byte_rle() {
        let cases = vec![
            (vec![], vec![]),
            (vec![10], vec![255, 10]),
            (vec![10, 20, 30], vec![253, 10, 20, 30]),
            (vec![10, 10, 10], vec![0, 10]),
            (vec![10, 20, 20, 20, 20], vec![255, 10, 1, 20]),
            (vec![10, 10, 10, 10, 10, 20, 30], vec![2, 10, 254, 20, 30]),
            (vec![10, 20, 20, 30], vec![252, 10, 20, 20, 30]),
            (iter::repeat(10).take(131).collect(), vec![127, 10, 255, 10]),
            ((0..140).collect(), [vec![128], (0..128).collect(), vec![244], (128..140).collect()].concat()),
        ];
        let mut rle = ByteRLE::new(&NoCompression::new().build());
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