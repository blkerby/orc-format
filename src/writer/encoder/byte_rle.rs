use crate::writer::compression::{Compression, CompressionStream};
use std::io::{Write, Result};


pub struct ByteRLE {
    sink: CompressionStream,
    buf: [u8; 128],
    buf_len: usize,
    run_len: usize,
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
