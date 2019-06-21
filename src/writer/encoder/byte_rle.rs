use std::io::{Write, Result};
use std::mem;

use crate::writer::compression::{Compression, CompressionStream, CompressionStreamPosition};


pub struct ByteRLE {
    sink: CompressionStream,
    buf: [u8; 128],
    buf_len: usize,
    run_len: usize,
    row_group_start: ByteRLEPosition,
}

pub struct ByteRLEPosition {
    pub compression_stream_pos: CompressionStreamPosition,
    pub run_offset: usize,
}

impl ByteRLE {
    pub fn new(compression: &Compression) -> Self {
        ByteRLE {
            sink: CompressionStream::new(compression),
            buf: [0; 128],
            run_len: 0,
            buf_len: 0,
            row_group_start: ByteRLEPosition {
                compression_stream_pos: CompressionStreamPosition {
                    chunk_position: 0,
                    chunk_offset: 0,
                },
                run_offset: 0,
            }
        }
    }

    pub fn finish_row_group(&mut self) -> ByteRLEPosition {
        mem::replace(&mut self.row_group_start, ByteRLEPosition {
            compression_stream_pos: self.sink.position(),
            run_offset: if self.run_len > 0 { self.run_len } else { self.buf_len }
        })
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
                let new_run_offset = if
                    self.row_group_start.compression_stream_pos == self.sink.position() &&
                        self.row_group_start.run_offset >= self.buf_len - 2 {
                    Some(self.row_group_start.run_offset - (self.buf_len - 2))
                } else { None };
                self.buf_len -= 2;
                self.finish_group();
                self.run_len = 3;
                self.buf_len = 1;
                self.buf[0] = x;
                if let Some(r) = new_run_offset {
                    self.row_group_start.compression_stream_pos = self.sink.position();
                    self.row_group_start.run_offset = r;
                }
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

    pub fn finish<W: Write>(&mut self, w: &mut W) -> Result<u64> {
        self.finish_group();
        self.sink.finish(w)
    }

    pub fn estimated_size(&self) -> usize {
        self.sink.estimated_size()
    }
}
