use std::io::{Write, Result};
use crate::writer::compression::{Compression, CompressionStream};
use super::varint::VarInt;

struct IntRLEv1<T: VarInt> {
    sink: CompressionStream,
    buf: Vec<T>,
    run_len: u8,
    last_val: T,
    delta: i64,
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

    pub fn finish<W: Write>(&mut self, w: &mut W) -> Result<()> {
        self.0.finish(w)
    }

    pub fn estimated_size(&self) -> usize {
        self.0.estimated_size()
    }
}
