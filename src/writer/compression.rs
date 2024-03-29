use crate::protos::orc_proto;
use std::io::{Result, Write};
use byteorder::{LittleEndian, WriteBytesExt};


use crate::buffer::Buffer;
use common::{CompressionTrait, Compressor};

pub use no_compression::NoCompression;
pub use snappy::SnappyCompression;
pub use self::zstd::ZstdCompression;

mod common;
mod no_compression;
mod snappy;
mod zstd;

#[derive(Clone)]
pub struct Compression(CompressionEnum);

impl Compression {
    pub(crate) fn kind(&self) -> orc_proto::CompressionKind {
        self.0.kind()
    }

    pub(crate) fn block_size(&self) -> usize {
        self.0.block_size()
    }

    fn compressor(&self) -> Option<Box<dyn Compressor>> {
        self.0.compressor()
    }
}

#[derive(Clone)]
enum CompressionEnum {
    No(NoCompression),
    Snappy(SnappyCompression),
    Zstd(ZstdCompression),
}

// We could eliminate this boilerplate using enum-dispatch, but it doesn't work yet with RLS.
// Maybe use macros ...
impl CompressionTrait for CompressionEnum {
    fn kind(&self) -> orc_proto::CompressionKind {
        match self {
            CompressionEnum::No(x) => x.kind(),
            CompressionEnum::Snappy(x) => x.kind(),
            CompressionEnum::Zstd(x) => x.kind(),
        }
    }

    fn block_size(&self) -> usize {
        match self {
            CompressionEnum::No(x) => x.block_size(),
            CompressionEnum::Snappy(x) => x.block_size(),
            CompressionEnum::Zstd(x) => x.block_size(),
        }
    }

    fn compressor(&self) -> Option<Box<dyn Compressor>> {
        match self {
            CompressionEnum::No(x) => x.compressor(),
            CompressionEnum::Snappy(x) => x.compressor(),
            CompressionEnum::Zstd(x) => x.compressor(),
        }
    }

}

impl NoCompression {
    pub fn build(self) -> Compression {
        Compression(CompressionEnum::No(self))
    }
}

impl SnappyCompression {
    pub fn build(self) -> Compression {
        Compression(CompressionEnum::Snappy(self))
    }
}

impl ZstdCompression {
    pub fn build(self) -> Compression {
        Compression(CompressionEnum::Zstd(self))
    }

}

struct BlockInfo {
    is_original: bool,
    length: usize,
}

pub(crate) struct CompressionStream {
    compressor: Option<Box<dyn Compressor>>,
    buf: Buffer,
    output: Buffer,
    output_block_info: Vec<BlockInfo>,
}

#[derive(Copy, Clone)]
pub(crate) struct CompressionStreamPosition {
    // Offset of the start of the compression block (if applicable)
    block_start: Option<u64>,
    // Offset within the block
    offset: u64,
}

impl CompressionStreamPosition {
    pub fn record(&self, out: &mut Vec<u64>) {
        if let Some(x) = self.block_start {
            out.push(x);
        }
        out.push(self.offset);
    }
}

impl CompressionStream {
    pub fn new(compression: &Compression) -> Self {
        CompressionStream {
            compressor: compression.compressor(),
            buf: Buffer::with_capacity(compression.block_size()),
            output: Buffer::new(),
            output_block_info: Vec::new(),
        }
    }

    pub fn position(&self) -> CompressionStreamPosition {
        CompressionStreamPosition {
            block_start: if let Some(_) = &self.compressor {
                    Some((self.output.len() + 3*self.output_block_info.len()) as u64)
                } else { None },
            offset: self.buf.len() as u64,
        }
    }

    fn finish_block(&mut self) {
        if self.buf.len() == 0 {
            return;
        }
        if let Some(compressor) = &mut self.compressor {
            let i = self.output.len();
            compressor.compress(&self.buf, &mut self.output);
            let len = self.output.len() - i;
            if len > self.buf.len() {
                // Compression was unsuccessful, in that the compressed output was larger than
                // the input. In this case, the ORC spec requires that we instead store the
                // original uncompressed data. With a little fancier bookkeeping, we could
                // avoid copying here and just keep the data where it already is (in self.buf).
                let new_output_len = i + self.buf.len();
                self.output[i..new_output_len].copy_from_slice(&self.buf);
                self.output_block_info.push(BlockInfo {
                    is_original: true,
                    length: self.buf.len(),
                });
                self.output.resize(new_output_len);
            } else {
                self.output_block_info.push(BlockInfo {
                    is_original: false,
                    length: len,
                });
            }
            self.buf.resize(0);
        }
    }

    #[inline(always)]
    pub fn write_u8(&mut self, b: u8) {
        if self.buf.len() >= self.buf.capacity() {
            self.finish_block();
            self.buf.write_u8(b);
        } else {
            self.buf.write_u8(b);
        }
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        if let Some(_) = &mut self.compressor {
            if self.buf.len() + bytes.len() > self.buf.capacity() {
                let i = self.buf.capacity() - self.buf.len();
                self.buf.write_bytes(&bytes[..i]);
                self.finish_block();
                self.buf.resize(0);
                self.buf.write_bytes(&bytes[i..]);
            } else {
                self.buf.write_bytes(bytes);
            }
        } else {
            self.buf.write_bytes(bytes);
        }
    }

    pub fn finish<W: Write>(&mut self, out: &mut W) -> Result<()> {
        if let Some(_) = &self.compressor {
            self.finish_block();
            let mut i = 0;
            for info in &self.output_block_info {
                let header = info.length * 2 + (info.is_original as usize);
                out.write_u24::<LittleEndian>(header as u32)?;
                out.write_all(&self.output[i..(i + info.length)])?;
                i += info.length;
            }
            self.output.resize(0);
            self.output_block_info.clear();
            Ok(())
        } else {
            out.write_all(&self.buf)?;
            self.buf.resize(0);
            Ok(())
        }
    }

    pub fn estimated_size(&self) -> usize {
        self.output.len() + self.buf.len()
    }
}

impl Write for CompressionStream {
    fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        self.write_bytes(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
