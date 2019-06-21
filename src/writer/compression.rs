use crate::protos::orc_proto;
use std::io::{Result, Write};
use byteorder::{LittleEndian, WriteBytesExt};



use crate::buffer::Buffer;

pub mod snappy;

const MAX_BLOCK_SIZE: usize = 0x7fffff;

trait CompressionImpl {
    fn kind(&self) -> orc_proto::CompressionKind;
    fn block_size(&self) -> usize;
    fn compressor(&self) -> Option<Box<dyn Compressor>>;
}

pub struct Compression(Box<dyn CompressionImpl>);

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

trait Compressor {
    fn compress(&mut self, input: &[u8], output: &mut Buffer);
}

// Include a private dummy field inside, to prevent external construction of the struct
// except through the public API. This leaves open the possibility for us to add additional
// fields later in a backward-compatible way.
pub struct NoCompression {
    _dummy: (),
}

impl NoCompression {
    pub fn new() -> NoCompression {
        NoCompression { _dummy: () }
    }

    pub fn build(self) -> Compression {
        Compression(Box::new(self))
    }
}

impl CompressionImpl for NoCompression {
    fn kind(&self) -> orc_proto::CompressionKind {
        orc_proto::CompressionKind::NONE
    }

    fn block_size(&self) -> usize {
        0
    }

    fn compressor(&self) -> Option<Box<dyn Compressor>> {
        None
    }
}


struct BlockInfo {
    is_original: bool,
    length: usize,
}

#[derive(Eq, PartialEq)]
pub struct CompressionStreamPosition {
    pub chunk_position: usize,
    pub chunk_offset: usize,
}

pub struct CompressionStream {
    compressor: Option<Box<dyn Compressor>>,
    buf: Buffer,
    output: Buffer,
    output_block_info: Vec<BlockInfo>,
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
            chunk_position: self.output.len(),
            chunk_offset: self.buf.len(),
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
                self.output[i..(i + self.buf.len())].copy_from_slice(&self.buf);
                self.output_block_info.push(BlockInfo {
                    is_original: true,
                    length: self.buf.len(),
                });
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

    pub fn finish<W: Write>(&mut self, out: &mut W) -> Result<u64> {
        if let Some(_) = &self.compressor {
            self.finish_block();
            let mut i = 0;
            let mut size = 0;
            for info in &self.output_block_info {
                let header = info.length * 2 + (info.is_original as usize);
                out.write_u24::<LittleEndian>(header as u32)?;
                out.write_all(&self.output[i..(i + info.length)])?;
                i += info.length;
                size += info.length + 3;
            }
            println!("Compressed size: {}", size);
            Ok(size as u64)
        } else {
            println!("Uncompressed buf len: {}", self.buf.len());
            out.write_all(&self.buf)?;
            let len = self.buf.len();
            self.buf.resize(0);
            Ok(len as u64)
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
