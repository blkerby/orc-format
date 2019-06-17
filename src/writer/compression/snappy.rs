use snap;
use super::{CompressionImpl, Compression, Compressor, MAX_BLOCK_SIZE};
use crate::buffer::Buffer;

pub struct SnappyCompression { 
    block_size: usize,
}

impl SnappyCompression {
    pub fn new() -> SnappyCompression {
        SnappyCompression { 
            block_size: 262144,
        }
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        assert!(block_size <= MAX_BLOCK_SIZE);
        self.block_size = block_size;
        self
    } 

    pub fn build(self) -> Compression {
        Compression(Box::new(self))
    }
}

impl CompressionImpl for SnappyCompression {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn compressor(&self) -> Option<Box<dyn Compressor>> {
        Some(Box::new(SnappyCompressor {
            encoder: snap::Encoder::new(),
        }))
    }
}

struct SnappyCompressor {
    encoder: snap::Encoder,
}

impl Compressor for SnappyCompressor {
    fn compress(&mut self, input: &[u8], output: &mut Buffer) {
        let current_len = output.len();
        let max_additional_len = snap::max_compress_len(input.len());
        output.ensure_size(current_len + max_additional_len);
        let additional_len = self.encoder.compress(input, &mut output[current_len..]).unwrap();
        output.resize(current_len + additional_len);
    }
}
