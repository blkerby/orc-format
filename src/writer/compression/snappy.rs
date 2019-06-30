use snap;
use crate::protos::orc_proto;
use super::common::{CompressionTrait, Compressor, MAX_BLOCK_SIZE};
use crate::buffer::Buffer;

#[derive(Clone)]
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
}

impl CompressionTrait for SnappyCompression {
    fn kind(&self) -> orc_proto::CompressionKind {
        orc_proto::CompressionKind::SNAPPY
    }

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
