use zstd;
use zstd_safe;

use crate::protos::orc_proto;
use super::common::{CompressionTrait, Compressor, MAX_BLOCK_SIZE};
use crate::buffer::Buffer;

#[derive(Clone)]
pub struct ZstdCompression { 
    block_size: usize,
    compression_level: i32,
}

impl ZstdCompression {
    pub fn new() -> Self {
        Self { 
            block_size: 262144,
            compression_level: 3,
        }
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        assert!(block_size <= MAX_BLOCK_SIZE);
        self.block_size = block_size;
        self
    }

    pub fn with_compression_level(mut self, compression_level: i32) -> Self {
        assert!(compression_level >= 1 && compression_level <= 22);
        self.compression_level = compression_level;
        self
    }
}

impl CompressionTrait for ZstdCompression {
    fn kind(&self) -> orc_proto::CompressionKind {
        orc_proto::CompressionKind::ZSTD
    }

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn compressor(&self) -> Option<Box<dyn Compressor>> {
        Some(Box::new(ZstdCompressor {
            compression_level: self.compression_level
        }))
    }
}

struct ZstdCompressor {
    compression_level: i32,
}

impl Compressor for ZstdCompressor {
    fn compress(&mut self, input: &[u8], output: &mut Buffer) {
        let current_len = output.len();
        let max_additional_len = zstd_safe::compress_bound(input.len());
        output.ensure_size(current_len + max_additional_len);
        let additional_len = zstd::block::compress_to_buffer(input, 
            &mut output[current_len..], self.compression_level).unwrap();
        output.resize(current_len + additional_len);
    }
}
