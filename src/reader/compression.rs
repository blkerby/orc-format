
trait CompressionImpl {
    fn decompressor(&self) -> Option<Box<dyn Decompressor>>;
}

pub struct Compression(Box<dyn CompressionImpl>);
