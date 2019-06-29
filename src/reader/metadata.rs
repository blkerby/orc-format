use crate::schema::Schema;
use crate::reader::compression::Compression;

pub struct Compression {

}

pub struct Splice {

}

pub struct Metadata {
    schema: Schema;
    compression: Compression;
    splices: Vec<Splice>;
}