mod protos;
mod reader;
mod writer;
mod schema;

pub use writer::Writer;

#[cfg(test)]
mod tests {
    use super::*;
    use super::schema::*;

    #[test]
    fn writer() {
        let schema = Schema::Int;
        let mut v: Vec<u8> = vec![];
        let writer = Writer::new(&mut v, &schema);
        writer.finish();
        println!("{:?}", v);
    }
}
