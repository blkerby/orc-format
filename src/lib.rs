pub struct Field {
    name: String,
    schema: TypeDescription
}

pub enum TypeDescription {
    Int,
    Struct(Vec<Field>)
}

struct TypeData {
    id: u32,
}

struct Writer<'a, T: Write> {
    inner: T,
    schema: &'a TypeDescription
}





struct RowBatch {

}

mod protos;
mod buffer;
mod reader;
mod writer;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer() {
        let schema = TypeDescription::Int;
        let mut v: Vec<u8> = vec![];
        let mut writer = Writer::new(&mut v, &schema);
        writer.write_footer();
        println!("{:?}", v);
    }
}
