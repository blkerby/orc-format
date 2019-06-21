pub struct Field(pub String, pub Schema);

pub enum Schema {
    Short,
    Int,
    Long,
    String,
    Struct(Vec<Field>)
}
