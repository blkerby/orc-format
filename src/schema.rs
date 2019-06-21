pub struct Field(pub String, pub Schema);

pub enum Schema {
    Short,
    Int,
    Long,
    Float,
    Double,
    String,
    Struct(Vec<Field>)
}
