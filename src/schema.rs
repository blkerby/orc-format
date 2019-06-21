pub struct Field(pub String, pub Schema);

pub enum Schema {
    Short,
    Int,
    Long,
    Date,
    Float,
    Double,
    String,
    Struct(Vec<Field>)
}
