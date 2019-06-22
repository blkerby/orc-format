pub struct Field(pub String, pub Schema);

pub enum Schema {
    Boolean,
    Short,
    Int,
    Long,
    Date,
    Float,
    Double,
    // Decimal(u8, u8),
    String,
    Struct(Vec<Field>)
}
