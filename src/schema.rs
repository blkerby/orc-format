pub struct Field(pub String, pub Schema);

pub enum Schema {
    Boolean,
    Short,
    Int,
    Long,
    Date,
    Float,
    Double,
    Timestamp,
    Decimal(u32, u32),
    String,
    Binary,
    Char(u32),
    VarChar(u32),
    Struct(Vec<Field>),
    List(Box<Schema>),
    Map(Box<Schema>, Box<Schema>),
    Union(Vec<Schema>),
}
