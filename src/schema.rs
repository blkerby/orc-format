pub struct Field {
    pub name: String,
    pub schema: Schema
}

pub enum Schema {
    Short,
    Int,
    Long,
    Struct(Vec<Field>)
}
