use super::encoder::{BooleanRLE, SignedIntRLEv1};

struct LongData {
    present: BooleanRLE,
    data: SignedIntRLEv1,
}

struct StructData {
    present: BooleanRLE,
    children: Vec<Data>
}

enum Data {
    Long(LongData),
    Struct(StructData)
}
