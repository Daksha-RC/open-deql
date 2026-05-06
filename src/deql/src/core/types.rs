/// DeQL data types for field definitions.
#[derive(Debug, Clone, PartialEq)]
pub enum DeqlType {
    Uuid,
    String,
    Int,
    Decimal { precision: u8, scale: u8 },
    Timestamp,
    Boolean,
}

/// A typed field definition used in aggregates, commands, and events.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub data_type: DeqlType,
    pub is_key: bool,
}

/// A `dotted.key = value` configuration pair.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigPair {
    pub key: String,
    pub value: ConfigValue,
}

/// Configuration value variants.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    StringLit(String),
    IntLit(i64),
    DecimalLit(f64),
    BoolLit(bool),
    List(Vec<String>),
}
