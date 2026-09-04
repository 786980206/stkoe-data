use std::sync::Arc;

use crate::types::DataType;

/// 字段逻辑描述：只有名称与类型，不携带 encoding / compression / offset 等物理属性。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSchema {
    pub name: Arc<str>,
    pub data_type: DataType,
}

impl FieldSchema {
    pub fn new(name: impl Into<Arc<str>>, data_type: DataType) -> Self {
        FieldSchema { name: name.into(), data_type }
    }
}

/// 逻辑 Schema。从 Dataset / Data 的逻辑结构得到，不独立持久化（无 Schema 文件）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<FieldSchema>,
}

impl Schema {
    pub fn new(fields: Vec<FieldSchema>) -> Self {
        Schema { fields }
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| &*f.name == name)
    }

    pub fn data_type_of(&self, name: &str) -> Option<DataType> {
        self.position(name).map(|i| self.fields[i].data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_by_name() {
        let schema =
            Schema::new(vec![
                FieldSchema::new("sym", DataType::UInt16),
                FieldSchema::new("time", DataType::TimestampUs),
                FieldSchema::new("price", DataType::Float64),
            ]);
        assert_eq!(schema.len(), 3);
        assert_eq!(schema.position("price"), Some(2));
        assert_eq!(schema.data_type_of("price"), Some(DataType::Float64));
        assert_eq!(schema.data_type_of("missing"), None);
    }
}
