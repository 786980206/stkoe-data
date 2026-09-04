use crate::column::{Column, ColumnView};
use crate::error::FormatError;
use crate::schema::Schema;

/// 拥有 / 物化的表数据（`create_dataset` / `create_meta_file` 的输入）。
///
/// `schema` 包含全部逻辑字段（含 sym / time）；所有列统一 length；
/// 物化列都是连续单 Buffer（DataView → Data 的复制发生在此边界）。
#[derive(Debug, Clone)]
pub struct Data {
    pub schema: Schema,
    pub columns: Vec<Column>,
}

impl Data {
    pub fn new(schema: Schema, columns: Vec<Column>) -> Result<Self, FormatError> {
        if columns.len() != schema.fields.len() {
            return Err(FormatError::InvalidLayout(format!(
                "column count {} does not match schema field count {}",
                columns.len(),
                schema.fields.len()
            )));
        }
        if let Some(first) = columns.first() {
            let length = first.length();
            for (field, col) in schema.fields.iter().zip(&columns) {
                if col.length() != length {
                    return Err(FormatError::InvalidLayout(format!(
                        "column '{}' length {} != {}",
                        field.name,
                        col.length(),
                        length
                    )));
                }
            }
        }
        Ok(Data { schema, columns })
    }

    pub fn length(&self) -> usize {
        self.columns.first().map(|c| c.length()).unwrap_or(0)
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.schema.position(name).map(|i| &self.columns[i])
    }

    pub fn as_view(&self) -> DataView<'_> {
        let columns = self.columns.iter().map(|c| c.as_view()).collect();
        DataView::new(self.schema.clone(), columns)
            .expect("owned Data is consistent by construction")
    }
}

/// 多列非拥有视图：read / write 的统一交换结构。
///
/// - non-owning、read-only 优先；可只含部分字段（projection）；
/// - 所有列统一 `length`；不同列可来自不同 Buffer；列内多段对 DataView 透明。
#[derive(Debug, Clone)]
pub struct DataView<'a> {
    pub schema: Schema,
    pub columns: Vec<ColumnView<'a>>,
    length: usize,
}

impl<'a> DataView<'a> {
    pub fn new(schema: Schema, columns: Vec<ColumnView<'a>>) -> Result<Self, FormatError> {
        if columns.len() != schema.fields.len() {
            return Err(FormatError::InvalidLayout(format!(
                "column count {} does not match schema field count {}",
                columns.len(),
                schema.fields.len()
            )));
        }
        let length = columns.first().map(|c| c.length()).unwrap_or(0);
        for (field, col) in schema.fields.iter().zip(&columns) {
            if col.length() != length {
                return Err(FormatError::InvalidLayout(format!(
                    "column '{}' length {} != {}",
                    field.name,
                    col.length(),
                    length
                )));
            }
            if col.data_type() != field.data_type {
                return Err(FormatError::InvalidLayout(format!(
                    "column '{}' data type {:?} does not match schema {:?}",
                    field.name,
                    col.data_type(),
                    field.data_type
                )));
            }
        }
        Ok(DataView { schema, columns, length })
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn column(&self, name: &str) -> Option<&ColumnView<'a>> {
        self.schema.position(name).map(|i| &self.columns[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::types::DataType;

    fn col(name_data: (DataType, usize)) -> Column {
        Column::zeroed(name_data.0, name_data.1, false)
    }

    #[test]
    fn data_validates_uniform_length() {
        let schema = Schema::new(vec![
            crate::schema::FieldSchema::new("a", DataType::Int32),
            crate::schema::FieldSchema::new("b", DataType::Int64),
        ]);
        let ok = Data::new(
            schema.clone(),
            vec![col((DataType::Int32, 4)), col((DataType::Int64, 4))],
        );
        assert!(ok.is_ok());

        let bad = Data::new(
            schema,
            vec![col((DataType::Int32, 4)), col((DataType::Int64, 5))],
        );
        assert!(bad.is_err());
    }

    #[test]
    fn dataview_column_lookup_and_type_check() {
        let schema = Schema::new(vec![
            crate::schema::FieldSchema::new("sym", DataType::UInt16),
            crate::schema::FieldSchema::new("price", DataType::Float64),
        ]);
        let sym = Column::zeroed(DataType::UInt16, 10, false);
        let price = Column::zeroed(DataType::Float64, 10, false);
        let columns = vec![sym.as_view(), price.as_view()];
        let view = DataView::new(schema, columns).unwrap();
        assert_eq!(view.length(), 10);
        assert!(view.column("price").is_some());
        assert!(view.column("missing").is_none());

        let sym2 = Column::zeroed(DataType::UInt16, 10, false);
        let wrong = Column::zeroed(DataType::Int64, 10, false);
        let wrong_type = vec![sym2.as_view(), wrong.as_view()];
        assert!(DataView::new(
            Schema::new(vec![
                crate::schema::FieldSchema::new("sym", DataType::UInt16),
                crate::schema::FieldSchema::new("price", DataType::Float64),
            ]),
            wrong_type
        )
        .is_err());
    }
}
