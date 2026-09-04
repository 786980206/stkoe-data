# 类型与 NULL 编码

## DataType（ID 0–13）

| ID | 类型 | 单值大小 | NULL 表示 |
| --: | --- | ---: | --- |
| 0 | `BOOL` | 1 B | 保留值（如 `0x02`） |
| 1 | `INT32` | 4 B | `INT32_MIN` |
| 2 | `INT64` | 8 B | `INT64_MIN` |
| 3 | `FLOAT32` | 4 B | canonical NaN |
| 4 | `FLOAT64` | 8 B | canonical NaN |
| 5 | `DATE32` | 4 B | `INT32_MIN` |
| 6 | `TIMESTAMP_US` | 8 B | `INT64_MIN` |
| 7 | `INT8` | 1 B | `INT8_MIN`（`0x80`） |
| 8 | `INT16` | 2 B | `INT16_MIN`（`0x8000`） |
| 9 | `UINT8` | 1 B | 全 1（`0xFF`） |
| 10 | `UINT16` | 2 B | 全 1（`0xFFFF`） |
| 11 | `UINT32` | 4 B | 全 1（`0xFFFFFFFF`） |
| 12 | `UINT64` | 8 B | 全 1（`0xFFFF…`） |
| 13 | `DATE64` | 8 B | `INT64_MIN` |

> 7–13 为**增量扩展**：只新增 ID 取值，不改动 ID 0–6 的语义，旧文件零迁移。无符号类型的 NULL 借用全 1（MAX）位型——比较仍是 bit pattern。

## NULL bit pattern

不使用 validity bitmap，用类型内置特殊值。判断 NULL 时**比较 bit pattern**，而非 `value == NaN`。

| 类型 | NULL bit pattern |
| --- | --- |
| INT32 | `0x80000000` |
| INT64 | `0x8000000000000000` |
| FLOAT32 | `0x7FC00000` |
| FLOAT64 | `0x7FF8000000000000` |
| DATE32 | `0x80000000` |
| TIMESTAMP_US | `0x8000000000000000` |
| BOOL | 保留值，例如 `0x02` |
| INT8 / INT16 | `0x80` / `0x8000` |
| UINT8 / UINT16 / UINT32 / UINT64 | 全 1（`0xFF…`） |
| DATE64 | `0x8000000000000000` |

FLOAT 语义：

```text
canonical NaN = NULL
其他 NaN     = 普通 NaN
```

## Arrow / Parquet 映射

| TIME 类型 | Arrow | Parquet |
| --- | --- | --- |
| `DATE32` | `date32` | DATE |
| `TIMESTAMP_US` | `timestamp[us]` | TIMESTAMP_MICROS |

META Header 的 `time_type` 直接声明其一，不再拆 `time_type` / `time_unit`。

## NULL 语义保留（Splayed → Arrow）

```text
canonical NaN    ->  Arrow validity bitmap = 0（NULL）
其他普通 NaN     ->  Arrow validity = 1, value = NaN
```

## 实现要点

- `DataType::size_of() / null_bytes() / from_id(id) / as_str()`。
- `RawValue::read_le / write_le` 与全类型构造/访问器（`from_*` / `as_*`）。
- `fill_null(buf, ty)` 按类型填 NULL 哨兵。
- 测试中非 canonical NaN 用 `f64::from_bits(0x7FF8000000000001)`（`f64::NAN` 可能产生相同位型）。
