// DuckDB 扩展壳：注册 read_splayed(dir) 表函数，把扫描委托给
// splayed-duckdb（Rust 层）的 C ABI，填充 DuckDB DataChunk。
//
// ⚠ 本文件是壳骨架：本仓库环境无 DuckDB SDK，无法编译验证；
//   请对照 duckdb-extension-template 与 duckdb.hpp 校对 API。
//
// 零拷贝说明：定长列经 Rust 的 data_ptr 直接 memcpy 进 DuckDB 向量缓冲
//（C API 路径稳定）；若接扩展工程后改用 C++ Vector(LogicalType, data_ptr)
// 构造，可进一步去掉这次拷贝（见 splayed-duckdb/src/ffi.rs 契约）。

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/validity_mask.hpp"

#include <cstring>
#include <string>
#include <vector>

// ---- splayed-duckdb C ABI（与 crates/splayed-duckdb/src/ffi.rs 对齐）----
extern "C" {
struct splayed_column_view {
  uint8_t kind;        // 0 定长 / 1 字典
  uint8_t type_id;     // splayed_format::DataType id | 200=SYM 字典
  const uint8_t* data; // 定长：LE 值；字典：u32 索引
  uint32_t data_len;
  const uint8_t* validity; // LSB-first，bit=1 有效；空=无 NULL
  uint32_t validity_len;
  int32_t dict_count;
};
struct splayed_batch {
  int32_t row_count;
  int32_t column_count;
  int32_t system_columns;
  const splayed_column_view* columns;
};

void* splayed_dataset_open(const char* dir, uint32_t dir_len);
void splayed_dataset_close(void* handle);
int32_t splayed_dataset_schema_count(void* handle);
int32_t splayed_dataset_schema_field(void* handle, int32_t index, const char** name_out,
                                     uint32_t* name_len_out, uint8_t* type_out, uint8_t* nullable_out);
void* splayed_scan_open(void* dataset, const char* const* cols, const uint32_t* col_lens,
                        int32_t col_count, uint32_t batch_rows, int32_t parallelism);
int32_t splayed_scan_next(void* scan, splayed_batch* out);
int32_t splayed_scan_dict_value(void* scan, int32_t column, int32_t idx,
                                const char** out_ptr, uint32_t* out_len);
void splayed_scan_close(void* scan);
const char* splayed_last_error();
}

namespace {

// bind 阶段收集的列信息。
struct BindData : public duckdb::TableFunctionData {
  std::string dir;
  int32_t column_count = 0;
  std::vector<std::string> names;
  std::vector<duckdb::LogicalType> types;
  bool failed = false;
};

struct GlobalState : public duckdb::GlobalTableFunctionState {
  void* dataset = nullptr;
  void* scan = nullptr;
  bool done = false;
};

std::unique_ptr<duckdb::FunctionData> Bind(duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
                                           std::vector<duckdb::LogicalType>&, std::vector<std::string>&) {
  auto data = std::make_unique<BindData>();
  // read_splayed(dir) — 第一个参数为目录。
  if (input.inputs.size() < 1 || input.inputs[0].IsNull()) {
    throw duckdb::InvalidInputException("read_splayed requires a directory argument");
  }
  data->dir = input.inputs[0].ToString();

  const auto dir = data->dir.c_str();
  // schema 预览：临时打开读取列结构（C ABI；失败则抛错）。
  void* ds = splayed_dataset_open(dir, static_cast<uint32_t>(data->dir.size()));
  if (!ds) {
    throw duckdb::InvalidInputException("read_splayed: open failed: %s", splayed_last_error());
  }
  data->column_count = splayed_dataset_schema_count(ds);
  data->names.reserve(data->column_count);
  data->types.reserve(data->column_count);
  for (int32_t i = 0; i < data->column_count; i++) {
    const char* nm = nullptr;
    uint32_t nm_len = 0, ty = 0, nul = 0;
    splayed_dataset_schema_field(ds, i, &nm, &nm_len, &ty, &nul);
    data->names.emplace_back(nm, nm_len);
    switch (ty) {
      case 5:  data->types.emplace_back(duckdb::LogicalType::DATE); break;
      case 6:  data->types.emplace_back(duckdb::LogicalType::TIMESTAMP); break;
      case 1:  data->types.emplace_back(duckdb::LogicalType::INTEGER); break;
      case 2:  data->types.emplace_back(duckdb::LogicalType::BIGINT); break;
      case 3:  data->types.emplace_back(duckdb::LogicalType::FLOAT); break;
      case 4:  data->types.emplace_back(duckdb::LogicalType::DOUBLE); break;
      case 200: data->types.emplace_back(duckdb::LogicalType::VARCHAR); break; // SYM 字典
      default: throw duckdb::InvalidInputException("read_splayed: unsupported type id %u", ty);
    }
  }
  splayed_dataset_close(ds);

  // 回填返回列（DuckDB bind 的 out 参数）。
  return_types = data->types;
  names = data->names;
  return data;
}

std::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobal(duckdb::ClientContext&,
                                                             duckdb::TableFunctionInitInput& input) {
  auto state = std::make_unique<GlobalState>();
  auto& bind = input.bind_data->Cast<BindData>();
  state->dataset = splayed_dataset_open(bind.dir.c_str(),
                                        static_cast<uint32_t>(bind.dir.size()));
  if (!state->dataset) {
    throw duckdb::InvalidInputException("read_splayed: open failed: %s", splayed_last_error());
  }
  return state;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitLocal(duckdb::ExecutionContext&,
                                                              duckdb::TableFunctionInitInput&) {
  return nullptr;
}

void Function(duckdb::ClientContext&, duckdb::TableFunctionInput& data, duckdb::DataChunk& output) {
  auto& bind = data.bind_data->Cast<BindData>();
  auto& g = data.global_state->Cast<GlobalState>();
  if (bind.failed || g.done) {
    return; // 空 chunk 结束
  }
  if (!g.scan) {
    // 扫描请求列：schema 全列（time, sym, fields...）。
    std::vector<std::string> field_names;
    for (size_t i = 2; i < bind.names.size(); i++) field_names.push_back(bind.names[i]);
    std::vector<const char*> cnames;
    std::vector<uint32_t> clens;
    for (auto& n : field_names) { cnames.push_back(n.c_str()); clens.push_back((uint32_t)n.size()); }
    g.scan = splayed_scan_open(g.dataset, cnames.data(), clens.data(), (int32_t)cnames.size(),
                               static_cast<uint32_t>(output.size()), 1);
    if (!g.scan) {
      bind.failed = true;
      throw duckdb::InvalidInputException("read_splayed: scan open failed: %s", splayed_last_error());
    }
  }

  splayed_batch batch{};
  int rc = splayed_scan_next(g.scan, &batch);
  if (rc < 0) {
    throw duckdb::InvalidInputException("read_splayed: scan failed: %s", splayed_last_error());
  }
  if (rc == 0) { g.done = true; return; }

  output.SetCardinality(static_cast<duckdb::idx_t>(batch.row_count));
  for (int32_t c = 0; c < batch.column_count && c < (int32_t)output.ColumnCount(); c++) {
    const auto& view = batch.columns[c];
    auto& vec = output.data[c];
    if (view.kind == 1) { // 字典字符串（SYM）
      auto& mask = duckdb::FlatVector::Validity(vec);
      for (int32_t r = 0; r < batch.row_count; r++) {
        uint32_t idx = 0;
        std::memcpy(&idx, view.data + r * 4, 4);
        const char* s = nullptr; uint32_t slen = 0;
        splayed_scan_dict_value(g.scan, c, (int32_t)idx, &s, &slen);
        duckdb::FlatVector::GetData<duckdb::string_t>(vec)[r] =
            duckdb::StringVector::AddStringOrBlob(vec, duckdb::string_t(s, slen));
      }
      (void)mask;
    } else if (view.data && view.data_len > 0) {
      std::memcpy(duckdb::FlatVector::GetData<char>(vec), view.data, view.data_len);
      // 非定长（VARCHAR 的字典均已走 kind==1），此处仅定长列。
      if (view.validity) {
        auto& mask = duckdb::FlatVector::Validity(vec);
        for (int32_t r = 0; r < batch.row_count; r++) {
          bool valid = (view.validity[r / 8] >> (r % 8)) & 1;
          if (!valid) mask.SetInvalid(r);
        }
      }
    }
  }
  output.SetCardinality(static_cast<duckdb::idx_t>(batch.row_count));
}

void Finalize(duckdb::ClientContext&, duckdb::TableFunctionInput& data) {
  auto& g = data.global_state->Cast<GlobalState>();
  if (g.scan) splayed_scan_close(g.scan);
  if (g.dataset) splayed_dataset_close(g.dataset);
}

} // namespace

// 扩展入口（DuckDBExtension 模板）。
void Load(DuckDB &db) {
  duckdb::TableFunction tf("read_splayed", {duckdb::LogicalType::VARCHAR},
                           Function, Bind, InitGlobal, InitLocal);
  tf.name = "read_splayed";
  tf.to_string = nullptr;
  duckdb::ExtensionUtil::RegisterFunction(db, tf);
}

std::string Name() { return "splayed_read"; }
std::string Version() { return "0.1.0"; }