cmake_minimum_required(VERSION 3.15)
project(splayed_read)

# ── 说明 ────────────────────────────────────────────────────────────────────
# 最小骨架：实际的 DuckDB 扩展请以 duckdb-extension-template 为准
# （本工程 + duckdb-src 子模块 + 扩展 CMake 宏）。这里给出关键链接点。
# ─────────────────────────────────────────────────────────────────────────────

set(SPLAYED_RUST_LIB "${CMAKE_CURRENT_SOURCE_DIR}/../../target/release/splayed_duckdb.dll.lib"
    CACHE FILEPATH "splayed-duckdb Rust 层 import lib（Windows/MSVC）")

add_library(splayed_read MODULE extension.cpp)
target_compile_features(splayed_read PRIVATE cxx_std_17)
target_include_directories(splayed_read PRIVATE
  ${DUCKDB_SRC}/src/include          # duckdb.hpp / function 头
  ${DUCKDB_EXTENSION_TEMPLATE}/src/include)
target_link_libraries(splayed_read PRIVATE "${SPLAYED_RUST_LIB}")
# Windows 下把 Rust cdylib 一并拷贝到扩展目录（Rust 层 dll 需随扩展分发）。
add_custom_command(TARGET splayed_read POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_if_different
          "${CMAKE_CURRENT_SOURCE_DIR}/../../target/release/splayed_duckdb.dll"
          "$<TARGET_FILE_DIR:splayed_read>")