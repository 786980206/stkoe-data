# AGENTS.md

Guidance for AI agents working on the Splayed V1 codebase.

## Project Summary

Splayed V1 is a Rust columnar storage engine for `SYM × TIME × FIELD` financial time-series data. The design spec lives in `plan.md` — it is the **authoritative source of truth** for all format and API decisions. Before changing any format, header, or function signature, read the relevant section of `plan.md`.

## Workspace Layout

```
Cargo.toml                 # workspace root
crates/
  splayed-format/           # NO deps — binary format definitions (types, headers, meta, field)
  splayed-codec/           # depends on format — encoding + compression (PLAIN, ZSTD, compact_field)
  splayed-core/            # depends on format + codec — reader (mmap), field_writer, dataset, scanner
  splayed-arrow/           # depends on core — Arrow conversion, create_meta/create_table/update_table
```

**Dependency invariant:** `splayed-core` must NEVER depend on Arrow. Arrow-related functions live in `splayed-arrow`.

## Build & Test

```bash
cargo build        # must succeed with zero warnings
cargo test         # all tests must pass
```

- **Zero warnings policy.** If a build produces warnings, fix them before moving on.
- **Tests are integration tests** in `crates/splayed-arrow/tests/integration.rs`. They create temp dirs, write datasets, and read them back.
- On Windows, mmap'd files cannot be written to. If you open a `FieldReader` and then try `compact_field` or `update_field` on the same path, close the reader first (drop it) — otherwise you'll get OS error 1224.

## Format Constants (do not change without updating plan.md)

| Constant | Value |
|---|---|
| META magic | `SPLAYMTA` (u64 LE) |
| FIELD magic | `SPLAYFLD` (u64 LE) |
| Header size | 64 bytes (both META and FIELD) |
| Data offset | 64 (immediately after header) |
| SYM INDEX record | 12 bytes: `time_start(4) + time_count(4) + row_start(4)` |
| META file name | `.meta` |
| Format version | 1 |

## Key Design Decisions

1. **Full pre-declaration:** `time_count` = row capacity. SYM INDEX has no separate `row_capacity` field. `row_start = sum(prior time_counts)`.
2. **NULL via sentinel bit pattern:** No validity bitmap. Canonical NaN = NULL for floats. `INT32_MIN`/`INT64_MIN` = NULL for ints. Compare **bit patterns**, not values.
3. **In-place update:** `create_field` pre-allocates all-NULL data. `update_field` overwrites in place. No file extension. `compact_field` is the only operation that rewrites the file body.
4. **Compressed = read-only:** After `compact_field`, `update_field` is rejected. `compression != NONE` means read-only.
5. **Generation:** u64, strictly monotonic. Updated on `update_field` success. Reader rejects FIELDs with mismatched generation.
6. **Atomic META commit:** Write `.meta.new` → fsync → atomic rename to `.meta`.

## How to Add a New Feature

1. Find the relevant section in `plan.md`.
2. Implement in the correct crate (format → codec → core → arrow).
3. Add unit tests in the module.
4. Add integration tests in `crates/splayed-arrow/tests/integration.rs` (or a new test file).
5. Run `cargo build && cargo test` — zero warnings, all pass.
6. Update `plan.md` phase status and `README.md` if needed.

## How to Add a New DataType

1. Add variant to `DataType` enum in `splayed-format/src/types.rs`.
2. Implement `size_of`, `null_bytes`, `from_id`, `as_str`.
3. Add `RawValue::from_*` and `as_*` methods.
4. Update `arrow_conv.rs` with the Arrow mapping.
5. Add tests for NULL detection and roundtrip.

## Testing Patterns

- Use `std::env::temp_dir()` with `std::process::id()` for unique temp dirs.
- Always `fs::remove_dir_all` at end of test (or use `let _ = fs::remove_dir_all` at start).
- For roundtrip tests: create via `create_table`, read via `FieldReader`, assert values.
- For error tests: use `matches!(result, Err(ExpectedError::Variant))`.

## What NOT to Do

- Do not add Arrow dependency to `splayed-core`.
- Do not change header layouts without updating `plan.md` §5.1/§5.2.
- Do not use `bytemuck::from_bytes` on `Vec<u8>`-backed slices — use `bytemuck::pod_read_unaligned` (alignment issue).
- Do not assume `f64::NAN` is different from canonical NaN — it may produce the same bits. Use `f64::from_bits(0x7FF8000000000001)` for a non-canonical NaN in tests.
- Do not keep a `FieldReader` open while writing to the same file on Windows.

## Current Phase Status

- Phase 1–3: **Complete** (Format, Reader, Writer function interfaces).
- Phase 4: **In progress** (Scanner, ColumnView).
- Phase 6: Partial (ZSTD done, LZ4/DELTA/RLE planned).
- Phase 7: In progress (Arrow conversion).
- Phase 8–9: Planned (DataFusion, DuckDB).
