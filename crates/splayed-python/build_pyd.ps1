# build_pyd.ps1 — 构建 splayed.pyd（Python 扩展，release）。
#
# 用法：
#   pwsh -File crates/splayed-python/build_pyd.ps1
#   pwsh -File crates/splayed-python/build_pyd.ps1 -Python C:\path\to\python.exe
#
# 产物：crates/splayed-python/splayed.pyd（import splayed 用）。
# 说明：crate-type 默认只含 rlib（cargo test/clippy 零 MSVC 链接告警，与 DuckDB
#   同款约定）；本脚本按需以 cdylib 链接生成扩展。--config 经变量传递（PowerShell
#   会剥掉字面字符串内的引号，变量内容原样传给 cargo）。
param(
    [string]$Python = ""
)
$ErrorActionPreference = "Stop"
if ($Python) { $env:PYO3_PYTHON = $Python }
$crate = $PSScriptRoot                                   # crates/splayed-python
$ws = Split-Path -Parent (Split-Path -Parent $crate)     # workspace 根
$cfg = "lib.crate-type=['cdylib','rlib']"
Push-Location $ws
try {
    cargo build -p splayed-python --release --features extension-module --config $cfg
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed ($LASTEXITCODE)" }
} finally {
    Pop-Location
}
$dst = Join-Path $crate "splayed.pyd"
Copy-Item (Join-Path $ws "target\release\splayed.dll") $dst -Force
Write-Host "OK -> $dst"
