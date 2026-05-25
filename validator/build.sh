#!/bin/bash
# Скрипт сборки Rust-валидатора через maturin
set -e
pip install maturin
maturin develop --release
