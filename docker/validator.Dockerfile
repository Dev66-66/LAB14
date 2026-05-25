FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      build-essential \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install --no-cache-dir maturin

WORKDIR /validator
COPY . .

RUN maturin build --release
RUN pip install target/wheels/*.whl
