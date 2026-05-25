FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      curl \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Кешируем зависимости отдельным слоем.
COPY analyzer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Собираем и устанавливаем Rust-валидатор через maturin.
COPY validator/ /validator
RUN pip install maturin \
    && cd /validator && maturin build --release \
    && pip install /validator/target/wheels/*.whl

COPY analyzer/ .

# Непривилегированный пользователь для запуска процесса.
RUN addgroup --system appuser \
    && adduser --system --ingroup appuser appuser \
    && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "-m", "usecases.pipeline"]
