FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data/processed /tmp/hf_cache && chmod -R 777 /app/data /tmp/hf_cache

ENV HF_HOME=/tmp/hf_cache
ENV HF_DATASET_REPO=3cdiqolbu/chicago-crime-clean

EXPOSE 8501

HEALTHCHECK --start-period=60s CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["sh", "-c", "python -m src.utils --download-hf && exec streamlit run app.py --server.port=8501 --server.address=0.0.0.0"]