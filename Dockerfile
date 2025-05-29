FROM apache/airflow:2.7.3

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-dev g++ && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    spacy==3.7.4 \
    pandas==2.0.3 \
    psycopg2-binary==2.9.7 \
    requests==2.31.0 && \
    python -m spacy download ru_core_news_sm

USER airflow