FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY docker/streamlit/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY streamlit_app /app/streamlit_app

EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "streamlit_app/Home.py", "--server.port=8501", "--server.address=0.0.0.0"]
