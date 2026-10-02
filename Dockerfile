FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

# ffmpeg (trae ffprobe) para validar que el archivo descargado sea un video; git para instalar el repo del descargador
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && (pip install --no-cache-dir --upgrade git+https://github.com/krypton-byte/tiktok-downloader \
        || pip install --no-cache-dir --upgrade tiktok_downloader) \
    && python -c "import tiktok_downloader as td; assert hasattr(td, 'ttdownloader'), 'falta ttdownloader'"

COPY main.py .

ENV PORT=8000
EXPOSE 8000

# API pública: sin API_KEY cualquiera con el dominio puede usarla.
# 1 solo worker: el límite de descargas simultáneas vive dentro del proceso
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers --forwarded-allow-ips='*'"]
