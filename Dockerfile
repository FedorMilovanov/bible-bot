FROM python:3.11.15-slim-trixie@sha256:baf89808ec37adeaab83cec287adb4a2afa4a11c1d51e961c7ec737877e61af6

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PORT=8080 APP_ENV=production HOME=/tmp

RUN apt-get update \
    && apt-get install -y --no-install-recommends fonts-dejavu-core \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --no-log-init --no-create-home --shell /usr/sbin/nologin app \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --no-cache-dir --upgrade \
        pip==26.2.1 setuptools==83.0.0 wheel==0.47.0 \
    && python -m pip install --no-cache-dir -r requirements.txt \
    && python -m pip check

COPY --chown=10001:10001 . .
USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.getenv('PORT', '8080') + '/live', timeout=2).read()" || exit 1

CMD ["python", "bot.py"]
