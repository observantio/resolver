FROM python:3.11.11-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

COPY requirements.txt ./

RUN python -m pip install --upgrade pip==24.3.1 && \
    pip wheel --wheel-dir /wheels -r requirements.txt


FROM python:3.11.11-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY --from=builder /wheels /wheels

RUN python -m pip install --upgrade pip==24.3.1 && \
    pip install --no-index --find-links=/wheels /wheels/* && \
    rm -rf /wheels && \
    groupadd --system --gid 10001 app && \
    useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin app

COPY . /app

RUN chown -R 10001:10001 /app

USER 10001:10001

EXPOSE 4322

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:4322/api/v1/ready', timeout=4).read()"

CMD ["python", "-u", "main.py"]
