FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get -o Acquire::Retries=4 -o Acquire::https::Timeout=40 update \
    && apt-get -o Acquire::Retries=4 -o Acquire::https::Timeout=40 install -y --no-install-recommends ffmpeg

COPY pyproject.toml README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/pip pip install -e .

COPY recommend-radio/backend/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt

COPY recommend-radio/backend .
COPY recommend-radio/deploy/docker/migrate.py ./docker_migrate.py

EXPOSE 5000 9090

CMD ["python", "-m", "uvicorn", "web_asgi:app", "--host", "0.0.0.0", "--port", "5000"]
