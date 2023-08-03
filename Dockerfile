ARG build_for=linux/amd64
FROM --platform=$build_for python:3.11.2-slim-bullseye as base

LABEL maintainer="Alex Robinson"

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

WORKDIR /usr/app/dbt
VOLUME /usr/app

FROM base as dbt
WORKDIR /usr/app
COPY requirements.txt requirements.txt
RUN python -m pip install -r requirements.txt --no-cache-dir

FROM dbt
WORKDIR /usr/app
COPY . .
RUN dbt deps
RUN mkdir -p /root/.dbt
COPY profiles.yml /root/.dbt/profiles.yml
CMD ["dbt", "build"]
