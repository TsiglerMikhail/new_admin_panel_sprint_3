FROM python:3.11

WORKDIR /etl

SHELL ["/bin/bash", "-c"]

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt requirements.txt

RUN apt update \
    && apt install -y --no-install-recommends gettext netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir

COPY . .

RUN chmod -R 775 run.sh

ENTRYPOINT ["./run.sh"]
