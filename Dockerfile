FROM python:3.11-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends \
    git

RUN python -m pip install --upgrade pip

RUN mkdir -p /app

RUN mkdir -p /app/certs

COPY ./src/constraints.txt /app/constraints.txt

COPY ./src/requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r ./requirements.txt --constraint ./constraints.txt

COPY ./src /app

RUN chmod +x docker-entrypoint.sh

EXPOSE 8080

ENTRYPOINT ./docker-entrypoint.sh
