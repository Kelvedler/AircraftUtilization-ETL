FROM python:3.11-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends \
    supervisor \
    git \
    logrotate \
    cron

RUN python -m pip install --upgrade pip

RUN mkdir -p /app/certs /app/airflow

COPY ./constraints.txt /app/constraints.txt
COPY ./requirements.txt /app/requirements.txt
COPY ./airflow_webserver_config.py /app/airflow/webserver_config.py
COPY ./logrotate /etc/logrotate.d/airflow
COPY ./rm_old_logs /etc/cron.d/airflow

WORKDIR /app

RUN pip install -r ./requirements.txt --constraint ./constraints.txt

COPY ./src /app

RUN chmod +x docker-entrypoint.sh /etc/cron.d/airflow

EXPOSE 8000

ENTRYPOINT ./docker-entrypoint.sh
