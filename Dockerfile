GNU nano 5.4                                           Dockerfile                                                     FROM python:3.9

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    libjpeg-dev \
    zlib1g-dev \
    libfreetype6-dev \
    cron \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY crontab /etc/cron.d/my-cron-job
RUN chmod 0644 /etc/cron.d/my-cron-job

RUN crontab /etc/cron.d/my-cron-job

RUN chmod +x /usr/src/app/Automation.py

CMD ["cron", "-f"]