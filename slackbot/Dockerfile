FROM python:3.12-alpine

COPY . /app
WORKDIR /app
EXPOSE 5000

RUN apk add --no-cache \
    build-base \
    python3-dev \
    libffi-dev \
    openssl-dev \
    musl-dev \
    tzdata

RUN cp /usr/share/zoneinfo/Asia/Seoul /etc/localtime
RUN echo "Asia/Seoul" > /etc/timezone

RUN pip3 install -r requirements.txt

CMD ["python3","app.py"]
