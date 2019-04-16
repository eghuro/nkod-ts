FROM python:3.6-alpine
COPY requirements.txt .
RUN apk --no-cache add gcc musl-dev python3-dev libffi-dev openssl-dev && pip install -r requirements.txt && apk del gcc musl-dev python3-dev libffi-dev openssl-dev

COPY . .

CMD gunicorn -k gevent -w 4 -b 0.0.0.0:8000 autoapp:app
EXPOSE 8000
