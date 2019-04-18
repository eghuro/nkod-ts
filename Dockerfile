ARG PYTHON_VERSION=3.6
FROM python:${PYTHON_VERSION}-alpine
COPY requirements.txt .
RUN apk --no-cache add g++ gcc musl-dev python3-dev libffi-dev openssl-dev libarchive-dev && pip install -r requirements.txt && apk del gcc g++ musl-dev python3-dev libffi-dev openssl-dev

COPY . .

CMD gunicorn -k gevent -w 4 -b 0.0.0.0:8000 autoapp:app
EXPOSE 8000
