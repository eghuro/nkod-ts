FROM python:3.6-alpine
COPY requirements.txt .
RUN apk add gcc musl-dev python3-dev libffi-dev openssl-dev && pip install -r requirements.txt && rm -rf /root/.cache && apk del gcc musl-dev python3-dev libffi-dev openssl-dev
RUN apk add --update nodejs nodejs-npm

COPY . .

CMD npm start
EXPOSE 8000
