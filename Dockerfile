FROM python:3.11.0-alpine3.16

COPY .* /app
RUN pip3 install -r /app/requirements.txt
