FROM python:3.6-alpine
MAINTAINER gorskimariusz13@gmail.com

LABEL "com.gorskimariusz.project"="pw-bd-project"

RUN apk update
RUN apk add alpine-sdk gcc linux-headers python3-dev musl-dev

# Set the working directory
RUN mkdir /app
WORKDIR /app

# Install app dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV PYTHONPATH "$PYTHONPATH:/app"
ENV RUNNING_IN_CONTAINER "True"

