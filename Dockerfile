FROM python:3.6-alpine
MAINTAINER gorskimariusz13@gmail.com

LABEL "com.gorskimariusz.project"="pw.bigdata.project.final"

# Set the working directory
RUN mkdir /app
WORKDIR /app

# Install app dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

