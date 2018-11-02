FROM docker.elastic.co/elasticsearch/elasticsearch:6.4.2
MAINTAINER gorskimariusz13@gmail.com

LABEL "com.gorskimariusz.project"="pw-bd-project"

RUN yum install curl

# Set the working directory
RUN mkdir /app
WORKDIR /app

# Install app dependencies
COPY . .

RUN chmod ugo+rwx /app/*sh

CMD /app/setup_gcs_repository.sh & /usr/local/bin/docker-entrypoint.sh
