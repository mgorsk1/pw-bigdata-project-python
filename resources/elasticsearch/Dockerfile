FROM docker.elastic.co/elasticsearch/elasticsearch:6.5.0
MAINTAINER gorskimariusz13@gmail.com

LABEL "com.gorskimariusz.project"="pw-bd-project"

RUN yum install curl

# Set the working directory
RUN mkdir /app
WORKDIR /app

# Install app dependencies
COPY . .

RUN chmod ugo+rwx /app/*sh

WORKDIR /usr/share/elasticsearch

RUN ./bin/elasticsearch-plugin install --silent --batch repository-gcs

CMD /usr/local/bin/docker-entrypoint.sh
