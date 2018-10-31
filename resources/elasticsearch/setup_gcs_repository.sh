#!/bin/sh

"---------- Setup gcs repository - Start ----------"

until curl -XGET 'http://localhost:9200/_cluster/health?pretty' 2>&1 | grep status | egrep "(green|yellow)"; do \
	echo "---------- Waiting for Elasticsearch to start... ----------" && sleep 1;\
	done

echo "---------- Elasticsearch is up and running ! ----------"

cd /usr/share/elasticsearch

plugin_check=$(./bin/elasticsearch-plugin list | grep repository-gcs)
keystore_check=$(./bin/elasticsearch-keystore list | grep gcs.client.default.credentials_file)

if [ -z "$plugin_check" ]
then
    echo "---------- Install repository-gcs plugin - Start  ----------"
    ./bin/elasticsearch-plugin install --batch repository-gcs
    echo "---------- Install repository-gcs plugin - Finish  ----------"
fi

if [ -z "$keystore_check" ]
then
    echo "---------- Register gcp credentials - Start ----------"
    ./bin/elasticsearch-keystore add-file gcs.client.default.credentials_file /app/key.json
    echo "---------- Register gcp credentials - Finish ----------"
fi

echo "---------- Setup gcs repository - Finish ----------"

exit