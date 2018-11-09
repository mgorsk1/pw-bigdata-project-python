PROJECT_NAME=pw-bd-project
SA_NAME=pubsub-all-meetup

ELASTICSEARCH_URL=localhost
ELASTICSEARCH_PORT=9201

PROTOBUF_SCHEMA?=meetup_rawdata

project-setup:
	# project setup
	gcloud projects create ${PROJECT_NAME}
	gcloud config set project ${PROJECT_NAME}

accounts-setup:
	gcloud config set project ${PROJECT_NAME}

	# accounts setup
	gcloud iam service-accounts create ${SA_NAME} --display-name "${SA_NAME}"

	gcloud projects add-iam-policy-binding \
		${PROJECT_NAME} \
		--member serviceAccount:${SA_NAME}@${PROJECT_NAME}.iam.gserviceaccount.com \
		--role roles/pubsub.publisher

	gcloud projects add-iam-policy-binding \
		${PROJECT_NAME} \
		--member serviceAccount:${SA_NAME}@${PROJECT_NAME}.iam.gserviceaccount.com \
		--role roles/pubsub.subscriber

keys-generate:
	gcloud config set project ${PROJECT_NAME}

	rm -f ./config/keys/gcp/key.json

	# keys generation
	gcloud iam service-accounts keys create \
		./config/keys/gcp/key.json \
		--iam-account ${SA_NAME}@${PROJECT_NAME}.iam.gserviceaccount.com

pubsub-setup:
	gcloud config set project ${PROJECT_NAME}

	# pubsub setup
	gcloud pubsub topics create meetup-rawdata
	gcloud pubsub topics create meetup-notify

	gcloud pubsub subscriptions create meetup-rawdata-subscription-elastic --topic meetup-rawdata
	gcloud pubsub subscriptions create meetup-rawdata-subscription-streaming --topic meetup-rawdata

function-register:
	gcloud config set project ${PROJECT_NAME}

	gcloud beta functions deploy \
		pushover_notify \
		--env-vars-file ./app/gcp_functions/pushover_notify/.env.yml \
		--source ./app/gcp_functions/pushover_notify \
		--runtime python37 \
		--trigger-topic meetup-notify

setup-gcp-env: project-setup accounts-setup keys-generate pubsub-setup function-register

setup-elk-env:
	docker-compose up -d elastic
	docker-compose up -d kibana

	until curl -XGET 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_cluster/health?pretty' 2>&1 | grep status | egrep "(green|yellow)"; do \
		echo "---------- Waiting for Elasticsearch to start... ----------" && sleep 1;\
	done

	docker exec -it pw-bigdata-project-python-utils_elastic_1 ./bin/elasticsearch-keystore add-file gcs.client.default.credentials_file /app/key.json

	curl -X POST 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_nodes/reload_secure_settings'

	curl -X PUT 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_snapshot/gcp_backup' \
		-H 'Content-Type: application/json' \
		-d @./resources/elasticsearch/gcp_backup_snapshot_repository.json

	docker-compose up -d index_rawdata

compile-pyrobuf-lib:
	pip3 uninstall -y pyrobuf-generated
	mkdir -p tmp/pyrobuf

	cd ./tmp/pyrobuf; \
	python3 -m pyrobuf \
		--install ../../resources/protobuf/${PROTOBUF_SCHEMA}.proto \
		--package message_proto; \
	cd ../../; \
	rm -frd tmp/pyrobuf

run-socket-reader:
	python3 ./app/socket_reader/main.py
