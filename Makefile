PROJECT_NAME?=pw-bd-project
PROJECT_SCOPE?=meetup

SA_NAME=pubsub-all-meetup

ELASTICSEARCH_URL=localhost
ELASTICSEARCH_PORT=9202

SSH_KEY='/home/mgorski/Dokumenty/keys/ovh/id_rsa'

MASTER_HOST='10.112.112.11'
MASTER_USER='mgorski'
MASTER_DIR='/home/mgorski/pw-bd-project-python/'

project-setup:
	# project setup
	gcloud projects create ${PROJECT_NAME}
	gcloud config set project ${PROJECT_NAME}

	read -n 1 -s -r -p "Enable billing on this project through Google Console and press any key to continue..."

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

	rm -f ./config/keys/${PROJECT_NAME}/*
	mkdir -p ./config/keys/${PROJECT_NAME}/

	# keys generation
	gcloud iam service-accounts keys create \
		./config/keys/${PROJECT_NAME}/key.json \
		--iam-account ${SA_NAME}@${PROJECT_NAME}.iam.gserviceaccount.com

pubsub-setup:
	sleep 60

	gcloud config set project ${PROJECT_NAME}

	# pubsub setup
	gcloud pubsub topics create ${PROJECT_SCOPE}-rawdata
	gcloud pubsub topics create ${PROJECT_SCOPE}-notify

	gcloud pubsub subscriptions create ${PROJECT_SCOPE}-rawdata-subscription-elastic --topic ${PROJECT_SCOPE}-rawdata
	gcloud pubsub subscriptions create ${PROJECT_SCOPE}-rawdata-subscription-streaming --topic ${PROJECT_SCOPE}-rawdata

function-register:
	gcloud config set project ${PROJECT_NAME}

	gcloud beta functions deploy \
		pushover_notify \
		--env-vars-file ./app/gcp_functions/pushover_notify/.env.yml \
		--source ./app/gcp_functions/pushover_notify \
		--runtime python37 \
		--trigger-topic ${PROJECT_SCOPE}-notify

backup-storage-create:
	gsutil mb -c regional -l us-west1 -p ${PROJECT_NAME} gs://${PROJECT_NAME}-${PROJECT_SCOPE}-elasticsearch-backup

setup-gcp-env: project-setup accounts-setup keys-generate pubsub-setup function-register backup-storage-create

setup-elk-env:
	docker-compose build
	docker-compose up -d elastic
	docker-compose up -d kibana

	until curl -XGET 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_cluster/health?pretty' 2>&1 | grep status | egrep "(green|yellow)"; do \
		echo "---------- Waiting for Elasticsearch to start... ----------" && sleep 1;\
	done

	docker exec -it pw-bd-project-python_elastic_1 ./bin/elasticsearch-keystore add-file gcs.client.default.credentials_file /app/key.json

    # change elastic config
	curl -X POST 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_nodes/reload_secure_settings'

	curl -X PUT 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_snapshot/gcp_backup' \
		-H 'Content-Type: application/json' \
		-d @./resources/elasticsearch/gcp_backup_snapshot_repository.json

	curl -XPUT 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_cluster/settings' \
	 -H 'Content-Type: application/json' \
	 -d '{"persistent" : {"cluster.routing.allocation.disk.threshold_enabled": false}}'

	docker-compose up -d index_rawdata acquire_rawdata spark_streaming

teardown-elk-env:
	docker rm -f $$(docker ps -aq --filter "label=com.gorskimariusz.project=pw-bd-project")

	docker volume rm pw-bd-project-python_esdata

compile-pyrobuf-lib:
	pip3 uninstall -y pyrobuf-generated
	mkdir -p tmp/pyrobuf

	cd ./tmp/pyrobuf; \
	python3 -m pyrobuf \
		--install ../../resources/protobuf/${PROJECT_SCOPE}_rawdata.proto \
		--package message_proto; \
	cd ../../; \
	rm -frd tmp/pyrobuf

run-socket-reader:
	python3 ./app/socket_reader/main.py

sync-repo:
	rsync -avh --chmod=0755 --progress --cvs-exclude --include '.env' --exclude '__pycache__' --exclude 'tmp' --exclude '.docker' --exclude 'venv' --exclude 'tests' --exclude '.vscode' --exclude='*.pyc' --exclude 'log' --exclude 'jar' --exclude '.idea' -e "ssh -i $(SSH_KEY)" ./* ${MASTER_USER}@$(MASTER_HOST):$(MASTER_DIR)/${PROJECT_FOLDER}

