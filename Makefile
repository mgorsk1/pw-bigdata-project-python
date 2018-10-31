PROJECT_ID=pw-bigdata-project
SA_NAME=pubsub-all-meetup

ELASTICSEARCH_URL=localhost
ELASTICSEARCH_PORT=9201

project-setup:
	# project setup
	gcloud projects create ${PROJECT_NAME}
	gcloud config set project ${PROJECT_NAME}

accounts-setup:
	gcloud config set project ${PROJECT_NAME}

	# accounts setup
	gcloud iam service-accounts create ${SA_NAME} --display-name "${SA_NAME}"
	gcloud projects add-iam-policy-binding \
		${PROJECT_ID} \
		--member serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
		--role roles/pubsub.publisher \
		--role roles/pubsub.subscriber

	# keys generation
	gcloud iam service-accounts keys create \
		./config/keys/gcp.json \
		--iam-account ${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com

	# pubsub setup
	gcloud pubsub topics create meetup-rawdata
	gcloud pubsub topics create meetup-notify

register-function:
	gcloud config set project ${PROJECT_ID}

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

	docker restart $$(docker ps -aq --filter "label=com.gorskimariusz.project=${PROJECT_NAME}")

	until curl -XGET 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_cluster/health?pretty' 2>&1 | grep status | egrep "(green|yellow)"; do \
		echo "---------- Waiting for Elasticsearch to start... ----------" && sleep 1;\
	done

	curl -X PUT 'http://${ELASTICSEARCH_URL}:${ELASTICSEARCH_PORT}/_snapshot/gcp_backup' \
		-H 'Content-Type: application/json' \
		-d @./resources/elasticsearch/gcp_backup_snapshot_repository.json

run-socket-reader:
	python3 ./app/socket_reader/main.py
