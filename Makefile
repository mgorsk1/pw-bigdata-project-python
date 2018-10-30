PROJECT_ID=pw-bigdata-project
SA_NAME=pubsub-all-meetup

setup-env:
	# project setup
	gcloud projects create ${PROJECT_ID}
	gcloud config set project ${PROJECT_ID}

	# accounts setup
	gcloud iam service-accounts create ${SA_NAME} --display-name "${SA_NAME}"
	gcloud projects add-iam-policy-binding \
		${PROJECT_ID} \
		--member serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
		--role roles/pubsub.publisher \
		--role roles/pubsub.subscriber

	# keys generation
	gcloud iam service-accounts keys create \
		D:\Media\Documents\Programming\Python\pw_bigdata_project_utils\config\keys\gcp.json \
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

run-socket-reader:
	python3 ./app/socket_reader/main.py
