#!/bin/sh

# create repository
curl -X PUT 'http://localhost:9201/_snapshot/gcp_backup/%3Cmeetup-%7Bnow%2Fd%7D%3E'

# get created snapshots
curl -X GET 'http://localhost:9201/_snapshot/gcp_backup/_all'

# restore from repository
curl -X POST 'http://localhost:9201/_snapshot/gcp_backup/meetup-2018.11.02/_restore'


