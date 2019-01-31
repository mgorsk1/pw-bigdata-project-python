#!/bin/sh

# create snapshot
curl -X PUT 'http://10.112.112.11:9202/_snapshot/gcp_backup/%3Cmeetup-%7Bnow%2Fd%7D%3E'

# get list of created snapshots
curl -X GET 'http:///10.112.112.11:9202/_snapshot/gcp_backup/_all?v'

# restore snapshot from repository
curl -X POST 'http:///10.112.112.11:9202/_snapshot/gcp_backup/meetup-2019.27.01/_restore'


