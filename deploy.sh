#!/usr/bin/env bash

DEST="avAbs@avira4.pub.echtzeit.net:apps/couchbase-hadoop-connector/"

rsync -avz --delete src pom.xml cb-serial-importer "$DEST"
