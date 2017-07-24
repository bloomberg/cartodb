#!/bin/bash

if [ $# -ne 3 ]; then
    echo "usage: bulk_delete_users.sh <usernames_file> <db_host> <db_port>"
    exit 3
fi

USERNAMES_FILE=$1
DB_HOST=$2
DB_PORT=$3

USERNAMES=($(cat $USERNAMES_FILE | tr "\n" " "))

for USERNAME in ${USERNAMES[@]}
do
    FORCE_DELETION_WOUT_PROMPT="true"
    bundle exec rake user:deletion:by_username["$USERNAME","$FORCE_DELETION_WOUT_PROMPT"]
    RC=$?
    if [ $RC -ne 0 ]; then
        echo "WARNING: bundle exec rake user:deletion:by_username['$USERNAME'] failed with RC=$RC"
    fi
    psql -U postgres -d carto_db_development -h $DB_HOST -p $DB_PORT -c  "delete from user_creations where username='$USERNAME';"
    psql -U postgres -d carto_db_development -h $DB_HOST -p $DB_PORT -c  "delete from user_infos where username='$USERNAME';"
done


