#!/bin/bash

export AIRFLOW_HOME="$(pwd)/airflow"
echo $AIRFLOW_HOME

airflow db init

airflow users create \
--username "admin" \
--firstname "Dags" \
--lastname "Author" \
--role Admin \
--email "eid@example.com" \
--password "admin"

DAGS_FOLDER="$(pwd)/airflow/scripts"
sed -i "s|^dags_folder = .*|dags_folder = $DAGS_FOLDER|" $AIRFLOW_HOME/airflow.cfg


airflow webserver &
airflow scheduler &

echo "Airflow setup is complete. Access the webserver at http://localhost:8080"
