# IFRS17

This repository is a data comparison framework, which will be predominantly used in IFRS17 programme FDP testing phase.

## Technologies Used

* Language - Python
* Test library - Pytest
* Package installer - pip
* Other libraries - Pandas, numpy, boto3, psycopg2

## How to build project

1. clone the repo to your local system and navigate to project location
2. python -m venv <Project_Location>/.venv
3. set the project interpreter to the new location inside <Project_Location>/.venv
4. cd .venv\Scripts
5. activate
6. pip install -r requirements.txt

## How to run project

pytest -s -k 'UEPR' test_database.py --html=report.html --self-contained-html (-k is a keyword which will execute only tests that contains ‘UEPR’ as test name, to execute all tests, remove –k ‘UEPR’)

Report will be generated in reports folder

## Installations required

1. Python 3.7.7
2. Pycharm
3. git 2.28

## Scheduling tests using airflow

1. Connect to the linux instance 10.88.192.9
2. switch to the user "airflow" (su - airflow)
3. password <contact Naveen, Arjun, Johnson> 
4. navigate to /home/airflow/airflow/dags where we have existing dags
5. create a new py file (touch filename.py)
6. copy the contents from an existing dag (eg. insurancecontract.py) and change the command accordingly
7. newly created dag must now be available for execution which can be verified in http://10.88.192.9:8080/admin/
8. Once the dag is executed, report will be available in fdp-test-automation-reports-test bucket
9. Commands to start,stop and check status of airflow webserver and scheduler
    sudo systemctl stop airflow-scheduler.service
    sudo systemctl start airflow-scheduler.service
    sudo systemctl status airflow-scheduler.service
    sudo systemctl stop airflow-webserver.service
    sudo systemctl start airflow-webserver.service
    sudo systemctl status airflow-webserver.service