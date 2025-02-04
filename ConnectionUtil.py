import os
import psycopg2
import yaml


def connect_database(database):
    """
    connect to a database
    :param database:
    :return: connection
    """
    with open('connection.yaml') as file:
        connection_detail = yaml.load(file, Loader=yaml.FullLoader)['connections']['connection-'+os.getenv("env")]
        connection = psycopg2.connect(user=os.getenv("postgresusername"),
                                      password=os.getenv("postgrespassword"),
                                      host=connection_detail[database]['host'],
                                      port=connection_detail[database]['port'],
                                      database=connection_detail[database]['database'])
        return connection


def connect_s3():
    """
    connect to s3 bucket
    :return: s3 connection
    """
    with open('connection.yaml') as file:
        connection_detail = yaml.load(file, Loader=yaml.FullLoader)['Connections']['Connection']
        return 's3://' + connection_detail['S3']['Bucket-Name'] + '/' + connection_detail['S3'][
            'Key-Name']