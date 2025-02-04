import numpy as np
import pandas as pd
import psycopg2
import s3fs
import logging
from database_comparator.ConnectionUtil import connect_database, connect_s3

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)
log = logging.getLogger()

fs = s3fs.S3FileSystem(anon=False)


def execute_query(database, query):
    """
    execute a query against db or read a csv from s3 bucket
    :param database:
    :param query:
    :return: dataframe
    """
    try:
        expected_val = pd.read_sql_query(query, connect_database(database))
        return expected_val
    except(Exception, psycopg2.Error) as exception:
        log.error("Exception :" + exception)
        assert False


def compare_results(expected_result, actual_result, source_dataset, target_dataset):
    """
    compare two dataframes based on primary key and compare columns
    :param expected_result:
    :param actual_result:
    :param source_dataset:
    :param target_dataset:
    :return: matched_records - dataframe containing records from both dataframes
             unmatched_records - dataframe containing only differences between both dataframes
             missing_records - dataframe containing records that are missing in the actual result
    """
    global mismatch_records
    comparison_df = pd.merge(expected_result,
                             actual_result,
                             how='outer',
                             left_on=source_dataset["dataset"]['primary-key'].split(","),
                             right_on=target_dataset["dataset"]['primary-key'].split(","),
                             indicator=True)
    records = comparison_df[comparison_df['_merge'] == 'both']
    missing_records = comparison_df[comparison_df['_merge'] != 'both']
    source_compare_fields = source_dataset["dataset"]['compare-fields'].split(",")
    target_compare_fields = target_dataset["dataset"]['compare-fields'].split(",")
    for source_fields, target_fields in zip(source_compare_fields, target_compare_fields):
        source_compare_field, target_compare_field = derive_compare_fields(source_fields,
                                                                           target_fields)
        records['compare: ' + source_fields+" vs "+target_fields] = np.where(
            records[source_compare_field.lower()] == records[target_compare_field.lower()],
            'True',
            'False')
    mismatch_records = records[records.astype(str).apply(lambda r: r.str.contains('False').any(), axis=1)]
    matched_records = records[records.astype(str).apply(lambda r: not r.str.contains('False').any(), axis=1)]
    return matched_records, missing_records, mismatch_records


def derive_compare_fields(source_compare_field, target_compare_field):
    """
    if both source and target compare fields are same, append a character to both to make them distinct
    :param source_compare_field:
    :param target_compare_field:
    :return: comparison fields
    """
    if source_compare_field == target_compare_field:
        return source_compare_field + "_x", target_compare_field + "_y"
    else:
        return source_compare_field, target_compare_field
