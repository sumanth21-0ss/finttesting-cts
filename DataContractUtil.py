import os

import pandas as pd
from pandas.io.sql import DatabaseError
import logging

from database_comparator.ConnectionUtil import connect_database

number_of_rows_to_fetch = "fetch first 100 rows only"
schema = "fdp."
select_count = "select count(*) from "
where_string = " where "
select = "select "
invalid_message = " is invalid"
log = logging.getLogger()
batchkey_is = "batchkey='"
AND = "' and "
null_and = " is null and "


def validate_conditional_mandatory(landing_table, batchkey, columns):
    """
    validate if a column is not empty based on another column.
    :param df: dataframe created from the csv file
    :param columns: column name, including dependant column
    :return: dataframe with a column of True/False based on validation
    """
    validations = []
    connection = connect_database(os.getenv("database"))
    for column in columns:
        column_name = column['column']['column_name']
        count_query = select_count + schema + landing_table + where_string + batchkey_is + batchkey + \
                      AND + column_name + " is null and " + \
                      column['column']['conditions'][0]
        log.debug("cond mandatory count query : " + count_query)
        try:
            count = str(pd.read_sql_query(count_query, connection)['count'][0])
            if count != '0':
                query = select + column_name + " as " + column_name + "_cond_mand_fail,* from " + schema + landing_table + where_string + batchkey_is + batchkey + \
                        AND + column_name + null_and + \
                        column['column']['conditions'][0] + " " + number_of_rows_to_fetch
                df1 = pd.read_sql_query(query, connection)
                errors_dict = {'count': count, 'dataframe': df1}
                validations.append(errors_dict)
        except DatabaseError as e:
            error = column_name + str(e)
            exc_dict = {column_name + '_cond_mand_error': [error]}
            exc_df1 = pd.DataFrame(data=exc_dict)
            exc = {'count': 'error', 'dataframe': exc_df1}
            validations.append(exc)
    return validations


def validate_mandatory(landing_table, batchkey, columns):
    """
    validate whether the mandatory columns supplied are not empty
    :param landing_table: landing table name
    :param columns: column names
    :return: list of errors found
    """
    validations = mandatory_validation(landing_table, batchkey, columns, batchkey_is)
    return validations


def validate_mandatory_redshift(landing_table, batchkey, columns):
    """
    validate whether the mandatory columns supplied are not empty
    :param landing_table: landing table name
    :param columns: column names
    :return: list of errors found
    """
    psicle_column = "psiclebatchkey='"
    validation = mandatory_validation(landing_table, batchkey, columns, psicle_column)
    return validation



def mandatory_validation(landing_table, batchkey, columns, batchkey_column_name):
    """
    validate whether the mandatory columns supplied are not empty
    :param landing_table: landing table name
    :param columns: column names
    :return: list of errors found
    """
    validations = []
    connection = connect_database(os.getenv("database"))
    for column in columns:
        count_query = select_count + schema + landing_table + where_string + batchkey_column_name + batchkey + \
                      AND + \
                      column + " is null"
        log.debug("mandatory column count query : " + count_query)
        try:
            count = str(pd.read_sql_query(count_query, connection)['count'][0])
            if count != '0':
                query = select + column + " as " + column + "_null,* from " + schema + landing_table + where_string + \
                        batchkey_column_name + batchkey + AND + column + " is null " + number_of_rows_to_fetch
                df1 = pd.read_sql_query(query, connection)
                errors_dict = {'count': count, 'dataframe': df1}
                validations.append(errors_dict)
        except DatabaseError as e:
            error = column + str(e)
            exc_dict = {column + '_null': [error]}
            exc_df1 = pd.DataFrame(data=exc_dict)
            exc = {'count': 'error', 'dataframe': exc_df1}
            validations.append(exc)
    return validations



def validate_mandatory_reference(landing_table, batchkey, columns, batchkey_column_name):
    """
    validate that values in a column are within specified range
    :param landing_table: landing table name
    :param columns: column names
    :return: list of errors found
    """
    connection = connect_database(os.getenv("database"))
    validations = []
    for column in columns:
        column_name = column.split(",")[0]
        reference_column_name = column.split(",")[1]
        reference_table_name = column.split(",")[2]
        count_query = select_count + schema + landing_table + " A" + where_string + batchkey_column_name + batchkey + \
                      AND + "not exists" \
                            "(select " + reference_column_name + " from " + schema + reference_table_name + " B " \
                                                                                                            "where A." + column_name + "=B." + reference_column_name + ")"
        log.debug("mandatory ref count query : " + count_query)
        try:
            count = str(pd.read_sql_query(count_query, connection)['count'][0])
            if count != '0':
                query = select + column_name + " as " + column_name + "_ref_fail,* from " + schema + landing_table + " A" + \
                        where_string + batchkey_column_name + batchkey + \
                        AND + "not exists" \
                              "(select " + reference_column_name + " from " + schema + reference_table_name + " B " \
                               "where A." + column_name + "=B." + reference_column_name + ") " + number_of_rows_to_fetch
                log.debug("mandatory ref query: " + query)
                df1 = pd.read_sql_query(query, connection)
                errors_dict = {'count': count, 'dataframe': df1}
                validations.append(errors_dict)
        except DatabaseError as e:
            error = column_name + str(e)
            exc_dict = {column_name + '_ref_fail': [error]}
            exc_df1 = pd.DataFrame(data=exc_dict)
            exc = {'count': 'error', 'dataframe': exc_df1}
            validations.append(exc)
    return validations


def validate_mandatory_reference_redshift(landing_table, batchkey, columns):
    """
    validate that values in a column are within specified range
    :param landing_table: landing table name
    :param columns: column names
    :return: list of errors found
    """
    pscicle_column_name = "psiclebatchkey='"
    validations_red = validate_mandatory_reference(landing_table, batchkey, columns, pscicle_column_name)
    return validations_red


def validate_mandatory_ref(landing_table, batchkey, columns):
    """
    validate whether the mandatory columns supplied are not empty
    :param landing_table: landing table name
    :param columns: column names
    :return: list of errors found
    """
    validations = validate_mandatory_reference(landing_table, batchkey, columns, batchkey_is)
    return validations
