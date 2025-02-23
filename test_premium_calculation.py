import collections
import utils.PremiumCalculation
import pandas as pd
import os
from database_comparator.ConnectionUtil import connect_database


batchkeys = os.getenv('batchkeys')


def surrogate_key_startquery(batchkey):
    """
    query to find the first surrogate key for the batchkey set(20000)
    :param batchkey
    :return: query as String
    """
    return "SELECT t.grosswrittenpremiumtransactionsurrogatekey FROM ( SELECT grosswrittenpremiumtransactionsurrogatekey, ROW_NUMBER() "\
            "OVER (ORDER BY grosswrittenpremiumtransactionsurrogatekey) AS rownum FROM FDP.vl_grosswrittenpremiumtransactions where batchkey='"+batchkey+"') AS t " \
            "WHERE t.rownum % 20000 = 1"


def surrogate_key_endquery(batchkey):
    """
    query to find the last surrogate key for the batchkey set(20000)
    :param batchkey
    :return: query as String
    """
    return "SELECT t.grosswrittenpremiumtransactionsurrogatekey FROM ( SELECT grosswrittenpremiumtransactionsurrogatekey, ROW_NUMBER() "\
            "OVER (ORDER BY grosswrittenpremiumtransactionsurrogatekey) AS rownum FROM FDP.vl_grosswrittenpremiumtransactions where batchkey='"+batchkey+"') AS t " \
            "WHERE t.rownum % 20000 = 0 or t.rownum = (select count(*) from FDP.vl_grosswrittenpremiumtransactions where batchkey='"+batchkey+"')"


def validation_df_query(begin, last, batchkey):
    """
    query to fetch the records within the surrogatekey range for the batchkey from validation table
    :param begin: first surrogatekey
    :param last: last surrogatekey
    :param batchkey: batchkey
    :return: query as String
    """
    return "select C.systemidentifier,C.insurancecontractidentifier,C.productidentifier," \
                      "c.grosswrittenpremiumtransactionidentifier,c.grosswrittenpremiumtransactionsurrogatekey," \
                      "c.insuranceproductsurrogatekey,c.transactiondate,c.onriskdate,c.offriskdate," \
                      "c.functionalcurrencycodeidentifier,c.transactionalcurrencycodeidentifier," \
                      "c.grosswrittenpremiumfunctionalamount,c.grosswrittenpremiumtransactionalamount," \
                      "c.grosswrittenpremiumtransactiontypeidentifier,c.uprcalculationflag from Fdp.vl_grosswrittenpremiumtransactions C " \
                      "where C.etlflag = 'V' AND C.batchkey='" + batchkey + "' AND  " \
                       "C.grosswrittenpremiumtransactionsurrogatekey BETWEEN '" + str(begin) + "' AND '" + str(last) + "' order by " \
                       "c.grosswrittenpremiumtransactionsurrogatekey ASC"


def transformation_df_query(begin, last, batchkey):
    """
    query to fetch the records within the surrogatekey range for the batchkey from transformation table
    :param begin: first surrogatekey
    :param last: last surrogatekey
    :param batchkey: batchkey
    :return: query as String
    """
    return "SELECT SUM(A.grossunearnedpremiumtransactionalamount) as tn_sum_grossunearnedpremiumtransactionalamount," \
               "SUM(A.grossunearnedpremiumfunctionalamount) as tn_sum_grossunearnedpremiumfunctionalamount," \
               "SUM(A.grossearnedpremiumtransactionalamount) as tn_sum_grossearnedpremiumtransactionalamount," \
               "SUM(A.grossearnedpremiumfunctionalamount) as tn_sum_grossearnedpremiumfunctionalamount," \
               "A.grosswrittenpremiumtransactionsurrogatekey FROM fdp.tn_grosswrittenpremiumtransactions A " \
               "where A.batchkey = '" + batchkey + "' AND A.grosswrittenpremiumtransactionsurrogatekey BETWEEN  '"\
                + str(begin) + "' AND '" + str(last) + "' AND A.grosswrittenpremiumtransactionidentifier not Like 'SYS%' " \
                "GROUP BY A.grosswrittenpremiumtransactionsurrogatekey ORDER BY grosswrittenpremiumtransactionsurrogatekey ASC"


def contractstatus_lookup_query(batchkey):
    """
    query to fetch the contractstatus from the lookup table ST_insurancecontractpolicydetails
    :param batchkey: batchkey
    :return: query as String
    """
    return  "select A.grosswrittenpremiumtransactionsurrogatekey,C.contractstatusidentifier " \
              "from fdp.vl_grosswrittenpremiumtransactions A, fdp.st_insuranceproductdetails B, " \
              "fdp.ST_insurancecontractpolicydetails C " \
              "where A.insuranceproductsurrogatekey = B.insuranceproductsurrogatekey AND " \
              "B.insurancecontractsurrogatekey=C.insurancecontractsurrogatekey " \
              "and A.batchkey='" + batchkey + "'"


def create_scenario(connection, batchkey, total_scenarios, total_idlist):
    """
    Create individual scenario for batchkeys with surrogate key pairs
    :param connection: database connection
    :param batchkey: batchkey
    :param total_scenarios: total_scenarios
    :param total_idlist: total_idlist
    :return: total_scenarios, total_idlist
    """
    surrogate_startdf = pd.read_sql_query(surrogate_key_startquery(batchkey), connection)
    surrogate_enddf = pd.read_sql_query(surrogate_key_endquery(batchkey), connection)
    for i in range(len(surrogate_enddf)):
        scenario = {}
        surrogate_start = surrogate_startdf.iloc[i, 0]
        surrogate_end = surrogate_enddf.iloc[i, 0]
        scenario['master_df_query'] = validation_df_query(surrogate_start, surrogate_end, batchkey)
        scenario['tn_df_query'] = transformation_df_query(surrogate_start, surrogate_end, batchkey)
        scenario['contractstatus_lookup'] = contractstatus_lookup_query(batchkey)
        scenario['start'] = str(surrogate_start)
        scenario['end'] = str(surrogate_end)
        scenario['batchkey'] = str(batchkey)
        scenario['connection'] = connection
        total_scenarios['Scenarios'].append(scenario)
        total_idlist.append('sk_' + str(surrogate_start) + '_' + str(surrogate_end))
    return total_scenarios, total_idlist


def pytest_generate_tests(metafunc):
    """
    Generate pytest scenarios
    :param metafunc:  metafunc
    """
    total_scenarios = collections.defaultdict(list)
    total_idlist = []
    connection = connect_database(os.getenv("database"))
    batchkeys_list = batchkeys.split(',')
    for batchkey in batchkeys_list:
        create_scenario(connection, batchkey, total_scenarios, total_idlist)
    metafunc.parametrize('scenario', total_scenarios["Scenarios"], ids=total_idlist)


def test_premium(scenario,extra, request):
    reportfolder = request.config.getoption('reportfolder')
    utils.PremiumCalculation.test_premium_calculation(scenario, reportfolder)
