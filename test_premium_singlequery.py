import collections
import os

from pytest_html import extras
from tests.FintAsserts import singlequery_test, generate_tests


def pytest_generate_tests(metafunc):
    total_scenarios = collections.defaultdict(list)
    total_idlist = []
    premium_yaml_files = 'tests/premium_fdp_integration/premium_sttm_validation'
    # generate test from grosswrittenpremium single query yaml
    total_scenarios, total_idlist = generate_tests("select distinct batchkey from "
                                                   "fdp.ln_grosswrittenpremiumtransactions where "
                                                   "batchkey in ("+os.getenv('batchkeys')+")",
                                                   "_grosswritten_singlequery.yaml", total_scenarios, total_idlist,
                                                   premium_yaml_files)
   # generate test from OraclePremium single query yaml
    total_scenarios, total_idlist = generate_tests("select distinct batchkey from "
                                                   "fdp.ln_oraclepremium where "
                                                   "batchkey in ("+os.getenv('batchkeys')+")",
                                                   "_oraclepremium_singlequery.yaml", total_scenarios, total_idlist,
                                                   premium_yaml_files)
    metafunc.parametrize('scenario', total_scenarios["Scenarios"], ids=total_idlist)


def test_premiums_singlequery(scenario, extra, request):
    premium_failed_records_count, premium_records_count = singlequery_test(scenario, extra, request)
    if premium_failed_records_count > 0 or premium_records_count == 0:
        if premium_records_count == 0:
            extra.append(extras.html("Empty record. Please make sure that the data is populated"))
        assert False
    else:
        assert True
