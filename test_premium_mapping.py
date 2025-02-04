import collections
import os
from pytest_html import extras

from tests.FintAsserts import mapping_test, generate_tests


# Generate tests from yaml files
def pytest_generate_tests(metafunc):
    total_scenarios = collections.defaultdict(list)
    total_idlist = []
    premium_yaml_files = 'tests/premium_fdp_integration/premium_sttm_validation'
    # generate test from grosswrittenpremium mapping yaml
    total_scenarios, total_idlist = generate_tests("select distinct batchkey from "
                                                   "fdp.ln_grosswrittenpremiumtransactions where "
                                                   "batchkey in ("+os.getenv('batchkeys')+")",
                                                   "_grosswritten_mapping.yaml", total_scenarios, total_idlist,
                                                   premium_yaml_files)
    # generate test from manualpartnerInsurancerevenue mapping
    total_scenarios, total_idlist = generate_tests("select distinct batchkey from "
                                                   "fdp.ln_oraclepremium where "
                                                   "batchkey in ("+os.getenv('batchkeys')+")",
                                                   "_oraclepremium_mapping.yaml", total_scenarios, total_idlist,
                                                   premium_yaml_files)
    metafunc.parametrize('scenario', total_scenarios["Scenarios"], ids=total_idlist)


def test_premiums_mapping(scenario, extra, request):
    premium_matched_records_count, premium_mismatch_records_count, premium_missing_records_count = mapping_test(scenario, extra, request)
    if premium_missing_records_count == 0 and premium_matched_records_count == 0 and premium_mismatch_records_count == 0:
        extra.append(extras.html("Empty record. Please make sure that the data is populated"))
        assert False
    if premium_missing_records_count > 0 or premium_matched_records_count < 1 or premium_mismatch_records_count > 0:
        assert False
    else:
        assert True
