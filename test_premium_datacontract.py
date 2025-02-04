import os
from copy import deepcopy

import pytest
import yaml

from database_comparator.DataBaseUtil import execute_query
from database_comparator.DataContractUtil import validate_conditional_mandatory, validate_mandatory, \
    validate_mandatory_ref
from tests.FintAsserts import generate_report


def pytest_generate_tests(metafunc):
    contracts_to_validate = []
    idlist = []
    with open(
            "tests/premium_fdp_integration/premium_datacontract_validation/premium_data_contract.yaml") as file:
        yamlcontent = yaml.load(file, Loader=yaml.FullLoader)
        for contract in yamlcontent['premium_data_contract']:
            batchkeys = execute_query(os.getenv("database"),
                                      "select distinct batchkey from fdp." + contract['contract'][
                                          'landing_table'] + " where batchkey in ("+ os.getenv('batchkeys') +")")
            for batchkey in batchkeys['batchkey']:
                batch_contract = deepcopy(contract)
                batch_contract['contract']['batchkey'] = str(batchkey)
                contracts_to_validate.append(batch_contract)
                idlist.append(batch_contract['contract']['name'] + ' - ' + str(batchkey))
    metafunc.parametrize('contract', contracts_to_validate, ids=idlist)


def test_premium_conditional_mandatory_columns(contract, extra, request):
    with open(contract['contract']['schema']) as schemafile:
        yamlcontent = yaml.load(schemafile, Loader=yaml.FullLoader)
        if 'conditional_mandatory_columns' not in yamlcontent:
            pytest.skip('no conditional mandatory columns')
        conditional_mandatory_columns = yamlcontent['conditional_mandatory_columns']
    premium_report_dir = request.config.getoption('reportfolder')
    premium_errors = validate_conditional_mandatory(contract['contract']['landing_table'], contract['contract']['batchkey'], conditional_mandatory_columns)
    generate_report(errors=premium_errors, report_dir=premium_report_dir, report_filename="premium_cond_mandatoryreportlog_",
                    extra=extra)
    if len(premium_errors) > 0:
        assert False
    else:
        assert True


def test_premium_mandatory_columns(contract, extra, request):
    with open(contract['contract']['schema']) as schemafile:
        yamlcontent = yaml.load(schemafile, Loader=yaml.FullLoader)
        mandatory_columns = yamlcontent['mandatory_columns']
    premium_report_dir = request.config.getoption('reportfolder')
    premium_errors = validate_mandatory(contract['contract']['landing_table'], contract['contract']['batchkey'], mandatory_columns)
    generate_report(errors=premium_errors, report_dir=premium_report_dir, report_filename="premium_mandatoryreportlog_",
                    extra=extra)
    if len(premium_errors) > 0:
        assert False
    else:
        assert True


def test_premium_mandatory_reference_data(contract, extra, request):
    with open(contract['contract']['schema']) as schemafile:
        yamlcontent = yaml.load(schemafile, Loader=yaml.FullLoader)
        if 'reference_columns' not in yamlcontent:
            pytest.skip('no reference columns')
        mandatory_reference_columns = yamlcontent['reference_columns']
    premium_report_dir = request.config.getoption('reportfolder')
    premium_errors = validate_mandatory_ref(contract['contract']['landing_table'], contract['contract']['batchkey'], mandatory_reference_columns)
    generate_report(errors=premium_errors, report_dir=premium_report_dir, report_filename="premium_mandatoryrefreportlog_",
                    extra=extra)
    if len(premium_errors) > 0:
        assert False
    else:
        assert True
