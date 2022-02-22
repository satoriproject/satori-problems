# vim:ts=4:sts=4:sw=4:et
import os
import sys
import time

from util import ctxyaml

from satori.client.common import want_import
want_import(globals(), '*')

from testing.common import copy_file, make_test_data, upload_blob


def normalize_keys(keys):
    return sorted(map(str, keys))


def normalize_val(val):
    if val.is_blob:
        return (str(val.filename), str(val.value))
    else:
        return str(val.value)


def map_has_changed(local_map, remote_map):
    if normalize_keys(local_map.keys()) != normalize_keys(remote_map.keys()):
        return True
    for key, local_val in local_map.items():
        remote_val = remote_map[key]
        if normalize_val(local_val) != normalize_val(remote_val):
            return True
    return False
  

def sync(opts):
    with open(opts.MAPPING) as mapping_file:
        mapping = ctxyaml.load(mapping_file)

    contest = Contest.filter(ContestStruct(name=mapping.common.contest))
    if not contest:
        raise RuntimeError('Contest %s does not exist' % mapping.common.contest)
    contest = contest[0]

    if 'prefix' in mapping.common:
        prefix = mapping.common.prefix
    else:
        prefix = mapping.common.contest

    for problem_key, problem_value in mapping.problems.items():
        problem_type = 'OI' if 'examples' in problem_value else 'ACM'
        print "Syncing problem %s (%s)" % (problem_key, problem_type)
        problem_name = '[' + prefix + '] ' + problem_key
        problem = Problem.filter(ProblemStruct(name=problem_name))
        if not problem:
            print " Problem %s does not exist, creating new problem" % problem_name
            problem = Problem.create(ProblemStruct(name=problem_name))
        else:
            problem = problem[0]
        suite_names = ['tests', 'examples'] if problem_type == 'OI' else ['all']
        default_suite = None
        for suite_name in suite_names:
            tests = []
            for test_pair in problem_value[suite_name].items():
                test_name = test_pair[0]
                test_data = make_test_data(test_pair)
                test = Test.filter(TestStruct(name=test_name, problem=problem))
                if not test:
                    print " Test %s does not exist, creating" % test_name
                    test = Test.create(
                            TestStruct(name=test_name, problem=problem),
                            test_data)
                else:
                    test = test[0]
                    if map_has_changed(test_data, test.data_get_map()):
                        print " Test %s exists but has changed, updating" % test_name
                        test = test.modify_full(
                                TestStruct(name=test_name, problem=problem),
                                test_data)
                tests.append(test)
            suite = TestSuite.filter(TestSuiteStruct(problem=problem, name=suite_name))
            suite_struct = TestSuiteStruct(
                    problem=problem,
                    name=suite_name,
                    dispatcher='SerialDispatcher' if problem_type == 'ACM' else 'ParallelDispatcher',
                    reporter='ACMReporter' if problem_type == 'ACM' else 'PointsReporter',
                    accumulators='')
            if problem_type == 'ACM':
                suite_params = {
                        'ACMReporter.show_tests': AnonymousAttribute(value='false', is_blob=False),
                        'ACMReporter.run_all': AnonymousAttribute(value='false', is_blob=False)}
            else:
                if suite_name == 'examples':
                    suite_params = {
                            'PointsReporter.show_tests': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.use_judge_score': AnonymousAttribute(value='false', is_blob=False),
                            'PointsReporter.display_time': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.run_all': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.falling': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.show_score': AnonymousAttribute(value='false', is_blob=False)}
                else:
                    suite_params = {
                            'PointsReporter.show_tests': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.use_judge_score': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.display_time': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.run_all': AnonymousAttribute(value='true', is_blob=False),
                            'PointsReporter.falling': AnonymousAttribute(value='false', is_blob=False),
                            'PointsReporter.show_score': AnonymousAttribute(value='true', is_blob=False)}
            test_params = [{} for _ in tests]
            if not suite:
                print " Test suite does not exist, creating"
                suite = TestSuite.create(suite_struct, suite_params, tests, test_params)
            else:
                suite = suite[0]
                if (suite.dispatcher != suite_struct.dispatcher or
                    suite.reporter != suite_struct.reporter or
                    suite.accumulators != suite_struct.accumulators or
                    map_has_changed(suite_params, suite.params_get_map()) or
                    [t.name for t in suite.get_tests()] != [t.name for t in tests]):
                    print " Test suite exists but has changed, updating"
                    suite = suite.modify_full(suite_struct, suite_params, tests,
                            test_params)
            if (problem_type == 'ACM' or
                (suite_name == 'tests' and problem_value.get('publish') == 'results') or
                (suite_name == 'examples' and problem_value.get('publish') != 'results')):
                default_suite = suite
        if default_suite is None:
            raise("Something went wrong, cannot determine default test suite")
        attachments = []
        if 'logos' in mapping.common:
            attachments += mapping.common.logos
        if 'attachments' in problem_value:
            attachments += problem_value.attachments
        statement = problem_value.statement.getvalue()

        if problem_type == 'ACM':
            header = "Problem %s: %s" % (problem_key, problem_value.name)
        else:
            header = problem_value.name
        if header not in statement:
            print " WARNING: Problem statement does not contain '%s'" % header

        problem_mapping = ProblemMapping.filter(ProblemMappingStruct(
            contest=contest, code=problem_key))
        group = problem_value.group if 'group' in problem_value else ''
        problem_mapping_struct = ProblemMappingStruct(
                contest=contest, problem=problem, code=problem_key,
                title=problem_value.name, default_test_suite=default_suite,
                group=group)
        if not problem_mapping:
            print " Problem mapping does not exist, creating"
            problem_mapping = ProblemMapping.create(problem_mapping_struct)
            for attachment in attachments:
                path = attachment.path
                problem_mapping.statement_files_set_blob_path(
                        os.path.basename(path), path)
            try:
                problem_mapping.statement = statement
            except SphinxException as sphinx_exception:
                print sphinx_exception
        else:
            problem_mapping = problem_mapping[0]
            if (problem_mapping.problem != problem_mapping_struct.problem or
                problem_mapping.title != problem_mapping_struct.title or
                problem_mapping.default_test_suite != problem_mapping_struct.default_test_suite or
                problem_mapping.statement != statement or
                problem_mapping.group != problem_mapping_struct.group):
                print " Problem mapping exists but has changed, updating"
                problem_mapping = problem_mapping.modify(problem_mapping_struct)
                for attachment in attachments:
                    path = attachment.path
                    problem_mapping.statement_files_set_blob_path(
                            os.path.basename(path), path)
                try:
                    problem_mapping.statement = statement
                except SphinxException as sphinx_exception:
                    print sphinx_exception
