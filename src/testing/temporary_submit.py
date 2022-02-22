# vim:ts=4:sts=4:sw=4:et
import os
import sys
import time

from util import ctxyaml

from satori.client.common import want_import
want_import(globals(), '*')

from testing.common import copy_file, make_test_data, upload_blob


def _temporary_submit_internal(
        test_pair, submit_file_path, override_time=None, store_io=False):
    test_data = make_test_data(test_pair, True)
    if override_time is not None and 'time' in test_data:
        test_data['_original_time'] = test_data['time']
        test_data['time'] = AnonymousAttribute(is_blob=False, value=override_time)
    if store_io:
        test_data['store_io'] = AnonymousAttribute(is_blob=False, value='true')
    if submit_file_path[:1] == '%':
        submits = Submit.filter(SubmitStruct(id=int(submit_file_path[1:])))
        if not submits:
            raise RuntimeException("Cannot find submit " + submit_file_path)
        submit_data = submits[0].data_get_map()
    else:
        submit_data = {'content': upload_blob(submit_file_path)}
    submit = TemporarySubmit.create(test_data, submit_data)
    print 'Testing %s on %s, temporary submit id: %d' % (
            submit_data['content'].filename,
            test_data['name'].value,
            submit.id)
    return submit


def _prettyprint_table(table):
    widths = [max(map(len, column)) for column in zip(*table)]
    for row in table:
        row = [elem.ljust(width) for (elem, width) in zip(row, widths)]
        print '  '.join(row)


def _results_header():
    return ['solution', 'test', 'status', 'time', 'temporary submit id']


def _get_from_map(attr_map, attr):
    if attr in attr_map:
        return str(attr_map[attr].value)
    else:
        return '?'


def _get_time_limit(test_map):
    if '_original_time' in test_map:
        return '%s (%s)' % (
                _get_from_map(test_map, '_original_time'),
                _get_from_map(test_map, 'time'))
    else:
        return _get_from_map(test_map, 'time')


def _submit_to_results_row(submit):
    test_map = submit.test_data_get_map()
    result_map = submit.result_get_map()
    result_row = []
    result_row.append(submit.submit_data_get_map()['content'].filename)
    result_row.append(_get_from_map(test_map, 'name'))
    result_row.append(_get_from_map(result_map, 'status'))
    result_row.append('%s / %s' % (
        _get_from_map(result_map, 'execute_time_cpu'),
        _get_time_limit(test_map)))
    result_row.append(str(submit.id))
    return result_row


def _results_to_2d_table(solutions, submits):                                    
    assert len(submits) % len(solutions) == 0                                    
    header = ['test'] + list(solutions) + ['limit']                              
    table = [header]                                                             
    for i in range(0, len(submits) / len(solutions)):                            
        row = []                                                                 
        test_map = submits[i].test_data_get_map()                                
        row.append(_get_from_map(test_map, 'name'))                              
        for j in range(i, len(submits), len(submits) / len(solutions)):          
            result_map = submits[j].result_get_map()                             
            status = _get_from_map(result_map, 'status')                         
            if status == 'OK':                                                   
                status = _get_from_map(result_map, 'execute_time_cpu')           
            row.append(status)                                                   
        row.append(_get_time_limit(test_map))                                    
        table.append(row)                                                        
    return table


def _wait_for_results(submits):
    waiting_start = time.time()
    total = len(submits)
    print 'Waiting for results, %d/%d done' % (0, total),
    sys.stdout.flush()
    last_reported = 0
    while True:
        done = 0
        for submit in submits:
            if submit.result_get_list():
                done += 1
        if done >= total:
            break
        if done > last_reported:
            print
            print 'Waiting for results, %d/%d done' % (done, total),
            sys.stdout.flush()
            last_reported = done
        time.sleep(2)
        print '.',
        sys.stdout.flush()
    waiting_time = time.time() - waiting_start
    print
    print 'You had to wait %ds' % int(round(waiting_time))


def _store_result_blob(result_map, blob_name, out_fname):
    if blob_name in result_map:
        with open(out_fname, 'w') as out_file:
            copy_file(Blob.open(result_map[blob_name].value), out_file)


def _store_io(submit):                                                           
    test_name = submit.test_data_get_map()['name'].value
    result_map = submit.result_get_map()
    _store_result_blob(result_map, 'input_file', test_name + '.in')
    _store_result_blob(result_map, 'output_file', test_name + '.out')


def temporary_submit(opts):
    with open(opts.TESTSUITE) as tests_file:
        tests = ctxyaml.load(tests_file)
    
    submits = []
    for submit_file_path in opts.SOLUTIONS:
        for test_pair in tests.items():
            submit = _temporary_submit_internal(
                    test_pair, submit_file_path, opts.time, opts.store_io)
            submits.append(submit)

    _wait_for_results(submits)
    if not opts.verbose:
        if opts.results2d:                  
            table = _results_to_2d_table(opts.SOLUTIONS, submits)
        else:
            table = [_results_header()] + map(_submit_to_results_row, submits)
        _prettyprint_table(table)
    else:
        for submit in submits:
            print '=' * 70
            _verbose_result_internal(submit, opts.length_limit)
    if opts.store_io:
        for submit in submits:
            _store_io(submit)


def _print_bold_caption(caption):
    print '\033[1m' + caption + ':' + '\033[0m',


def _verbose_result_internal(submit, length_limit=4096):
    _print_bold_caption('solution')
    print submit.submit_data_get_map()['content'].filename
    _print_bold_caption('test')
    print submit.test_data_get_map()['name'].value

    result = submit.result_get_list()
    if not result:
        print 'no results available yet'
        return
    for attr in result:
        _print_bold_caption(attr.name)
        if attr.is_blob:
            print
            content = submit.result_get_blob(attr.name).read(length_limit)
            content = content.rstrip('\n')
            if not content:
                continue
            print content
        else:
            print attr.value


def verbose_result(opts):
    submit = TemporarySubmit.filter(TemporarySubmitStruct(id=int(opts.TSID)))
    if not submit:
        raise RuntimeError('Unknown temporary submit id')
    submit = submit[0]
    _verbose_result_internal(submit, opts.length_limit)
