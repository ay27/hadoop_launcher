#!/usr/bin/env python

import os
import time
import subprocess
import argparse
from config import *
import sys

files = ' '.join(input_file)

cmd_mkdir_input = 'hadoop fs -mkdir -p %s' % input_dir
cmd_rm_output = 'hadoop fs -rm -r %s' % output_dir
cmd_put_input_file = 'hadoop fs -put -f %s %s' % (files, input_dir)
cmd_run_task = 'hadoop jar %s %s %s' % (jar_file, input_dir, output_dir)
# cmd_ls_result_dir = 'hadoop fs -ls %s' % output_dir
# cmd_cat_result = 'hadoop fs -cat %s'


def runit(cmd, log_it=False):
    if log_it:
        p = subprocess.Popen('%s &> %s' % (cmd, log_file), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    rc = p.returncode
    if rc == 0:
        return out
    else:
        raise AssertionError('\n%s' % err)


def run_task(log_it=False):
    print('clean input dir and output dir')
    runit(cmd_mkdir_input, log_it)
    try:
        print('delete output file? (yes)')
        line = sys.stdin.readline().strip()
        if line.upper() == 'YES' or len(line) == 0:
            runit(cmd_rm_output, log_it)
        else:
            print('output dir already exist')
            sys.exit(-1)
    except AssertionError:
        pass
    print('upload input files...')
    runit(cmd_put_input_file, log_it)
    print('upload success')

    print('start hadoop task...')
    start_time = time.time()
    runit(cmd_run_task, log_it)
    print('hadoop task finish, elapsed time = %f' % (time.time() - start_time))


def get_result():
    print('result')


def parse_args():
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers(help='sub command')

    run_parser = subparser.add_parser('run', help='run the hadoop task')
    run_parser.set_defaults(command_object=run_task)
    run_parser.add_argument('-log', action='store_const',
                            const=True, default=False, help='log file')

    get_result_parser = subparser.add_parser('result', help='get result')
    get_result_parser.set_defaults(command_object=get_result)
    args = parser.parse_args()
    try:
        if args.log:
            args.command_object(args.log)
        else:
            args.command_object()
    except AttributeError:
        args.command_object()


if __name__ == '__main__':
    parse_args()
