#!/usr/bin/env python

"""
Compute the dsecriptive stats per user
"""
import sys
import os
#sys.path.insert(0, os.path.abspath('../'))

import simplejson as js
import argparse
#from analyzer 
import UserAnalyzer as ua

def read_input(file):
    for line in file:
        yield line.strip()
        
def read_mapper_output(file, delimiter='\t'):
    for line in file:
        yield line.rstrip().split('\t')

def map():
    records = read_input(sys.stdin)
    i = 0
    for record in records:
        #print record
        data = js.loads(record)
        key = data['uid']
        analyser = ua.UserAnalyzer(data)
        if analyser.check_bot():
            continue
        res = analyser.run()
        res['uid'] = key
        print '%s\t%s'%(i, js.dumps(res))
        i += 1

def reduce():
    data = read_mapper_output(sys.stdin)
    for d in data:
        print d[1]
            
        

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='This script reads log entries as \
        system std and parse it for entity linking and further analysis. It parses the \
        following logs: AOL, KB.')

    argparser.add_argument('action', type=str, 
                    help='choose between "map" and "reduce"')

    args = vars(argparser.parse_args())
    if args.get('action') == 'map':
        map()
    elif args.get('action') == 'reduce':
        reduce() 





