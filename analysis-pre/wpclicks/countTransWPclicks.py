#!/usr/bin/env python

"""
This script counts the transition frequencies of entity pairs. 

We count total counts over all dumps
"""

import argparse
import sys
import itertools as it

def read_input(file):
    for line in file:
        yield line.rstrip().split('\t') 

def read_mapper_input(file):
    for line in file:
        yield line.strip().split('\t')

def map():
    lines = read_input(sys.stdin)    
    c1, c2, count = '', '', ''
    for line in lines:
        # 2015-01: prev_id, curr_id, count, prev_title, curr_title
        if len(line) == 5:
            prev_id, curr_id, count, c1, c2 = line
        # 2015-02: prev_id, curre_id, count, prev_title, curr_title, type
        elif len(line) == 6:
            prev_id, curr_id, count, c1, c2, type = line
        # 2016: prev_title, current_title, count, count 
        elif len(line) == 4:
            c1, c2, type, count = line
        if c1.startswith('other') or count in ['', 'n']:
            continue
        else:
            print '%s %s\t%s'%(c1, c2, count)
    
def reduce():
    data = read_mapper_input(sys.stdin)
    for key, group in it.groupby(data, lambda x: x[0]):
        count = 0
        for g in group:
            count += int(g[1])
        print '%s\t%s'%(key, count)
 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Compute statistics of WP click streams")
    parser.add_argument("action", type=str, 
                        help='choose between "map" and "reduce"')
    args = vars(parser.parse_args())
    if args.get('action') == 'map':
        map()
    elif args.get('action') == 'reduce':
        reduce()
