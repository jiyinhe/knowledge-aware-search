#!/usr/bin/env python

"""
This script generate distribution of the frequencies of click pairs
in WP click stream. It uses the output of countTransWPclicks.py

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
    for line in lines:
        print '%s\t%s'%(line[1], 1)
 
def reduce():
    data = read_mapper_input(sys.stdin)
    for key, group in it.groupby(data, lambda x: x[0]):
        count = 0
        for g in group:
            count += 1
        print '%s\t%s'%(key, count)
 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Compute distribution of frequencies of click pairs from WP click streams")
    parser.add_argument("action", type=str, 
                        help='choose between "map" and "reduce"')
    args = vars(parser.parse_args())
    if args.get('action') == 'map':
        map()
    elif args.get('action') == 'reduce':
        reduce()
