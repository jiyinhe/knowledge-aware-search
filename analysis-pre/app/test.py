#!/usr/bin/env python


import sys
import test1

#t = test1.testClass()

def read_input(file):
    for line in file:
        yield line.strip()
        
def read_mapper_output(file, delimiter='\t'):
    for line in file:
         yield line.strip().rsplit(delimiter, 1)
def map():
    records = read_input(sys.stdin)
    for line in records:
        print '%s\t%s'%(1, line) 

def reduce():
    data = read_mapper_output(sys.stdin)
    for d in data:
        print d
 
if __name__ == '__main__':
    if sys.argv[1] == 'map':
        map()
    else:
        reduce()
        
