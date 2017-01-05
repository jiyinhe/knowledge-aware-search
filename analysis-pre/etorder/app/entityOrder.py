#!/usr/bin/env python


"""
It extract the order of entity mentions in queries 

Input log format: 
{"uid": x, 
"queries": [{"qid": "x", "qtime": "x", 
"entities": [{"flag": "1/1", "score": -5.63634846369048, "intent": "...", "entity": "x"}], 
"urls": [[counter, rank, "url"]], 
"qtext": "x", 
"phase": x}

Output format:
entity1 entity2 count
where entity1 is mentioned before entity2
"""
import argparse
import simplejson as js
import sys
import UserAnalyzer as ua
import itertools as it

entity_thresh = -3

def read_input(file):
    for line in file:
        yield line

def read_mapper_input(file):
    for line in file:
        yield line.strip().split('\t')

def map():
    lines = read_input(sys.stdin)
    # print the order of all entity pairs within a user's history
    for line in lines:
        u_history = js.loads(line)
        analyzer = ua.UserAnalyzer(u_history)
        i = 0
        prev_entities = []
        for q in u_history['queries']:
            # First check for queries that not new queries (e.g. from pagination)
            notnewquery = analyzer.check_newquery(i)
            if not notnewquery:
                # Then check for entities
                entities = analyzer.check_entities(i)
                # process this query
                for e in entities:
                    for pe in prev_entities:
                        print '%s %s\t%s'%(pe, e['entity'], 1)
                    prev_entities.append(e['entity'])
            i += 1

def reduce():
    data = read_mapper_input(sys.stdin)
    for key, group in it.groupby(data, lambda x: x[0]):
        #count = sum([int(x[1]) for x in group])
        count = 0
        for g in group:
            count += 1
        print '%s\t%s'%('\t'.join(key.split()), count)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='This script extract the order of entity mentions in queries.')

    argparser.add_argument('action', type=str, 
                    help='choose between "map" and "reduce"')

    args = vars(argparser.parse_args())
    if args.get('action') == 'map':
        map()
    elif args.get('action') == 'reduce':
        reduce() 


