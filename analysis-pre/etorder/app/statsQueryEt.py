#!/usr/bin/env python


"""
Examine stats of the entity pairs in query: 
- frequency of the et pair in WPclickstream (if doesn't occur then count 0)
- count of number of users own an et pair 

Note: WP concepts in search logs are url encoded, but not in WPClickStream

Input1: from preprocessed search log:
{"uid": x, 
"queries": [{"qid": "x", "qtime": "x", 
"entities": [{"flag": "1/1", "score": -5.63634846369048, "intent": "...", "entity": "x"}], 
"urls": [[counter, rank, "url"]], 
"qtext": "x", 
"phase": x}

Input2: from WPclickstream
""" 
import sys
import itertools as it
import urllib
import argparse
import UserAnalyzer as ua
import ParseWPClickStream as pwp
import json as js

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
        # First check if it's input 1 or 2 
        fields = line.rstrip().split('\t')
        if len(fields) == 1:
            # From log
            u_history = js.loads(line)
            analyzer = ua.UserAnalyzer(u_history)
            if analyzer.check_bot():
                continue
            i = 0
            prev_entities = set([])
            for q in u_history['queries']:
                # First check for queries that not new queries (e.g. from pagination)
                notnewquery = analyzer.check_newquery(i)
                if not notnewquery:
                    # Then check for entities
                    entities = analyzer.check_entities(i)
                    # process this query
                    for e in entities:
                        for pe in prev_entities:
                            print '%s %s\t%s'%(pe, e['entity'], 'user%s'%u_history['uid'])
                        # Only count unique pairs
                        if not e['entity'] in prev_entities:
                            prev_entities.add(e['entity'])
                i += 1
        else:
            # Form WP clickstream
            res = pwp.parse_line(fields)
            if not res == None:
                if not res[0].startswith('other'):
                    print '%s %s\t%s'%res


def reduce():
    data = read_mapper_input(sys.stdin)
    for key, group in it.groupby(data, lambda x: x[0]):
        # number of users that owns the pair
        users = set([])
        # number of click transitions in wp
        wp_count = 0
        for g in group:
            if g[1].startswith('user'):
                users.add(g[1])
            # If discovered a WP count  
            else:
                wp_count = int(g[1])

        if not len(users) == 0:     
            print '%s\t%s\t%s'%(key, len(users), wp_count)


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='This script computes stats of entity pair mentions in queries: \n \
        - count of et pair frequencies by users (how many users have this et pair \n \
        - frequency of the et pair in WPclickstream (0 if not exist)')

    argparser.add_argument('action', type=str, 
                    help='choose between "map" and "reduce"')

    args = vars(argparser.parse_args())
    if args.get('action') == 'map':
        map()
    elif args.get('action') == 'reduce':
        reduce() 


