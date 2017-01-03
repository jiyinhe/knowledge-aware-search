#!/usr/bin/env python

"""
Analyse logs from a user session. 
- # queries per user: single query user should be filtered out

"""
import sys
#import os
#sys.path.insert(0, os.path.abspath('../'))
from datetime import datetime
import itertools as it
#from utils 
import QueryNormalizer as qn

class UserAnalyzer(object):
    def __init__(self, data, entity_threshold=-3):
        self.queries = data['queries']
        self.e_thresh = entity_threshold
        self.res = {}
        self.qn = qn.QueryNormalizer()

        # Prepare for clustering of queries based on qtext, urls, and entities
        # c_qtext contains 
        #{key, [[list of queries added to this group], count_addnew]}
        # where key is the set of terms occured in the group
        # count_addnew is the number of time new terms were added by a new query 
        self.c_qtext = {} 
        # c_clicks, c_et contains
        # {key, [[list of clicked urls added to this group], count_addnewurl]}
        # where key is a repeated query (after cleaning)
        self.c_clicks = {} 
        # {key, [[list of queries added to this group], count_addednewquery]}
        # where key is the entities identified 
        self.c_et = {}

    def run(self):
        # Sort the queries in time
        self.queries.sort(key=lambda x: datetime.strptime(x['qtime'], '%Y-%m-%d %H:%M:%S'))

        for i in range(len(self.queries)):
            # Check if a query is a new query
            self.check_newquery(i)
            # Check if a query is a direct url input
            self.check_url(i)
            # Threshold on entities
            self.check_entities(i)

            thisquery = self.queries[i]
            # Filter out non new queries and direct url input
            if thisquery['newquery'] and (not thisquery['is_url']):
                # Test change of mind
                self.change_of_queries(thisquery)            
                self.change_of_clicks(thisquery)
                self.change_of_entities(thisquery)
    
        # number of queries issued 
        newqueries = list(it.ifilter(lambda x: x['newquery']==False, self.queries))
        # Among new queries, number of queries that are not direct urls
        noturls = list(it.ifilter(lambda x: x['is_url']==False, newqueries))
        # Among those, number of queries with entities above threshold -3
        withentities = list(it.ifilter(lambda x: len(x['entities'])>0, noturls)) 
        # Among noturls, number of queries with clicks
        withclicks = list(it.ifilter(lambda x: not x['urls'][0][1] == '', noturls))

        self.res['#q'] = len(newqueries)
        self.res['#not_urls'] = len(noturls)
        self.res['#w_et'] = len(withentities)
        self.res['#w_click'] = len(withclicks)
        # Number of clusters that contains similar but not identical queries
        with_qchange = it.ifilter(lambda x: self.c_qtext[x][1] > 0, self.c_qtext)
        self.res['#change_q'] = len(list(with_qchange))
        # Number of clusters that users have clicked new results for the same query
        with_cchange = it.ifilter(lambda x: self.c_clicks[x][1] > 0, self.c_clicks)
        self.res['#change_c'] = len(list(with_cchange))
        # Number of clusters that users have issued different queries containing the
        # same entities
        with_echange = it.ifilter(lambda x: self.c_et[x][1] > 0, self.c_et)
        self.res['#change_e'] = len(list(with_echange))

        return self.res
        # Clustering of queries - qtext 
#        for x in self.c_qtext:
#            print x, self.c_qtext[x]
        # Clustering of queries - clicks
#        for x in self.c_clicks:
#            print x, self.c_clicks[x]
        # Clustering of queries - entities
#        for x in self.c_et:
#            print x, self.c_et[x]
#        print


    def check_newquery(self, idx):
        """ If consecutive queries are the same, we assume that is a pagination
             or rehit the search button rather than a new query"""
        # first query is not a pagination
        self.queries[idx]['newquery'] = False
        if idx > 0 and self.queries[idx]['qtext'] == self.queries[idx-1]['qtext']:
            self.queries[idx]['newquery'] = True

    def check_url(self, idx):
        """
        Check if a  query is a direct url inputs
        """
        if len(self.queries[idx]['qtext'].split('.'))>1 and \
            len(self.queries[idx]['qtext'].split(' ')) == 1:
            self.queries[idx]['is_url'] = True
        else:
            self.queries[idx]['is_url'] = False

    def check_entities(self, idx):
        """ Threshold on entities """
        self.queries[idx]['entities'] = list(it.ifilter(lambda x: 
            x['score']>self.e_thresh, self.queries[idx]['entities']))
        
    def change_of_queries(self, query):
        """Find sequencies of queries that share the same words but are not
        identical"""
        qtext = query['qtext']
        q = self.qn.remove_stopwords(qtext)
        q = self.qn.remove_punc(q)

        qwords = set(q.split(' '))
        foundgroup = False
        for key in self.c_qtext:
            terms = set(key.split(' '))
            # If there are overlapping words, then it's in the same group
            if len(qwords.intersection(terms)) > 0:
                newkey = ' '.join(sorted(list(terms.union(qwords))))
                # Update the group and its key
                group, counter = self.c_qtext.pop(key)
                group.append(query)
                # If adding new terms to the group, counter + 1  
                if len(qwords - terms) > 0:
                    counter += 1
                self.c_qtext[newkey] = [group, counter]
                foundgroup = True
                break
        # Otherwise create a newgroup
        if not foundgroup:
            newkey = ' '.join(sorted(list(qwords)))
            if not newkey.strip() == '':
                self.c_qtext[newkey] = [[query], 0]

    def change_of_clicks(self, query):
        """Find sequences of queries that are identical, but with different clicked
        results."""
        qtext = query['qtext']
        q = self.qn.remove_stopwords(qtext)
        q = self.qn.remove_punc(q)

        qwords = ' '.join(sorted(q.split(' ')))
        foundgroup = False
        for key in self.c_clicks:
            if qwords == key:
                # Found a group, check urls
                # get current urls
                past_urls = set(self.c_clicks[key][0])
                current_urls = set([c[2] for c in query['urls'] if not c[2] == ''])
                # new urls clicked
                if len(current_urls - past_urls) > 0:
                    self.c_clicks[key][1] += 1
                self.c_clicks[key][0] += [c[2] for c in query['urls'] if not c[2] == '']
                foundgroup = True
                break
        if not foundgroup:
            # create a new group
            self.c_clicks[qwords] = [[c[2] for c in query['urls'] if not c[2] == ''], 0]

    def change_of_entities(self, query):
        """Find sequences of queries that share the same entities but with different
        intent part """
        entities = [c['entity'] for c in query['entities'] if c['score'] >= self.e_thresh]

        newkey = ' '.join(sorted(entities))
        foundgroup = False 
        for key in self.c_et:
            if key == newkey:
                # Found a group, check if the query is the same
                qs, counter = self.c_et[key]
                if not query['qtext'] in qs:
                    counter += 1
                qs.append(query['qtext'])                
                self.c_et[key] = [qs, counter]
                foundgroup = True
                break
        if not foundgroup:
            self.c_et[newkey] = [[query['qtext']], 0]

