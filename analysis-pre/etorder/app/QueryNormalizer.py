#!/usr/bin/env python

"""
Some util functions to normalize queries
"""
import re
import sys

class QueryNormalizer(object):
    def __init__(self):
        # List of stopwords 
        self.sw = [ " the ", " of ", " a ", " at ", " in ", "www.", ".com"]

    def normalize_space(self, query):
        return re.sub(r'\s+', ' ', query.strip())

    def remove_stopwords(self, query):
        query = self.normalize_space(query)
        for w in self.sw:
            query = query.replace(w, ' ')
        return query

    def remove_punc(self, query):
        # Remove non alphanumerical characters
        return self.normalize_space(re.sub(r'\W+', ' ', query))

if __name__ == '__main__':
    qry = sys.argv[1]
    normalizer = QueryNormalizer()
    print 'normalize space:',
    print normalizer.normalize_space(qry)
    print 'stopwords_remove:', 
    print normalizer.remove_stopwords(qry)
    print 'remove non alphanumeric characters', 
    print normalizer.remove_punc(qry)   
