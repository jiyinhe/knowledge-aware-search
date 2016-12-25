#!/usr/bin/env python

import argparse
import sys
import simplejson as js
import itertools as it

"""
Format of final log record:
{'uid': userid (either exact or proximated, or leave as "U")
 'qid': querid that is unique within a single user, for example, composed of uid +
timestamp
 'qtext': query text,
 'urls': [(url1, timestamp, rank), (url2 ..], clicked urls
 'qtype': query type: [new|page|na] whether it's a new query or a pagination, or not
    known
 'qtime': timestamp of the query
 'qel': entity linking result of the query
}
"""
# A parser factory for different logs
class ParserFactory:
    factories = {}
    def createParser(parsername):
        if not ParserFactory.factories.has_key(parsername):
            ParserFactory.factories[parsername] = \
                eval(parsername + '.Factory()')
        return ParserFactory.factories[parsername].create()
    createParser = staticmethod(createParser)

# An abstract class
class Parser (object):
    def __init__(self):
        self.counter = 0
        self.ignore = None
        self.delimiter = '\t'

    def read_input(self, file):
        for line in file:
            self.counter += 1
            if line.startswith(self.ignore):
                continue
            yield [self.counter] + line.strip().split(self.delimiter)
        
    def read_mapper_output(self, file, delimiter='\t'):
        for line in file:
            yield line.strip().rsplit(delimiter, 1)

    def map(self, phase=1): 
        records = self.read_input(sys.stdin)
        for record in records:
            if phase == 1:
                self.map_phase1(record)
            elif phase == 2:
                self.map_phase2(record)
            elif phase == 3:
                self.map_phase3(record)
            elif phase == 4:
                self.map_phase4(record)

    def reduce(self, phase=1, delimiter='\t'):
        data = self.read_mapper_output(sys.stdin, delimiter)
        if phase == 1:
            self.reduce_phase1(data)
        elif phase == 2:
            self.reduce_phase2(data)
        elif phase == 3:
            self.reduce_phase3(data)
        elif phase == 4:
            self.reduce_phase4(data)
    
    def map_phase1(self, record): pass 
    def map_phase2(self, record): pass 
    def map_phase3(self, record): pass 
    def map_phase4(self, record): pass 


    def reduce_phase1(self, data): pass
    def reduce_phase2(self, data): pass
    def reduce_phase3(self, data): pass
    def reduce_phase4(self, data): pass

class AOLParser(Parser):
    # Format of AOL: AnonID Query QueryTime ItemRank ClickURL
    # Each line in the data represents one of two types of events:
    #    1. A query that was NOT followed by the user clicking on a result item.
    #    2. A click through on an item in the result list returned from a query.
    #In the first case (query only) there is data in only the first three columns/fields
    #-- namely AnonID, Query, and QueryTime (see above). 
    #In the second case (click through), there is data in all five columns.  For click
    #through events, the query that preceded the click through is included.  Note that if
    #a user clicked on more than one result in the list returned from a single query,
    #there will be TWO lines in the data to represent the two events.  Also note that if
    #the user requested the next "page" or results for some query, this appears as a
    #subsequent identical query with a later time stamp.
    def __init__(self):
        self.counter = 0
        self.ignore = 'AnonID'
        self.delimiter = '\t'

    # Input is the original log, line by line, splitted
    # over fields, this function process one line
    # counter AnonID Query QueryTime ItemRank ClickURL
    def map_phase1(self, record):
        i = 0
        key, value = [], {}
        counter = -1 
        url = [counter, '', '']
        for item in record:
            if i == 0:
                url[0] = item
            if i>0 and i < 4:
                key.append(item)
            elif i == 4:
                # Try to convert the rank to int
                try:
                    url[1] = int(item)
                except ValueError:
                    # If not successful then mark it
                    url[1] = -1
            elif i == 5:
                url[2] = item
            i += 1

        value = {'uid': key[0],
            'qid': '-'.join(key), 
            'qtext': key[1],
            'qtime': key[2],
            'url': url,
            }
        v = js.dumps(value)
        k = '-'.join(key)
        print '%s\t%s'%(k, v)

    # Input is the original log, line by line splitted over 
    # fields. This function process one line
    # counter AnonID Query QueryTime ItemRank ClickURL
    def map_phase2(self, record):
        i = 0
        record = list(record)
        key = '-'.join(record[1:4])
        value = record[2]
        print '%s\t%s'%(key, value)

    # Input comes from either phase1 or phase2 output
    # each line of phase1 output: key, tab, json object
    # each line of phase2 output contains tab seperated fields:
    # key, 
    def map_phase3(self, record):
        record = list(record)
        # check from which file the record comes from
        if len(record) == 3:
            # from phase 1: counter, key, phase1, json object
            counter, key, content = record
            print '%s\t%s'%(key, content)
        elif len(record) == 9:
            # from result of entity linking:
            counter, key, etype, query, intentpart, et, score, flag1, flag2 = record 
            value = {
                'entity': et.strip(), 
                'intent': intentpart.strip(),
                'score': float(score.strip()),
                'flag': flag1.strip(),
            } 
            print '%s\t%s'%(key, js.dumps(value))

    # input is output of phase3, each line contains: counter, json object
    # about a query, the goal of phase4 is to group the json objects
    # by user id
    def map_phase4(self, record):
        counter, json = record
        item = js.loads(json) 
        k = item['uid']
        print '%s\t%s'%(k, js.dumps(item))

    def reduce_phase1(self, data):
        for key, group in it.groupby(data, lambda x: x[0]):
            urls = []
            entry = {}
            # merge the urls clicked for the same query
            for g in group:
                entry = js.loads(g[1])
                urls.append(entry['url'])
            entry['urls'] = urls
            entry['phase'] = 1
            # remove temporary field url
            entry.pop('url')
            # print entry
            print '%s\t%s'%(key, js.dumps(entry))

    def reduce_phase2(self, data):
        for key, group in it.groupby(data, lambda x: x[0]):
            g = list(group)
            print '%s\t%s'%(key, g[0][1])

    def reduce_phase3(self, data):
        # all entries in a group is about a query (key)
        # either from the log or from the entity linking output
        for key, group in it.groupby(data, lambda x: x[0]):
            entities = []
            item = {}
            for k, v in group:
                value = js.loads(v)
                if 'phase' in value:
                    item = value
                elif 'entity' in value:
                # from enetity linking
                    entities.append(value)
            # add entities
            item['entities'] = entities
            item['phase'] = 3
            print js.dumps(item)
 
    def reduce_phase4(self, data):
        # group input by key (userid)
        for key, group in it.groupby(data, lambda x: x[0]):
            item = {}
            item['uid'] = key
            eles = []
            for k, g in group:
                json = js.loads(g)
                json.pop('uid')
                eles.append(json)
            item['queries'] = eles
            print js.dumps(item)   
        
    class Factory:
        def create(self): return AOLParser()

class KBParser(object):
    pass



if __name__ == '__main__':
    # Note, the program reads in system stdin and process it 
    argparser = argparse.ArgumentParser(description='This script reads log entries as \
system std and parse it for entity linking and further analysis. It parses the \
following logs: AOL, KB.')
    argparser.add_argument('log', type=str,
                    help='choose between "AOL" and "KB"')
 
    argparser.add_argument('action', type=str, 
                    help='choose between "map" and "reduce"')

    argparser.add_argument('-p', '--phase', default=1, choices='1234',
                    help='Choose the processing phase (1, 2, 3, 4), default is 1.\n \
                    #phase 1: parse the log and extract information needed for the final format;\n \
                    #phase 2: parse the log and format it for entity linking;\n \
                    #phase 3: collect results from phase 1 and 2, and format the final result;\n \
                    #phase 4: group output from phase 3 with userid \
                    ')

    args = vars(argparser.parse_args())
    # Make parser
    parser = ParserFactory.createParser(args.get('log') + 'Parser')
    action = args.get('action')
    phase = int(args.get('phase', 1))
    
    if parser and action:
        if action == 'map':
            parser.map(phase)  
        elif action == 'reduce':
            parser.reduce(phase)
    
