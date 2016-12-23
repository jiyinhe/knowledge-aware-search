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

class AOLParser(object):
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

    def read_input(self, file):
        for line in file:
            self.counter += 1
            if line.startswith('AnonID'):
                continue
            yield [self.counter] + line.strip().split('\t')
        
    def map(self, phase=1): 
        records = self.read_input(sys.stdin)
        for record in records:
            if phase == 1:
                self.map_phase1(record)
            elif phase == 2:
                self.map_phase2()
            elif phase == 3:
                self.map_phase3()              

    def map_phase1(self, record):
        i = 0
        key = []
        value = {}
        counter = -1 
        url = [counter, '', '']
        for item in record:
            if i == 0:
                url[0] = item
            if i>0 and i < 4:
                key.append(item)
            elif i == 4:
                url[1] = int(item)
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

    def read_mapper_output(self, file):
        for line in file:
            yield line.strip().rsplit('\t', 1)

    def reduce(self, phase):
        data = self.read_mapper_output(sys.stdin)
        for key, group in it.groupby(data, lambda x: x[0]):
            urls = []
            entry = {}
            # merge the urls clicked for the same query
            for g in group:
                entry = js.loads(g[1])
                urls.append(entry['url'])
            entry['urls'] = urls
            # remove temporary field url
            entry.pop('url')
            # print entry
            print '%s\t%s'%(key, js.dumps(entry))

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

    argparser.add_argument('-p', '--phase', default=1, choices='123', 
                    help='Choose the processing phase (1, 2, or 3), default is 1.\n \
                    #phase 1: parse the log and extract information needed for the final format;\n \
                    #phase 2: parse the log and format it for entity linking;\n \
                    #phase 3: collect results from phase 1 and 2, and format the final result;\n \
                    ')

    args = vars(argparser.parse_args())
    # Make parser
    parser = None
    if args.get('log') == 'AOL':
        parser = AOLParser()
    
    action = args.get('action')
    phase = int(args.get('phase', 1))    
    
    if parser and action:
        if action == 'map':
            parser.map(phase)        
        elif action == 'reduce':
            parser.reduce(phase)
    
