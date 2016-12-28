#!/usr/bin/env python

"""
This class computes the WP click probabilities using
the WP click stream data. 
It computes two stats: 
- transition probability: p(c2|c1) where c1 and c2 are WP pages
- stationary probability: p(c2) -- likelihood that a user would 
arrive at c2 -- compute over all types of transitions

Input:
- stream of click data (from single file or from all files over time)

Output:
{c2: {'Ptr': [{c1: p(c2|c1)}...], 'Ps': p(c2)}}

P(c2|c1) = #(c1 -> c2)/# (c1) 
P(c2) = #(c2)/#all clicks
"""
import argparse
import sys
import simplejson as js
import itertools as it

class WPClicks(object):
    def read_input(self, file):
        for line in file:
            # Filter out header lines
            if line.startswith('prev'):
                continue
            yield line.rstrip().split('\t')
        
    def read_mapper_output(self, file, delimiter='\t'):
        for line in file:
            yield line.strip().split(delimiter)

    def map(self, stats, totcount=None):
        data = self.read_input(sys.stdin)
        for line in data:
            c1 = ''
            c2 = ''
            count = 0
            # 2015-01: prev_id, curr_id, count, prev_title, curr_title
            if len(line) == 5:
                c1 = line[3]
                c2 = line[4]
                count = int(line[2])
            # 2015-02: prev_id, curre_id, count, prev_title, curr_title, type
            elif len(line) == 6:
                c1 = line[3]
                c2 = line[4]
                count = int(line[2])
            # 2016: prev_title, current_title, type, count 
            elif len(line) == 4:
                c1 = line[0]
                c2 = line[1]
                count = int(line[3])
            if stats == 'tot':
                # Stationary probability: how likely that a user arrives at c2
                print 'key\t%s'%(count)
            elif stats == 'sta':
                print '%s\t%s'%(c2, count)
            elif stats == 'tra':
                # Only consider within WP transition
                if not c1.startswith('other'):
                    print '%s\t%s\t%s'%(c1, c2, count)

    def reduce(self, stats, totcount=None):
        data = self.read_mapper_output(sys.stdin)
        if stats == 'tot':
            print sum([int(c[1]) for c in data])
        elif stats == 'sta':
            for c2, group in it.groupby(data, lambda x: x[0]):
            # For stationary probability, group over c2
                counts = sum([int(g[1]) for g in group])
                prob = float(counts)/totcount
                print '%s\t%s\t%s'%(c2, counts, prob) 
        elif stats == 'tra':
            for c1, group in it.groupby(data, lambda x: x[0]):
                group = list(group)
                sum_c1 = sum([int(g[2]) for g in group]) 
                entries = [{g[1]:[int(g[2]), float(g[2])/sum_c1]}
                        for g in group
                    ]
                print '%s\t%s'%(c1, js.dumps(entries))
 

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='This script computes \
        transition and stataionary probaiblities of WP concept click stream.')
 
    argparser.add_argument('action', type=str, 
                    help='choose between "map" and "reduce"')
    argparser.add_argument('stats', type=str, 
                    help='options: [tot|sta|tra]\n \
                        "tot" (sum of all click counts);\n \
                        "sta": sum(c2 counts)/tot;\n \
                        "tra": sum(c2 counts)/sum(c1 counts) \
                    ')
    argparser.add_argument('-tot', type=int, 
                    help='options: suply the total number of counts for computing stationary probability')
   
    args = vars(argparser.parse_args())
    action = args.get('action')
    stats = args.get('stats')
    tot = args.get('tot', None)
    if stats == 'sta' and not tot:
        print "For option sta, -tot (total number of click count) should be provided."
        print 
        sys.exit()
 
    wpc = WPClicks()
    if action == 'map':
        wpc.map(stats, totcount=tot) 
    elif action == 'reduce':
        wpc.reduce(stats, totcount=tot)


