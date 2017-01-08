"""
Filter entity pairs in query log by:
- number of users
- whether occurs in WP click stream
"""
log="AOL"
datadir = '../../data'
inputdir = '%s/output_%s_statsQueryEt'%(datadir, log)

import itertools as it
import numpy as np
import os

# Load data
def load_data(inputdir):
    data = []
    for filename in os.listdir(inputdir):
        if not filename.startswith('part-'):
            continue
        f = open(inputdir + '/' + filename)
        for line in f:
            strs = line.strip().split('\t')
            data.append((strs[0], int(strs[1]), int(strs[2])))
    return data

data = load_data(inputdir)

user_thresh = 1 
WP_thresh = 0

print 'Number of unique entity pairs in the log:', len(data)
print

u_data = list(it.ifilter(lambda x: x[1]>user_thresh, data))
print 'Number of pairs owned by more than %s users:'%user_thresh,  len(u_data)
print

w_data = list(it.ifilter(lambda x: x[2]>WP_thresh, data))
print 'Number of pairs that occur in WP:', len(w_data)
w_counts = [x[2] for x in w_data]
print 'Max, min, median counts in WP:', max(w_counts), min(w_counts), np.median(w_counts)
print

uw_data = list(it.ifilter(lambda x: x[1] > user_thresh and x[2] > WP_thresh, data))
print 'Number of pairs that occur in WP and owned by more than %s users'%(user_thresh), len(uw_data)
uw_counts = [x[2] for x in uw_data]
print 'Max, min, median counts in WP:', max(uw_counts), min(uw_counts), np.median(uw_counts)


u_counts = [x[1] for x in u_data]
print max(u_counts), np.median(u_counts)
u_data.sort(key=lambda x: x[1], reverse=True)
for x in u_data[0:10]:
    print x

