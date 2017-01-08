"""
make plots from the descriptive statistics
"""
import simplejson as js
import os
#import pylab
import numpy as np
import itertools as it
from scipy.stats import ranksums

inputdir='../../data/output_AOL_preanalysis_descstats/'

keys = ["#change_q", "#w_et", "#w_click","#change_e","#change_c", "#q", "#not_urls"]
def load_data(inputdir):
    #data = {'#q': [],
    #    '#not_urls': [],
    #    '#w_click': [],
    #    '#w_et': [],
    #    '#change_q': [],
    #    '#change_c': [],
    #    '#change_e':[]
    #    }
    data = []
    for filename in os.listdir(inputdir):
        if not filename.startswith('part-'):
            continue
        f = open(inputdir + '/' + filename)
        for line in f:
            o = js.loads(line)
            data.append(o)
    return data

def filter_data(data): 
    print 
    print "Total number of users:", len(data)

    num_q = sorted([x['#q'] for x in data], reverse=True)
    print 'Number of queries per user:'
    print 'Max: %s'%max(num_q), 
    print 'Min: %s'%min(num_q),
    print 'Top 10: ', num_q[0:10]
    

    # users should have at least 2 new queries
    thresh = 2  
    data = list(it.ifilter(lambda x: x['#q'] >= thresh, data))
    print "1. Number of users issued at least %s queries:"%thresh, len(data)
    # Among these, filter out users that only do direct url input
    data = list(it.ifilter(lambda x: x['#not_urls']>=thresh, data))
    print "2. Among those, after filtering users that only do direct url input:", len(data)
 
    data1 = list(it.ifilter(lambda x: x['#w_click']>0, data))
    print "3. Filtered with 1 & 2, users that have at least one query with clicks:", len(data1)

    data2 = list(it.ifilter(lambda x: x['#w_et']>0, data))
    print "4. Filtered with 1 & 2, users that have at least one query linked with entity:", len(data2)

    data3 = list(it.ifilter(lambda x: x['#change_q']>0, data))
    print "5. Filtered with 1 & 2, users that have at least one set of queries that \
share words but are not identical:", len(data3)

    data4 = list(it.ifilter(lambda x: x['#change_c']>0, data))
    print "6. Filtered with 1 & 2, users that have at least one set of queries that \
are identical but with different clicked results:", len(data4)
    
    data5 = list(it.ifilter(lambda x: x['#change_e']>0, data))
    print "7. Filtered with 1 & 2, users that have at least one set of queries that \
are different but share the same entities:", len(data5)

    data6 = list(it.ifilter(lambda x: x['#change_q']>0 or x['#change_c']>0
            or x['#change_e'] > 0, data))
    print "8. Union of 5, 6, 7", len(data6)

    data7 = list(it.ifilterfalse(lambda x: x['#change_q']>0 or x['#change_c']>0
            or x['#change_e'] > 0, data))

    print "9. Not 5, 6, 7", len(data7)

    count_q6 = [q['#q'] for q in data6]
    count_q7 = [q['#q'] for q in data7]
    print "Mean(sd) of the number of queries - 8 vs 9:", 
    print np.mean(count_q6), '(%s)'%np.std(count_q6),
    print np.mean(count_q7), '(%s)'%np.std(count_q7)
    w, p = ranksums(count_q6, count_q7)
    print 'Ranksum test:', w, 'p-value:', p
    
    count_q6 = [q['#not_urls'] for q in data6]
    count_q7 = [q['#not_urls'] for q in data7]
    print "Mean(sd) of the number of non-url queries - 8 vs 9:", 
    print np.mean(count_q6), '(%s)'%np.std(count_q6),
    print np.mean(count_q7), '(%s)'%np.std(count_q7)
    w, p = ranksums(count_q6, count_q7)
    print 'Ranksum test:', w, 'p-value:', p
    print
    
 
def plot_distributions(data):
    pass
    #for key in data:
    #    plot_name = key + '.png'
    #    d = data[key]
    #    print "Stats:", key
    #    print "max:", max(d), "min:", min(d), "median:", np.median(d)
    #    print
       
    # Filter data
    # User should have at least 2 new queries
#    idx_q = [[i, data['#q'][i]] for i in range(len(data['#q']))]
#    filter_q = it.ifilter(lambda x: idx_q[1]>1, idx_q)
    # Among those, filter out users that only has direct url inputs
        
 
     
    
#        pylab.figure()
#        pylab.hist(d)
#        pylab.title(key)
#    pylab.show()
#    print "Total number of users:", len(data[key])

if __name__ == '__main__':
    data = load_data(inputdir) 
    filter_data(data)
    plot_distributions(data)




