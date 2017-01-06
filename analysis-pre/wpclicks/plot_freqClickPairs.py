"""
Plot the distribution of frequencies of 
entity pairs clicked in the WP clickstream data

The data is aggregated over all WP clickstream dumps
"""
import pylab
import numpy as np
import itertools as it
import matplotlib as mp

font = {'size'   : 16}
mp.rc('font', **font)

inputfile='../res/output_freqClickPairs.txt'
outputdir='../plots'
outputfile = 'freqClickPairs.png'

output = '%s/%s'%(outputdir, outputfile)

f = open(inputfile)
data = []
for line in f:
    number, count = line.strip().split('\t')
    data.append((int(number), int(count)))
f.close() 

data.sort(key=lambda x: x[0])
X = [x[0] for x in data]
Xlog = np.log(X)
Y = [x[1] for x in data]
#Ysum = sum(Y)
#Ynorm = [float(y)/Ysum for y in Y]
Ylog = np.log(Y)

pylab.plot(Xlog, Ylog, '.')

datamap = dict(data)
Xticklabels = [10, 50, 200, 1000, 5000, 50000]
Xticks = np.log(Xticklabels)

#Yticklabels = [1, 10, 100, 1000, 10000, 100000, 1000000]
#Yticks = np.log(Yticklabels)

pylab.xticks(Xticks, Xticklabels)
#pylab.yticks(Yticks, Yticklabels)
#pylab.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

pylab.xlabel('Counts of transitions between WP pages')
pylab.ylabel('Log (Frequencies of the counts)')

pylab.show()

pylab.savefig(output)




