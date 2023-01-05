#!/usr/bin/env python

import sys 
import numpy as np

def mapper(textinput):
    # input comes from STDIN (standard input)
    returnstring = ""
    for line in textinput.splitlines():
        # split the line into words
        #words = line.split()
        try:
            # remove leading and trailing whitespace
            line = line.strip()
            data = np.fromstring(line, dtype=float, sep='\t')
            returnstring = returnstring + ('%s\t%s\n' % (str(data[0]), str(np.average(data[2:]))))
        except:
            # e.g. empty line
            continue
        # increase counters
    print (returnstring)
    return returnstring

mapper(sys.stdin.read())
