#!/usr/bin/env python
#Reducer.py
import sys

year_temp = {}

def reducer(textinput):
    #Partitoner
    returnstring = "\n"
    year_temp = {}
    
    for line in textinput.splitlines():
        try:
            line = line.strip()
            year, temp = line.split('\t')
            if year in year_temp:
                year_temp[year].append(float(temp))
            else:
                year_temp[year] = []
                year_temp[year].append(float(temp))
        except:
            #empty line
            continue

    #Reducer
    for year in year_temp.keys():
        ave_temp = sum(year_temp[year])*1.0 / len(year_temp[year])
        print('%s\t%s'% (year, ave_temp))
        returnstring += ('%s\t%s\n' % (year, ave_temp))
        
    return returnstring
        
reducer(sys.stdin.read())
