#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 09:56:27 2016

@author: michele
"""
import argparse
import csv
import time
import os

parser = argparse.ArgumentParser()
parser.add_argument("folder", help='relative path to the folder containing the nodes and edges .csv files')
args = parser.parse_args()

t = time.strftime("%d%m%Y_%H%M%S")

DIR = os.path.abspath(args.folder)
NODES_CSV = os.path.join(DIR,'nodes.csv')
EDGES_CSV = os.path.join(DIR,'edges.csv')
OUTPUT_GDF = os.path.join(DIR,'graph.gdf')

def write_file(line):
    with open(OUTPUT_GDF, 'a') as output:
        output.write(line+'\n')
        


with open(NODES_CSV, 'rb') as nodes:
    reader = csv.reader(nodes)
    header = reader.next()
    write_file('nodedef>name VARCHAR,'+' VARCHAR, '.join(header[2:]) +' VARCHAR')
    #print ', '.join(header[1:])
    for row in reader:
        write_file(', '.join('"{}"'.format(i.replace(',',';')) for i in row[1:]))
    nodes.close()
    
with open(EDGES_CSV, 'rb') as edges:
    reader = csv.reader(edges)
    header = reader.next()
    write_file('edgedef>node1 VARCHAR, node2 VARCHAR, '+' VARCHAR, '.join(header[3:-1]) + ' VARCHAR, directed BOOLEAN')
    #print ', '.join(header[1:])
    for row in reader:
        edge_str = ', '.join(i.replace(',',';') for i in row[1:]) #+ ', ' + ', '.join('"{}"'.format(i.replace(',',';')) for i in row[3:-2]) 
        write_file(edge_str)
    edges.close()