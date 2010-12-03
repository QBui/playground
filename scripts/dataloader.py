#! /user/bin/env python
# -*- coding: latin-1 -*-
import os, sys

# Loading Census Data - Summary File 1
source_path = "/home/qbui/census/Summary_File_1"

if os.path.isdir(source_path) == False:
   raise ValueError, "source path does not existed"

print "Loading Census Data (Summary File 1) from ", repr(source_path)

#all_states = 

# Get the Data
#for file in FILES:
#   infile = open(file, "r")
#   while infile:
#      line = infile.readline()
#      fields = line.split()
#      n = len(fields)
#      if n == 0:
#         break
#      try:
#         # perform some data operation
#      except:
#         pass
#
