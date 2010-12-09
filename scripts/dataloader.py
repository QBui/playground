#! /usr/bin/env python
"""
This script will load census data per state
Usage: python dataloader.py <source_path> <state name>

Options:
   -s ..., --source=...    use specified source path or URL
   -n ..., --name=...      use specified state name
   -h, --help              show this help
   -d                      show debugging information while executing the program

Examples:
   dataloader.py -s /census/data/extract" -n California
   dataloader.py --source=/census/data/extract" --name=California
"""
__author__  = "Quan Bui (quan.bui@gmail.com)"
__version__ = "$Revision: 0.1 $"
__date__ = "$Date: 2010/12/06 12:00:12 $"
__copyright__ = "Copyright (c) 2010 Quan Bui"
__license__ = "Python"

# -*- coding: latin-1 -*-
import os, sys, commands
import getopt
import pymongo
from pymongo import Connection

import datetime
import csv
import json

_debug = 0

class DataImportError(Exception): pass

class CensusDataLoader:
   """ Census Data Loader """
   def __init__(self, source, state_name):      
      self.source = source
      self.state_name = state_name
      if _debug:
         print "Source:", self.source
         print "State Name:", self.state_name

   def load(self):
      print "Loading data for state:", self.state_name
      source_path = self.source + "/" + self.state_name
      cmd = "ls -l " + source_path + "/*[0-9].*| awk '{print $8 }'"
      output = commands.getoutput(cmd)
      files = output.split("\n")
      #print "Data Files:", repr(files)

      # Setup Mongo DB connection
      connection = Connection('localhost', 27017)
      db = connection['census_sf1_2000_raw']      
      collection = db[self.state_name]

      for file in files:
         print "Processing:", file
         #self._load_file_using_csv_lib(collection, file)
         self._load_file(collection, file)
         
   def _load_file(self, collection, file_name):
      infile = open(file_name, "r")         
      while infile:
         line = infile.readline()
         fields = line.split()
         n = len(fields)
         if n == 0:
            #print "Field Count is Zero"
            #print "Line", line
            #print "Fields", fields
            break
         try:
            fields = fields[0].split(",")
            #print "Row", fields
            # Convert fields to tuple     
            identifier = fields[:5]
            if _debug:
               print "Identifier", identifier     

            file_id = identifier[3]
            if _debug:
               print "File ID", file_id

            record_id = int(identifier[-1])
            if _debug:
               print "Record ID", record_id
            
            raw_data = [v for  v in tuple(map(float,fields[5:]))]
            # print "Raw Data", raw_data
            # data_size = len(raw_data)
            # print "Length", data_size

            # Create new entry
            found = collection.find_one({"rid": record_id})

            if (found == None):
               entry = self.build_a_row_rev2(record_id, file_id, identifier, raw_data)            
               if _debug:
                  print "New Entry", entry
               collection.insert(entry)

               # Ensure index on rid
               collection.ensure_index("rid", name="ix_rid")
            else:
               if _debug:
                  print "Update data using FileID:", file_id

               found["data"][str(file_id)] = raw_data
               collection.update({"rid": record_id}, {"$set": {"data":found["data"]}}, upsert=True, safe=True)
            
            #raise DataImportError, "Testing"            
         except Exception as e:
            print "Exception occurs", repr(e)
            #raise e, "Exception occurs"
            pass

        # Top at the first record. Useful for debugging purpose
        # break

   def build_a_row_rev2(self, record_id, file_id, identifier, raw_data):
      entry = {}
      entry["rid"] = record_id
      datamap = {}
      datamap[str(file_id)] = raw_data
      entry["data"] = datamap
#      print entry
      return entry

   def build_a_row(self, fields):
      field_map = dict(zip(xrange(len(fields)), fields))
      entry = {}
      entry["state"] = self.state_name.replace("_", " ")
#      entry["state_code"] = field_map[1]
      entry["creation_date"] = datetime.datetime.utcnow()
      entry["raw_data"] = dict([(str(k), v) for k,v in field_map.items()])
#      print entry
      return entry
   
   def _load_file_using_csv_lib(self, collection, file_name):
      f = open(file_name, 'r')
      fieldnames = ("", "")
      reader = csv.reader(f)
#      out = json.dumps([row for row in reader])
#      print out
      try:
         for row in reader:
            print row
            raise DataImportError, "Testing"
            entry = self.build_a_row(row)
            collection.insert(entry)
      except csv.Error as e:
         sys.exit('file %s, line %d: %s' % (f, reader.line_num, e))

def usage():
   print __doc__

def main(argv):
   source = "/census/data/extract"
   state_name = "California"
   try:
      opts, args = getopt.getopt(argv, "hs:n:d", ["help", "source=", "name="])
   except getopt.GetoptError:
      usage()
      sys.exit(2)
   for opt, arg in opts:
      #print "Argument", opt, arg
      if opt in ("-h", "--help"):
         usage()
         sys.exit()
      elif opt == '-d':
         global _debug
         _debug = 1
      elif opt in ("-s", "--source"):
         source = arg
      elif opt in ("-n", "--name"):
         state_name = arg

   #source = "".join(args)
   #print "Source:", repr(source)

   loader = CensusDataLoader(source, state_name)
   loader.load()


if __name__ == "__main__":
   main(sys.argv[1:])

      
