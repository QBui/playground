#! /user/bin/env python
# -*- coding: latin-1 -*-
import os, sys, commands

# Extract all zip files of Census Data - Summary File 1
source_path = "/home/qbui/census/Summary_File_1"
target_path = "/data/census/extract"

if os.path.isdir(source_path) == False:
   raise ValueError, "source path does not existed"

print "Extracting All Zip Files of Census Data (Summary File 1) from ", repr(source_path)

print "Target Path:", repr(target_path)
if os.path.isdir(target_path) == False:
   print "Recreate target path", target_path
   os.mkdir(target_path)

# Load all sub-directories name into all_states variables
cmd = "ls --ignore=*National* -l " + source_path + " |egrep '^d'|awk '{print $8 }'"
print "Load all states data using command:", repr(cmd)
output = commands.getoutput(cmd)
all_states = output.split("\n")
#print repr(all_states)

# Iterate through each states and extract zip files data
for state in all_states:
   print "Extract all zip files for:", state
   
   # Determine all zip files for this state
   #cmd = "ls -l " + source_path + "/" + state + "/*.zip | awk '{print $8 }'"
   #output = commands.getoutput(cmd)
   #zip_files = output.split("\n")
   #print "\t", repr(zip_files)

   # Ensure the target path exists
   target_subpath = target_path + "/" + state
   if os.path.isdir(target_subpath) == False:
      os.mkdir(target_subpath)

   # Extract all zip files
   # Note: Need to escape * character to prevent * from being expanded by bash shell
   zipfiles_path = source_path + "/" + state + "/\*.zip"
   cmd = "unzip -n " + zipfiles_path + " -d " + target_subpath + "/"
   print "Executing:", cmd
   output = commands.getoutput(cmd)
   print output
      

print "Done."

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
