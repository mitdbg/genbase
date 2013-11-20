#!/usr/bin/python

import os
import sys

def main(argv):
  format_data_for_hive(argv[1], argv[2] + os.path.basename(argv[1]))

def format_data_for_hive(filename, out_file):
  f = open(filename, 'r')
  fout = open(out_file, 'w')
  first = True
  for line in f:
    if first:
      first = False
    else:
      parts = line.split(', ')
      fout.write(','.join(parts))

if __name__ == "__main__":
  main(sys.argv)
