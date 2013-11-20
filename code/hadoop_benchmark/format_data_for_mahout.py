#!/usr/bin/python

import os
import sys

def main(argv):
  format_data_for_mahout(argv[1], argv[2] + os.path.basename(argv[1]) + '_mahout')

def format_data_for_mahout(filename, out_file):
  f = open(filename, 'r')
  fout = open(out_file, 'w')
  first = True
  for line in f:
    if first:
      first = False
    else:
      parts = line.split()
      parts[2] = "{0:.2f}".format(float(parts[2])) # special because original #s have precision of only 2 places after decimal
      fout.write(','.join(parts)+'\n')

if __name__ == "__main__":
  main(sys.argv)
