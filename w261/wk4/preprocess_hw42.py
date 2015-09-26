#!/usr/bin/env python
import sys
import os

if len(sys.argv) < 2:
    print "No input file is passed, Aborting!!!"
    sys.exit(1)

input_file = sys.argv[1]
output_file = input_file + '.pp'

try:
    os.remove(output_file)
except OSError:
    pass

last_visitor = None
with open(input_file, 'r') as f1:
    with open(output_file, 'a') as f2:
        for line in f1:
            line = line.strip()
            tokens = line.split(",")
            if len(tokens) == 3 and tokens[0] == 'C':
                last_visitor = tokens[2]

            if len(tokens) == 3 and tokens[0] == 'V':
                out_line = '{0},C,{1}\n'.format(line,last_visitor)
                f2.write(out_line)