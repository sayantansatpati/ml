#!/usr/bin/python

from random import randint
with open('random.txt', 'w') as f:
    for i in xrange(0,10000):
        r_number = randint(0,10000)
        f.write('{0},"NA"\n'.format(r_number))