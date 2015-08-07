#! /usr/bin/env python
import sys
import time

print "mem-lock stub, locking %s for %s seconds" % (sys.argv[1], sys.argv[2])
time.sleep(float(sys.argv[2]))
