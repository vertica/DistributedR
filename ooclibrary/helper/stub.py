#! /usr/bin/env python
import sys

print sys.argv

print >> sys.stderr, "STDERR"
print >> sys.stderr, sys.argv
