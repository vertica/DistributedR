import pdb
from subprocess import *
from os.path import expanduser
home = expanduser("~")
output = Popen(["find","%s/.m2/"%(home),"-name","*.jar"], stdout=PIPE).communicate()[0]
print ':'.join(output.split('\n')) + '.'
