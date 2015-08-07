import sys
import pathlib
import json
import re, datetime
import itertools
import IPython

# package log

path = sys.argv[1]
root = pathlib.Path(path)

def read_path(path):
	with open(str(path.absolute())) as stream:
		return stream.read()

def pretty_print(d):
	print json.dumps(d, indent=4, separators=(',', ':'))


class LogLine:

	STAT_RE = re.compile("PID: (?P<pid>\d+), CPU: (?P<cpu>\d+)%, MEM: rss=(?P<rss>\d+), vms=(?P<vms>\d+), shared=(?P<shared>\d+), data=(?P<data>\d+)")
	TIME_FORMAT = "%Y-%m-%d %H:%M:%S,%f"

	def __init__(self, line):
		self.line = line

	def parse_time(self):
		return datetime.datetime.strptime(self.line[:self.line.find("INFO") - 1], LogLine.TIME_FORMAT)

	def is_stat(self):
		return "PID:" in self.line

	def parse_stat(self):
		try:
			# print self.line[self.line.find("PID"):]
			return LogLine.STAT_RE.match(self.line[self.line.find("PID"):]).groupdict()
		except:
			return None

def parse_array(s):
	def remove_quote(s):
		return s[(s.find("\'") + 1):s.rfind("\'")]
	return [remove_quote(token) for token in s.split(",")]

def transform_time(t):
	return "%s.%s" % (t.seconds, t.microseconds / 1000)

def transform(d):
	d["meta"] = parse_array(d["meta"])
	# print d["meta"]
	
	def parse_log(log):
		lines = [LogLine(l) for l in log.split("\n")]
		begin_time = lines[0].parse_time()
		lines = [(transform_time(l.parse_time() - begin_time), l.parse_stat()) for l in lines if l.is_stat()]
		return lines

	d["log"] = parse_log(d["log"])

def fetch_meta(m):
	return {
		"size" : m[7],
		"script" : m[6],
		"group" : m[3],
	}

coll = []

for folder in root.iterdir():
	if not folder.is_dir():
		continue

	entity = {}
	entity["name"] = folder.name

	for f in folder.iterdir():
		entity[f.name] = read_path(f)

	transform(entity)
	entity["meta"] = fetch_meta(entity["meta"])

	coll.append(entity)

coll.sort(key=lambda e:(e["meta"]["script"][-3:]))
coll = [e for e in coll if e["meta"]["script"][-3:] == "f.R"]

coll.sort(key=lambda e:(int(e["meta"]["size"])))
rf_coll = [(e[0], list(e[1])) for e in itertools.groupby(coll, lambda e:(int(e["meta"]["size"])))]

def print_group(group):

	# pretty_print([(e["meta"], e["log"][-1], e["stdout"], e["stderr"]) for e in group]) # if e["meta"]["group"] == "memory:/mem-limit-4096-infswap"])
	for e in group:
		# print "%s+%s, %s, %s, %s" % (e["meta"]["script"], e["meta"]["group"], e["meta"]["size"], e["log"][-1][0], e["stdout"])
		print "%s+%s, %s, %s" % (e["meta"]["script"], e["meta"]["group"], e["meta"]["size"], e["log"][-1][0])

for e in rf_coll:
	print_group(e[1])