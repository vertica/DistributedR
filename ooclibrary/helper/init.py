import psutil
import subprocess
import sys
import tempfile
import time
import pathlib
import threading
import logging
import Queue
import json
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--debug")
parser.add_argument("--conf")
parser.add_argument("--repeat", type=int, default=1)
parser.add_argument("--ld_preload")
parser.add_argument("--clean_folder")
parser.add_argument("--concurrent", type=int, default=1)
parser.add_argument("--log_all", action="store_true")
arg = parser.parse_args()

if arg.debug:
	DEBUG = True
else:
	DEBUG = 		False

REMOTE = 		"/home/zhenche/test/"
LOCAL = 		"/tmp/test/"
OUT_FOLDER = 	"/tmp/logs/"


if DEBUG:
	EXE_PREFIX = 	["/tmp/a.py"]
else:
	EXE_PREFIX = 	["/usr/bin/time"]

R_COMMAND = 	["Rscript", "--vanilla"]

LEVEL = 		logging.DEBUG

MEM_WATCH_PERIOD = 	5



FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
STDERR_HANDLER = logging.StreamHandler(sys.stderr)
STDERR_HANDLER.setFormatter(FORMATTER)

ENV = os.environ.copy()

_logger = logging.getLogger("MAIN")
_logger.setLevel(LEVEL)
_logger.addHandler(STDERR_HANDLER)


def init_copy(src, dst):
	subprocess.call(["rm", "-r", dst])
	subprocess.call(["cp", "-r", src, dst])

def clean_folder():
	if arg.clean_folder:
		subprocess.call("rm %s/*" % arg.clean_folder, shell=True)

def touch_and_open(path, name, flag="w"):
	p = path / name
	p.touch()
	return open(str(p.absolute()), flag)

def new_folder():
	start_at = int(time.time() * 1000)
	folder = pathlib.Path(OUT_FOLDER) / str(start_at)
	folder.mkdir()
	return folder

def clean_group(group):
	pass

trans = ''.join(chr(c) if chr(c).isupper() or chr(c).islower() or chr(c).isdigit() else '_' for c in range(256))

def escape_everything(s):

	return "_".join([str(i).translate(trans).upper() for i in s])

class CgroupCommand:


	def cgexec_params(self, group):
		CGEXEC = ["cgexec", "-g", "memory:/%s" % group]
		return CGEXEC

	def get_logger(self):
		if hasattr(self, "logger"):
			return self.logger

		logger = logging.getLogger(self.name)
		file_handler = logging.StreamHandler(self.external_log)
		file_handler.setFormatter(FORMATTER)
		logger.addHandler(file_handler)
		logger.addHandler(STDERR_HANDLER)
		logger.setLevel(LEVEL)

		return logger

	def __init__(self, folder, group, command, args, env, suffix=""):
		args = [str(s) for s in args]


		self.name = escape_everything([group] + command + args + [suffix])
		self.args = EXE_PREFIX + self.cgexec_params(group) + command + args

		self.folder = folder / self.name
		self.folder.mkdir()

		self.meta = touch_and_open(self.folder, "meta")
		self.meta.write("%s" % (self.args, ) )
		self.meta.write("\n")
		self.meta.close()
		
		if arg.log_all:
			self.stdout = touch_and_open(self.folder, "log", "a")
			self.stderr = self.stdout
			self.external_log = self.stdout
		else:
			self.stdout = touch_and_open(self.folder, "stdout")
			self.stderr = touch_and_open(self.folder, "stderr")
			self.external_log = touch_and_open(self.folder, "log")

		self.env = env

		self.logger = self.get_logger()


	def __str__(self):
		return self.name

	def run(self):
		self.logger.info("Running command %s" % (self.args, ))
		self.logger.info("Under folder %s" % self.folder)
		clean_folder()
		self.process = subprocess.Popen(self.args, stdout=self.stdout, stderr=self.stderr, env=self.env)
		self.process_info = psutil.Process(self.process.pid)

	def print_process_info(self, p):
		cpu_percent = p.cpu_percent()
		if cpu_percent < 3:
			return
		mem_info = p.memory_info_ex()
		self.logger.info("PID: %d, CPU: %d%%, MEM: rss=%d vms=%d shared=%d data=%d", p.pid, cpu_percent, mem_info.rss, mem_info.vms, mem_info.shared, mem_info.data)
	
	def finished(self):
		if hasattr(self, "_finished"):
			return True
		if self.process.poll() != None:
			self._finished = True
			return True
		return False

	def print_resource_usage(self):

		if self.finished():
			return

		self.print_process_info(self.process_info)
		for p in self.process_info.children(True):
			self.print_process_info(p)


def run_R_script(folder, group, script, params, concurrent, env):

	for i in params:
		
		commands = []
		for j in range(concurrent):
			command = CgroupCommand(folder, group, R_COMMAND, [script, i], env, suffix=("proc_%d" % j))
			commands.append(command)

		for command in commands:
			command.run()
		
		while not all([command.finished() for command in commands]):
			for command in commands:
				command.print_resource_usage()
			time.sleep(MEM_WATCH_PERIOD)
		
		_logger.info("All task finished, return codes: %s" % [command.process.returncode for command in commands])
		clean_group(group)

if __name__ == '__main__':
	init_copy(REMOTE, LOCAL)
	if arg.conf:
		conf_path = arg.conf
	else:
		conf_path = "/tmp/conf.json"


	conf_script = json.load(open(conf_path))

	for i in range(arg.repeat):
		folder = new_folder()

		for conf in conf_script:
			env = os.environ.copy()

			if arg.ld_preload:
				env["LD_PRELOAD"] = arg.ld_preload

			if "LD_PRELOAD" in conf:
				env["LD_PRELOAD"] = conf["LD_PRELOAD"]

			print("OS environment: %s" % env)

			run_R_script(folder, conf["group"], LOCAL + conf["script"], conf["params"], arg.concurrent, env)
