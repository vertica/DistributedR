from init import *

_logger = logging.getLogger("MAIN")
_logger.setLevel(LEVEL)
_logger.addHandler(STDERR_HANDLER)

def run_binaries(folder, group, binaries, params, concurrent, env):

	for i in params:
		for binary in binaries:
		
			commands = []
			for j in range(concurrent):
				command = CgroupCommand(folder, group, [LOCAL + binary], i, env, suffix=("proc_%d" % j))
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
		print "FATAL: no configuration"
		sys.exit(1)


	conf_script = json.load(open(conf_path))

	for i in range(arg.repeat):
		folder = new_folder()

		for conf in conf_script:
			env = os.environ.copy()

			if "LD_PRELOAD" in conf:
				env["LD_PRELOAD"] = conf["LD_PRELOAD"]

			if arg.ld_preload:
				env["LD_PRELOAD"] = arg.ld_preload

			print("OS environment: %s" % env)

			run_binaries(folder, conf["group"], conf['binaries'], conf["params"], arg.concurrent, env)
