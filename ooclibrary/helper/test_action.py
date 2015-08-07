from init import *
from action_generate import *

BIN_CONF = [
	("test_access_performance-nohook-notest-nomadv", "mem-limit-4096-infswap"),

	("test_access_performance-hooked-notest-nomadv", "mem-limit-4096-noswap"),
	("test_access_performance-hooked-notest-advseq", "mem-limit-4096-noswap"),
	("test_access_performance-hooked-notest-advrnd", "mem-limit-4096-noswap"),
	
	("test_access_performance-hooked-notest-nomadv", "mem-limit-4096-infswap"),
	("test_access_performance-hooked-notest-advseq", "mem-limit-4096-infswap"),
	("test_access_performance-hooked-notest-advrnd", "mem-limit-4096-infswap"),
	]


def run_test(folder, group, binary, param):

	command = CgroupCommand(folder, group, binary, param)
	logger = command.logger

	command.run()
	command.watch_usage_till_finish()
	clean_group(group)

if __name__ == '__main__':
	init_copy(REMOTE, LOCAL)

	for i in xrange(1000):

		folder = new_folder()
		script = folder / "script"
		script.touch()

		with open(str(script.absolute()), "w") as stream:
			actions = generate_action([1,1,1,1,5,5], 10)
			_logger.info(actions)
			stream.write(actions)

		for bin_name, group in BIN_CONF:

			run_test(folder, group, [LOCAL + bin_name], [str(script.absolute())])
