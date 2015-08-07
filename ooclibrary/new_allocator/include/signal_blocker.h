#ifndef __UTIL_H__
#define __UTIL_H__

#include <signal.h>
#include <string.h>
#include <errno.h>

class signal_blocker{
	sigset_t _mask;
public:
	inline signal_blocker(){
		int ret = sigprocmask(SIG_BLOCK, &_mask, NULL);
		if (ret == -1){
			ERROR_PRINT("signal block failed, error: %s\n", strerror(errno));
		}
	}
	~signal_blocker(){
		int ret = sigprocmask(SIG_UNBLOCK, &_mask, NULL);
		if (ret == -1){
			ERROR_PRINT("signal unblock failed, error: %s\n", strerror(errno));
		}
	}
};


#endif