/********************************************************************
 *A scalable and high-performance platform for R.
 *Copyright (C) [2013] Hewlett-Packard Development Company, L.P.

 *This program is free software; you can redistribute it and/or modify
 *it under the terms of the GNU General Public License as published by
 *the Free Software Foundation; either version 2 of the License, or (at
 *your option) any later version.

 *This program is distributed in the hope that it will be useful, but
 *WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *General Public License for more details.  You should have received a
 *copy of the GNU General Public License along with this program; if
 *not, write to the Free Software Foundation, Inc., 59 Temple Place,
 *Suite 330, Boston, MA 02111-1307 USA
 ********************************************************************/

/**
 * Helper methods for managing Cgroups to limit resources in case of colocating
 * distributedR with Vertica.
 */
#ifndef __CGROUPS_H_
#define __CGROUPS_H_

#include <libcgroup.h>
#include <sstream>
#include "PrestoException.h"
#include <boost/variant.hpp>

using namespace boost;

namespace presto{

#define DISTRIBUTEDR_CGROUP "distributedr"
#define LIMIT_IN_BYTES "memory.limit_in_bytes"
#define CPUS "cpuset.cpus"
#define CPU_EXCLUSIVE "cpuset.cpu_exclusive"

enum Cgroup_Controller { MEMORY, CPUSET, CPUMODE};

//typedef boost::variant<long long int, std::string, bool> CgroupValue;

/**
 * Initialize Cgroups.
 */
void initialize_cgroup();

/**
 * Attach given process id to distributedR cgroup
 */
void attach_process(pid_t pid);

/**
 * Attach given process id's to distributedR cgroup
 */
void attach_processes(vector<pid_t> pids);

/**
 * Set Cgroup value for either the following:
 * 1. MEMORY
 * 2. CPUSET
 * 3. CPUMODE
 * INPUT: 1. The controller to set.
 *           {MEMORY,CPUSET,CPUMODE}
 *        2. The value to set.
 * Case 1: MEMORY
 *      VALID VALUE TO THE FUNCTION IS MEMORY SPECIFIED IN BYTES.
 * Case 2: CPUSET
 *      Specifies the CPUs that tasks in this cgroup are permitted to access.
 *      This is a comma-separated list, with dashes ("-") to represent ranges.
 *      For example, 0-2,6
 * Case 3: CPUMODE
 *      true: set cpu mode exclusive
 *      false: set cpu mode shared. (by default not exclusive.)
 */
void set_cgroup_value(Cgroup_Controller cgController, boost::variant<long long int, std::string, bool> cgValue);

} // namespace presto

#endif // __CGROUPS_H_
