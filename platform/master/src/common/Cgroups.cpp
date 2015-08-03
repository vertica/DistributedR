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

#include "Cgroups.h"

namespace presto{

void initialize_cgroup(){
    int i;
    i = cgroup_init();
    if(i){
        // Error out
        throw PrestoShutdownException("Error Initializing cgroup.");
    }
    return;
}

void attach_process(pid_t pid){
    struct cgroup *distributedr = cgroup_new_cgroup(DISTRIBUTEDR_CGROUP);

    if(!distributedr){
        throw PrestoShutdownException("Could not create distributedR cgroup.");
    }

    int i = cgroup_get_cgroup(distributedr);
    if(i){
        cgroup_free(&distributedr);
        throw PrestoShutdownException("Could not find distributedr control group. Please run the helper installer script distributedr_enable_colocation again to create the distributedr control group.");
    }

    i = cgroup_attach_task_pid(distributedr,pid);
    if(i){
        cgroup_free(&distributedr);
        throw PrestoShutdownException("Could not attach process to distributedr control group.");
    }
    cgroup_free(&distributedr);
    return;
}

void attach_processes(vector<pid_t> pids){
    pids.push_back(getpid());

    std::vector<pid_t>::size_type size = pids.size();
    for(unsigned j = 0; j < size; j++){
        attach_process(pids[j]);
    }
    return;
}

void set_cgroup_value(Cgroup_Controller cgController, boost::variant<long long int, std::string, bool> cgValue){

    int i;

    // STEP 1: CREATE REFERENCE DISTRIBUTEDR CGROUP.
    struct cgroup *distributedr = cgroup_new_cgroup(DISTRIBUTEDR_CGROUP);
    if(!distributedr){
        throw PrestoShutdownException("Could not create distributedr control group.");
    }

    switch(cgController){
    case MEMORY:
    {
        long long int& mem = boost::get<long long int>(cgValue);
        std::stringstream str_mem;
        str_mem << mem;

        // STEP 2: adding memory controller to distributed group if not already added.
        struct cgroup_controller *memory = cgroup_get_controller(distributedr, "memory");
        if(!memory){
            memory = cgroup_add_controller(distributedr, "memory");

            // error check. if still cannot add controller, throw error.
            if(!memory){
                cgroup_free(&distributedr);
                throw PrestoShutdownException("Could not add memory controller to distributedr control group.");
            }
        }

        // STEP 3: setting the limit passed in the function.
        i = cgroup_add_value_string(memory, LIMIT_IN_BYTES, str_mem.str().c_str());
        if(i){
            cgroup_free(&distributedr);
            throw PrestoShutdownException("Could not add memory limit to distributedr control group.");
        }
        break;
    }
    case CPUSET:
    {
        std::string& cpu_set = boost::get<std::string>(cgValue);

        // STEP 2: adding cpu controller to distributed group if not already added.
        struct cgroup_controller *cpuset = cgroup_get_controller(distributedr, "cpuset");
        if(!cpuset){
            cpuset = cgroup_add_controller(distributedr, "cpuset");

            // error check. if still cannot add controller, throw error.
            if(!cpuset){
                cgroup_free(&distributedr);
                throw PrestoShutdownException("Could not add cpuset controller to distributedr control group.");
            }
        }

        // STEP 3: setting the limit passed in the function.
        i = cgroup_add_value_string(cpuset, CPUS, cpu_set.c_str());
        if(i){
            cgroup_free(&distributedr);
            throw PrestoShutdownException("Could not apply cpu mapping to distributedr control group.");
        }
        break;
    }
    case CPUMODE:
    {
        bool& cpu_mode = boost::get<bool>(cgValue);

        // STEP 2: adding cpu controller to distributed group if not already added.
        struct cgroup_controller *cpuset = cgroup_get_controller(distributedr, "cpuset");
        if(!cpuset){
            cpuset = cgroup_add_controller(distributedr, "cpuset");

            // error check. if still cannot add controller, throw error.
            if(!cpuset){
                cgroup_free(&distributedr);
                throw PrestoShutdownException("Could not add cpuset controller to distributedr control group.");
            }
        }

        // STEP 3: setting the limit passed in the function.
        int mode_value = cpu_mode? 1 : 0;
        i = cgroup_add_value_int64(cpuset, CPU_EXCLUSIVE, mode_value);
        if(i){
            cgroup_free(&distributedr);
            throw PrestoShutdownException("Could not apply cpu mode to distributedr control group.");
        }
        break;
    }
    default:
        break;
    }

    // STEP 4: create the final distributedr cgroup
    struct cgroup *final_distributedr = cgroup_new_cgroup(DISTRIBUTEDR_CGROUP);
    if(!final_distributedr){
        cgroup_free(&final_distributedr);
        throw PrestoShutdownException("Could not create distributedR cgroup.");
    }

    // STEP 5: COPY the refernce distributedr cgroup to final distributedr cgroup.
    i = cgroup_copy_cgroup(final_distributedr, distributedr);
    if(i){
        cgroup_free(&final_distributedr);
        throw PrestoShutdownException("Could not copy refernce cgroup to final cgroup");
    }


    // STEP 6: update the kernel level cgroup
    i = cgroup_modify_cgroup(final_distributedr);
    if(i){
        cgroup_free(&final_distributedr);
        throw PrestoShutdownException("Could not modify the cpu mode for distributedr control group.");
    }
    return;

}


}
