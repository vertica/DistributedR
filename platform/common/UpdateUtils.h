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

// Utility functions to comunicate update information
// from executor to workers.

#ifndef _UPDATE_UTILS_
#define _UPDATE_UTILS_

#include <string>
#include <stdio.h>

namespace presto {

/** Append an update message to the file 'out'. Generally, it will be delivered to the worker
 * @param arr_name darray name with the update
 * @param size the size of input darray
 * @param empty if this darray is empty
 * @param out a file descriptor to write. Usually connected to worker using pipe
 * @return NULL
 */
static void AppendUpdate(const string& arr_name, size_t size,
                              bool empty, size_t rdim, size_t cdim, FILE* out) {
  fprintf(out, "%s %zu %d %zu %zu\n", arr_name.c_str(),
      size, empty ? 1: 0, rdim, cdim);
}

/** Append a task result to the file 'out'.
 * @param task_status the result of a task (EXCEPTION or SUCCEED)
 * @param message a meesage to be sent to a worker
 * @param out a file descriptor to write. Usually connected to worker using pipe
 * @return NULL
 */
static void AppendTaskResult(int32_t task_status, const char* message,
    FILE* out) {
  fprintf(out, "& %d 0 0 0\n%s\n", task_status, message);
  fflush(out);  // send the result to worker
}


/** Parse a line from in. If the line corresponds to an update, the
 * arr name, size, offset are filled up. If the line corresponds to a
 * task result, the next line is read to parse the task_msg.
 * @param in a file pointer to read from
 * @param arr_name name of an darray
 * @param size the size of a darray
 * @param empty indicates if a darray is empty
 * @param message message from remote entity
 * @return the value returned from fscanf
 */
static int32_t ParseUpdateLine(FILE* in, char* arr_name,
    size_t* size, int* empty, size_t* rdim, size_t* cdim, char* message) {
  int32_t ret = fscanf(in, " %s %zu %d %zu %zu", arr_name, size, empty, rdim, cdim);

  if (strncmp(arr_name, "&", 100) == 0) {
    // This is a task result. Read the next line to get the message
    int res = fscanf(in, "\n%[^\n]", message);
  }
  return ret;
}

}  // namespace presto

#endif  // _TRANSFER_SERVER_
