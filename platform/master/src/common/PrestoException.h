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

#ifndef __PRESTO_EXCEPTION__
#define __PRESTO_EXCEPTION__

#include <string>

#include "dLogger.h"

using namespace std;

namespace presto {
class PrestoWarningException : public std::exception {
  public:
  /** An expression of PrestoWarningException. Usually this exception is intended to be shown an error message
    */
    PrestoWarningException(string m = "PrestoWarningException") : msg(m) {LOG_WARN(m.c_str());}  // NOLINT
    ~PrestoWarningException() throw() {}
    const char* what() const throw() { return msg.c_str(); }

  private:
    string msg;
};

class PrestoShutdownException : public std::exception {
  public:
    /** PrestoShutdownException is thrown when the entire presto session needs to be shutdown
       */
    PrestoShutdownException(string m = "PrestoShutdownException") : msg(m) {LOG_ERROR(m.c_str());}  // NOLINT
    ~PrestoShutdownException() throw() {}
    const char* what() const throw() { return msg.c_str(); }

  private:
    string msg;
};
}
#endif
