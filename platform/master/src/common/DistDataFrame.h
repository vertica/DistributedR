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

#ifndef __DIST_DATA_FRAME_H__
#define __DIST_DATA_FRAME_H__

#include "ArrayData.h"
#include "dLogger.h"

namespace presto {
typedef struct {
  int64_t type;
  int64_t size;
  int64_t dims[2];
  StorageLayer store;
} dframe_header_t;

class DistDataFrame : public ArrayData {
 public:
  explicit DistDataFrame(const std::string &name, StorageLayer store=WORKER);
  DistDataFrame(const std::string &name, StorageLayer store, size_t r_size,
      const SEXP sexp, size_t size);
  virtual void LoadInR(RInside &R, const std::string &varname);
  virtual std::pair<std::int64_t, std::int64_t> GetDims() const;
  virtual ~DistDataFrame();

 private:
  dframe_header_t *header;
};
}
#endif
