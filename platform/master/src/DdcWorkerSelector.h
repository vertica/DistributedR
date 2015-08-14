
#ifndef DDC_WORKERSELECTOR_H_
#define DDC_WORKERSELECTOR_H_

#include <map>
#include <stdexcept>
#include <vector>
#include <boost/shared_ptr.hpp>

namespace ddc {

// chunkId -> worker. E.g.:
// chunk0 -> worker0, chunk1 -> worker1 ...
typedef std::map<int64_t, int32_t> ChunkWorkerMap;

class WorkerInfo {
public:
    explicit WorkerInfo(const std::string& hostname,
                        const uint64_t port,
                        const uint64_t numExecutors)
        : hostname_(hostname),
          port_(port),
          numExecutors_(numExecutors) {
    }

    WorkerInfo() {

    }

    std::string hostname() const {
        return hostname_;
    }
    uint64_t port() const {
        return port_;
    }
    uint64_t numExecutors() const {
        return numExecutors_;
    }

private:
    std::string hostname_;
    uint64_t port_;
    uint64_t numExecutors_;
};


/**
 * Determines which worker processes which chunk.
 */
class WorkerSelector {
 public:
    WorkerSelector() :
        chunkIndex_(0),
        configured_(false)
    {

    }

    int32_t getNextWorker();

    ChunkWorkerMap chunkWorkerMap() const;
    void setChunkWorkerMap(const ChunkWorkerMap &chunkWorkerMap);

private:
    ChunkWorkerMap chunkWorkerMap_;
    uint64_t chunkIndex_;
    bool configured_;
};

}  // namespace ddc

#endif // DDC_WORKERSELECTOR_H_
