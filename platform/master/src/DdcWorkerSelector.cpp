#include "DdcWorkerSelector.h"

namespace ddc {

ChunkWorkerMap WorkerSelector::chunkWorkerMap() const
{
    return chunkWorkerMap_;
}

void WorkerSelector::setChunkWorkerMap(const ChunkWorkerMap &chunkWorkerMap)
{
    chunkWorkerMap_ = chunkWorkerMap;
    chunkIndex_ = 0;
    configured_ = true;
}

int32_t WorkerSelector::getNextWorker() {
    if(!configured_) {
        throw std::runtime_error("Need to call setChunkWorkerMap() first.");
    }
    if (chunkWorkerMap_.find(chunkIndex_) == chunkWorkerMap_.end()) {
        // not found
        throw std::runtime_error("Key not found in chunkWorkerMap_");
    }
    int32_t worker = chunkWorkerMap_[chunkIndex_];
    ++chunkIndex_;
    return worker;
}



}  // namespace ddc
