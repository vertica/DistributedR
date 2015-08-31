#include "RequestLogger.h"

#include <iostream>
#include <algorithm>
#include <google/protobuf/text_format.h>

#include "dLogger.h"

namespace presto {


RequestLogger::RequestLogger(const string &name) : mName(name)
{
}

RequestLogger::~RequestLogger()
{
}

void RequestLogger::Update(google::protobuf::Message& message)
{
    string message_text;
    google::protobuf::TextFormat::PrintToString(message, &message_text);

    LOG_DEBUG("### begin %s", mName.c_str());
    // max log message size is 1024, platform/common/dLogger.h
    // split in chunks
    // choose 768 to be on the safe side
    unsigned int MAX_LOG_SIZE = 768;
    for(unsigned int i = 0; i < message_text.size(); i+= MAX_LOG_SIZE) {
        unsigned int chunk_size = min(static_cast<unsigned int>(message_text.size()) - i, MAX_LOG_SIZE);
        LOG_DEBUG("%s", message_text.substr(i, chunk_size).c_str());
    }
    LOG_DEBUG("### end %s", mName.c_str());
}

} //namespace presto
