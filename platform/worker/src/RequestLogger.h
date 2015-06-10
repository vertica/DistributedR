#ifndef REQUESTLOGGER_H
#define REQUESTLOGGER_H

#include "Observer.h"
#include <string>
#include <google/protobuf/message.h>

using namespace std;

namespace presto {

class RequestLogger : public IObserver<google::protobuf::Message>
{
public:
    explicit RequestLogger(const string &name);
    ~RequestLogger();

    void Update(google::protobuf::Message& message);
private:
    string mName;

};

} //namespace presto


#endif // REQUESTLOGGER_H
