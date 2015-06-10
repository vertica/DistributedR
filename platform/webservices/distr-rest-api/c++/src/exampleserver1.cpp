#include <cpprest/http_listener.h>
#include <cpprest/json.h>

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <functional>
#include <vector>
#include <string>
#include <utility>
#include <sstream>
#include <memory>

#define TRACE(msg)            wcout << msg
#define TRACE_ACTION(a, k, v) wcout << a << " (" << k << ", " << v << ")\n"

using namespace std;
using namespace web;
using namespace web::http;
using namespace web::http::experimental::listener;
  

class Cluster {
public:
    explicit Cluster(int id) : id_(id) {

    }
    json::value AsJSON(){
        json::value obj;
        obj["id"] = json::value::number(id_);
        return obj;
    }

private:
    int id_;
};

inline json::value ClustersAsJSON(const map<string, shared_ptr<Cluster> > &clusters)
{
    json::value result = json::value::array();

    size_t idx = 0;
    for (auto cluster = clusters.begin(); cluster != clusters.end(); cluster++)
    {
        result[idx++] = cluster->second->AsJSON();
    }
    return result;
}

// global variables
map<string, shared_ptr<Cluster> > clusters;
int nextId = 1;

/* handlers implementation */
void handle_get(http_request request)
{
   TRACE("\nhandle GET\n");

   ucout <<  request.to_string() << endl;

   auto paths = http::uri::split_path(http::uri::decode(request.relative_uri().path()));
   if (paths.empty())
   {
       request.reply(status_codes::OK, ClustersAsJSON(clusters));
       return;

   }
   string wtable_id = paths[0];
   const string table_id = wtable_id;

   // Get information on a specific cluster.
   auto found = clusters.find(table_id);
   if (found == clusters.end())
   {
       request.reply(status_codes::NotFound);
   }
   else
   {
       request.reply(status_codes::OK, found->second->AsJSON());
   }
}


void handle_post(http_request request)
{
    TRACE("\nhandle POST\n");
    cout <<  request.to_string() << endl;

    auto paths = uri::split_path(uri::decode(request.relative_uri().path()));

    if (paths.empty())
    {
        ostringstream nextIdString;
        nextIdString << nextId;
        shared_ptr<Cluster> cluster = make_shared<Cluster>(nextId);
        clusters[nextIdString.str()] = cluster;
        nextId += 1;

        request.reply(status_codes::OK, cluster->AsJSON());
        return;
    }
    else {
        request.reply(status_codes::BadRequest);
    }
}

void handle_put(http_request request)
{
    TRACE("\nhandle PUT\n");
    cout <<  request.to_string() << endl;

    request.reply(status_codes::MethodNotAllowed);
}

void handle_del(http_request request)
{
   TRACE("\nhandle DEL\n");
   cout <<  request.to_string() << endl;

   auto paths = uri::split_path(uri::decode(request.relative_uri().path()));

   if (paths.empty())
   {
       request.reply(status_codes::Forbidden, "ClusterId is required.");
       return;
   }
   string wtable_id = paths[0];

   const string table_id = wtable_id;

   // Get information on a specific table.
   auto found = clusters.find(table_id);
   if (found == clusters.end())
   {
       request.reply(status_codes::NotFound);
       return;
   }

   auto key = found->first;
   auto cluster = static_pointer_cast<Cluster>(found->second);

   size_t erased_keys = clusters.erase(key);
   if(erased_keys == 0) {
       request.reply(status_codes::NotFound);
       return;
   }
   else {
       request.reply(status_codes::OK);
   }
}

int main()
{
   http_listener listener("http://0.0.0.0:9000/clusters");
 
   listener.support(methods::GET, handle_get);
   listener.support(methods::POST, handle_post);
   listener.support(methods::PUT, handle_put);
   listener.support(methods::DEL, handle_del);
 
   try
   {
      listener
         .open()
         .then([&listener](){TRACE("\nListening ...\nPress ENTER to exit ...\n");})
         .wait();
 
      string line;
      getline(cin, line);
   }
   catch (exception const & e)
   {
      wcout << e.what() << endl;
   }
 
   return 0;
}
