#include <cpprest/http_client.h>
#include <cpprest/json.h>
 
using namespace web;
using namespace web::http;
using namespace web::http::client;
 
#include <iostream>
using namespace std;
 
void print_json(const json::value & jvalue)
{
   if (!jvalue.is_null())
   {
        cout << jvalue << endl;
   }
}
 
pplx::task<http_response> make_task_request(http_client & client, 
                                            method mtd, 
                                            json::value const & jvalue)
{
   return (mtd == methods::GET || mtd == methods::HEAD) ? 
      client.request(mtd, "/clusters") :
          (mtd == methods::DEL) ?
              client.request(mtd, "/clusters/1") :
                  client.request(mtd, "/clusters", jvalue);
}
 
void make_request(http_client & client, method mtd, json::value const & jvalue)
{
   make_task_request(client, mtd, jvalue)
      .then([](http_response response)
      {
         if (response.status_code() == status_codes::OK)
         {
            return response.extract_json();
         }
         return pplx::task_from_result(json::value());
      })
      .then([](pplx::task<json::value> previousTask)
      {
         try
         {
            print_json(previousTask.get());
         }
         catch (http_exception const & e)
         {
            cout << e.what() << endl;
         }
      })
      .wait();
}

int main()
{
   string uri = "http://localhost:9000";
   http_client client(uri);

   json::value obj;
   obj["name"] = json::value::string("mycluster");

   //POST is not supported
   cout << "\nput values\n";
   make_request(client, methods::PUT, obj);

   cout << "\nget values (GET)\n";
   make_request(client, methods::GET, json::value::null());

   cout << "\nget values (POST)\n";
   make_request(client, methods::POST, obj);

   cout << "\nget values (GET)\n";
   make_request(client, methods::GET, json::value::null());
 
   //delete cluster 1
   cout << "\ndelete values\n";
   make_request(client, methods::DEL, json::value::null());

   cout << "\nget values (GET)\n";
   make_request(client, methods::GET, json::value::null());
 
   return 0;
}
