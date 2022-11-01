// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>
#include <mutex>
#include <condition_variable>

class lock_server
{

private:
  std::map<lock_protocol::lockid_t, bool> lock_table;
  std::mutex cv_mtx, lock_table_mtx;
  std::condition_variable cv;

  bool test_and_set_stat(lock_protocol::lockid_t lid, bool stat);
  void set_stat(lock_protocol::lockid_t lid, bool stat);

protected:
  int nacquire;

public:
  lock_server();
  ~lock_server(){};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif