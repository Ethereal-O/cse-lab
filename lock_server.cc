// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server() : nacquire(0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2B part2 code goes here
  std::unique_lock<std::mutex> lck(cv_mtx);
  while (test_and_set_stat(lid, true))
  {
    cv.wait(lck);
  }
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2B part2 code goes here
  set_stat(lid, false);
  cv.notify_all();
  return ret;
}

bool lock_server::test_and_set_stat(lock_protocol::lockid_t lid, bool stat)
{
  // true == lock, false == unlock
  bool ret = false;
  std::map<lock_protocol::lockid_t, bool>::iterator it_ret;
  lock_table_mtx.lock();
  if ((it_ret = lock_table.find(lid)) == lock_table.end())
  {
    lock_table.insert(std::make_pair(lid, false));
    ret = false;
  }
  else
  {
    ret = it_ret->second;
  }
  lock_table[lid] = stat;
  lock_table_mtx.unlock();
  return ret;
}

void lock_server::set_stat(lock_protocol::lockid_t lid, bool stat)
{
  lock_table_mtx.lock();
  lock_table.insert(std::make_pair(lid, false));
  lock_table[lid] = stat;
  lock_table_mtx.unlock();
}
