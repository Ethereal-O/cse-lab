// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unordered_map>

#include "extent_server.h"
#include "persister.h"

// #define LAB2_PART1

extent_server::extent_server()
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  isRecovering = true;
  _persister->restore_logdata();

  // to check if the transaction is commited
  std::unordered_map<uint32_t, bool> is_commited;
  for (auto log : _persister->log_entries)
    if (log.type == chfs_command::CMD_COMMIT)
      is_commited.insert(std::make_pair(log.id, true));

  for (auto log : _persister->log_entries)
  {
    // if the transaction isn't commited, continue
    if (is_commited.find(log.id) == is_commited.end())
      continue;

    extent_protocol::extentid_t id;
    int tmp;
    switch (log.type)
    {
    case chfs_command::CMD_CREATE:
      std::cout << "recreating " << *(uint32_t *)log.parameters.substr(0, sizeof(uint32_t)).data() << std::endl;
      create(*(uint32_t *)log.parameters.substr(0, sizeof(uint32_t)).data(), id);
      break;
    case chfs_command::CMD_PUT:
      std::cout << "reputing " << *(extent_protocol::extentid_t *)log.parameters.substr(0, sizeof(extent_protocol::extentid_t)).data() << std::endl;
      put(*(extent_protocol::extentid_t *)log.parameters.substr(0, sizeof(extent_protocol::extentid_t)).data(), log.parameters.substr(sizeof(extent_protocol::extentid_t)), tmp);
      break;
    case chfs_command::CMD_REMOVE:
      std::cout << "reremoving " << *(extent_protocol::extentid_t *)log.parameters.data() << std::endl;
      remove(*(extent_protocol::extentid_t *)log.parameters.data(), tmp);
      break;
    default:
      break;
    }
  }
  isRecovering = false;
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

#ifdef LAB2_PART1
  // log
  if (!isRecovering)
  {
    chfs_command log(chfs_command::CMD_CREATE, 0, std::string((char *)&type, sizeof(type)));
    _persister->append_log(log);
  }
#endif

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;

  const char *cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

#ifdef LAB2_PART1
  // log
  if (!isRecovering)
  {
    chfs_command log(chfs_command::CMD_PUT, 0, std::string((char *)&id, sizeof(id)) + buf);
    _persister->append_log(log);
  }
#endif

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else
  {
    buf.assign(cbuf, size);
    free(cbuf);
  }

#ifdef LAB2_PART1
  // log
  if (!isRecovering)
  {
    chfs_command log(chfs_command::CMD_GET, 0, std::string((char *)&id, sizeof(id)) + buf);
    _persister->append_log(log);
  }
#endif

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;

  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

#ifdef LAB2_PART1
  // log
  if (!isRecovering)
  {
    chfs_command log(chfs_command::CMD_GETADDR, 0, std::string((char *)&id, sizeof(id)) + std::string((char *)&a, sizeof(a)));
    _persister->append_log(log);
  }
#endif

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  im->remove_file(id);

#ifdef LAB2_PART1
  // log
  if (!isRecovering)
  {
    chfs_command log(chfs_command::CMD_REMOVE, 0, std::string((char *)&id, sizeof(id)));
    _persister->append_log(log);
  }
#endif

  return extent_protocol::OK;
}

void extent_server::createLog(uint32_t txid, uint32_t type, extent_protocol::extentid_t &id)
{
  chfs_command log(chfs_command::CMD_CREATE, txid, std::string((char *)&type, sizeof(type)));
  _persister->append_log(log);
}

void extent_server::putLog(uint32_t txid, extent_protocol::extentid_t id, std::string buf, int &)
{
  chfs_command log(chfs_command::CMD_PUT, txid, std::string((char *)&id, sizeof(id)) + buf);
  _persister->append_log(log);
}

void extent_server::removeLog(uint32_t txid, extent_protocol::extentid_t id, int &)
{
  chfs_command log(chfs_command::CMD_REMOVE, txid, std::string((char *)&id, sizeof(id)));
  _persister->append_log(log);
}

void extent_server::beginLog(uint32_t txid)
{
  chfs_command log(chfs_command::CMD_BEGIN, txid, std::string(""));
  _persister->append_log(log);
}

void extent_server::commitLog(uint32_t txid)
{
  chfs_command log(chfs_command::CMD_COMMIT, txid, std::string(""));
  _persister->append_log(log);
}