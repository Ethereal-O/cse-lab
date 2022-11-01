// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  return ret;
}

void extent_client::createLog(uint32_t txid, uint32_t type, extent_protocol::extentid_t &id)
{
  es->createLog(txid, type, id);
}

void extent_client::putLog(uint32_t txid, extent_protocol::extentid_t eid, std::string buf)
{
  int r;
  es->putLog(txid, eid, buf, r);
}

void extent_client::removeLog(uint32_t txid, extent_protocol::extentid_t eid)
{
  int r;
  es->removeLog(txid, eid, r);
}

void extent_client::beginLog(uint32_t txid)
{
  es->beginLog(txid);
}

void extent_client::commitLog(uint32_t txid)
{
  es->commitLog(txid);
}
