#include "extent_server_dist.h"

// #define DEBUG_PART5
chfs_raft *extent_server_dist::leader() const
{
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0)
    {
        return this->raft_group->nodes[0];
    }
    else
    {
        return this->raft_group->nodes[leader];
    }
}

#define wait_time 1000

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id)
{
    // Lab3: your code here
#ifdef DEBUG_PART5
    printf("extent_server_dist::create %d\n", type);
#endif
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::command_type::CMD_CRT;
    cmd.type = type;
    int temp_idx, temp_term;
    bool ret = this->leader()->new_command(cmd, temp_term, temp_idx);
    if (ret)
    {
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        cmd.res->start = std::chrono::system_clock::now();
        if (!cmd.res->done)
        {
            ASSERT(
                cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::milliseconds(wait_time)) == std::cv_status::no_timeout,
                "extent_server_dist::create command timeout");
        }
        id = cmd.res->id;
    }
#ifdef DEBUG_PART5
    printf("extent_server_dist::create return %d\n", id);
#endif
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    // Lab3: your code here
#ifdef DEBUG_PART5
    printf("extent_server_dist::put %d %s\n", id, buf.c_str());
#endif
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::command_type::CMD_PUT;
    cmd.id = id;
    cmd.buf = buf;
    int temp_idx, temp_term;
    bool ret = this->leader()->new_command(cmd, temp_term, temp_idx);
    if (ret)
    {
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        cmd.res->start = std::chrono::system_clock::now();
        if (!cmd.res->done)
        {
            ASSERT(
                cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::milliseconds(wait_time)) == std::cv_status::no_timeout,
                "extent_server_dist::put command timeout");
        }
    }
#ifdef DEBUG_PART5
    printf("extent_server_dist::put return %d\n", cmd.res->done);
#endif
printf("extent_server_dist::put return %d\n", cmd.res->done);
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf)
{
    // Lab3: your code here
#ifdef DEBUG_PART5
    printf("extent_server_dist::get %d\n", id);
#endif
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::command_type::CMD_GET;
    cmd.id = id;
    int temp_idx, temp_term;
    bool ret = this->leader()->new_command(cmd, temp_term, temp_idx);
    if (ret)
    {
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        cmd.res->start = std::chrono::system_clock::now();
        if (!cmd.res->done)
        {
            ASSERT(
                cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::milliseconds(wait_time)) == std::cv_status::no_timeout,
                "extent_server_dist::get command timeout");
        }
        buf = cmd.res->buf;
    }
#ifdef DEBUG_PART5
    printf("extent_server_dist::get return %d\n", cmd.res->done);
#endif
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    // Lab3: your code here
#ifdef DEBUG_PART5
    printf("extent_server_dist::getattr %d\n", id);
#endif
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::command_type::CMD_GETA;
    cmd.id = id;
    int temp_idx, temp_term;
    bool ret = this->leader()->new_command(cmd, temp_term, temp_idx);
    if (ret)
    {
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        cmd.res->start = std::chrono::system_clock::now();
        if (!cmd.res->done)
        {
            ASSERT(
                cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::milliseconds(wait_time)) == std::cv_status::no_timeout,
                "extent_server_dist::getattr command timeout");
        }
        a = cmd.res->attr;
    }
#ifdef DEBUG_PART5
    printf("extent_server_dist::getattr return %d\n", cmd.res->done);
#endif
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &)
{
    // Lab3: your code here
#ifdef DEBUG_PART5
    printf("extent_server_dist::remove %d\n", id);
#endif
    chfs_command_raft cmd;
    cmd.cmd_tp = chfs_command_raft::command_type::CMD_RMV;
    cmd.id = id;
    int temp_idx, temp_term;
    bool ret = this->leader()->new_command(cmd, temp_term, temp_idx);
    if (ret)
    {
        std::unique_lock<std::mutex> lock(cmd.res->mtx);
        cmd.res->start = std::chrono::system_clock::now();
        if (!cmd.res->done)
        {
            ASSERT(
                cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::milliseconds(wait_time)) == std::cv_status::no_timeout,
                "extent_server_dist::remove command timeout");
        }
    }
#ifdef DEBUG_PART5
    printf("extent_server_dist::remove return %d\n", cmd.res->done);
#endif
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist()
{
    delete this->raft_group;
}