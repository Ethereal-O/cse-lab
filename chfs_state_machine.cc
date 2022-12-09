#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft()
{
    // Lab3: Your code here
    cmd_tp = command_type::CMD_NONE;
    type = 0;
    id = 0;
    buf = "";
    // res = std::make_shared<result>();
    res = new result();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) : cmd_tp(cmd.cmd_tp), type(cmd.type), id(cmd.id), buf(cmd.buf), res(cmd.res)
{
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft()
{
    // Lab3: Your code here
}

int chfs_command_raft::size() const
{
    // Lab3: Your code here
    return sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) + sizeof(int) + buf.size();
}

void chfs_command_raft::serialize(char *buf_out, int size) const
{
    // Lab3: Your code here
    if (size != this->size())
        return;
    std::string out_str;
    out_str += std::string((char *)&cmd_tp, sizeof(command_type));
    out_str += std::string((char *)&type, sizeof(uint32_t));
    out_str += std::string((char *)&id, sizeof(extent_protocol::extentid_t));
    int buf_size = buf.size();
    out_str += std::string((char *)&buf_size, sizeof(int));
    out_str += buf;
    memcpy(buf_out, out_str.c_str(), size);
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size)
{
    // Lab3: Your code here
    std::string in_str = std::string(buf_in, size);
    int offset = 0;
    memcpy(&cmd_tp, in_str.c_str() + offset, sizeof(command_type));
    offset += sizeof(command_type);
    memcpy(&type, in_str.c_str() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(&id, in_str.c_str() + offset, sizeof(extent_protocol::extentid_t));
    offset += sizeof(extent_protocol::extentid_t);
    int buf_size;
    memcpy(&buf_size, in_str.c_str() + offset, sizeof(int));
    offset += sizeof(int);
    buf = in_str.substr(offset, buf_size);
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd)
{
    // Lab3: Your code here
    unsigned long long ptr = (unsigned long long)cmd.res;
    int command_type = cmd.cmd_tp;
    m << command_type << cmd.type << cmd.id << cmd.buf << ptr;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd)
{
    // Lab3: Your code here
    int command_type;
    unsigned long long ptr;
    u >> command_type >> cmd.type >> cmd.id >> cmd.buf >> ptr;
    cmd.cmd_tp = (chfs_command_raft::command_type)command_type;
    cmd.res = (chfs_command_raft::result *)ptr;
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd)
{
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here

    mtx.lock();
    std::unique_lock<std::mutex> lock(chfs_cmd.res->mtx);
    if (chfs_cmd.res->done && (chfs_cmd.cmd_tp == chfs_command_raft::command_type::CMD_GET || chfs_cmd.cmd_tp == chfs_command_raft::command_type::CMD_GETA))
    {
        mtx.unlock();
        return;
    }
    switch (chfs_cmd.cmd_tp)
    {
    case chfs_command_raft::command_type::CMD_CRT:
    {
        extent_protocol::extentid_t id;
        es.create(chfs_cmd.type, id);
        chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_CRT;
        chfs_cmd.res->id = id;
        chfs_cmd.res->done = true;
        break;
    }

    case chfs_command_raft::command_type::CMD_PUT:
    {
        int r;
        es.put(chfs_cmd.id, chfs_cmd.buf, r);
        chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_PUT;
        chfs_cmd.res->done = true;
        break;
    }

    case chfs_command_raft::command_type::CMD_GET:
    {
        std::string buf;
        es.get(chfs_cmd.id, buf);
        chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_GET;
        chfs_cmd.res->buf = buf;
        chfs_cmd.res->done = true;
        break;
    }

    case chfs_command_raft::command_type::CMD_GETA:
    {
        extent_protocol::attr a;
        es.getattr(chfs_cmd.id, a);
        chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_GETA;
        chfs_cmd.res->attr = a;
        chfs_cmd.res->done = true;
        break;
    }

    case chfs_command_raft::command_type::CMD_RMV:
    {
        int r;
        es.remove(chfs_cmd.id, r);
        chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_RMV;
        chfs_cmd.res->done = true;
        break;
    }

    default:
        break;
    }
    chfs_cmd.res->cv.notify_all();
    mtx.unlock();
    return;
}
