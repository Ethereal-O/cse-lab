#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template <typename command>
class raft_storage
{
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    std::vector<log_entry<command>> logs;
    std::vector<char> snapshot;
    int snp_index;
    int snp_term;
    int current_term;
    int vote_for;
    void flush();
    void save_snapshot(int last_log_index, int last_log_term, std::vector<char> snapshot);

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string log_file_path;
    std::string snapshot_file_path;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir)
{
    // Lab3: Your code here
#define LOG_SUB_PATH "/log_store"
#define SNAPSHOT_SUB_PATH "/snapshot_store"
    log_file_path = dir + LOG_SUB_PATH;
    snapshot_file_path = dir + SNAPSHOT_SUB_PATH;
#undef LOG_SUB_PATH
#undef SNAPSHOT_SUB_PATH
    std::ifstream in_log(log_file_path, std::ios::in | std::ios::binary);
    if (in_log.is_open())
    {
        in_log.read((char *)&current_term, sizeof(int));
        in_log.read((char *)&vote_for, sizeof(int));

        int log_size;
        in_log.read((char *)&log_size, sizeof(int));

        logs.clear();
        for (int i = 0; i < log_size; ++i)
        {
            log_entry<command> new_entry;
            int size;
            char *buf;

            in_log.read((char *)&new_entry.term, sizeof(int));
            in_log.read((char *)&size, sizeof(int));

            buf = new char[size];
            in_log.read(buf, size);

            new_entry.cmd.deserialize(buf, size);
            logs.push_back(new_entry);
            delete[] buf;
        }
    }
    else
    {
        current_term = 0;
        vote_for = -1;
        logs.clear();
    }
    in_log.close();

    std::ifstream in_snapshot(snapshot_file_path, std::ios::in | std::ios::binary);
    if (in_snapshot.is_open())
    {
        in_snapshot.read((char *)&snp_index, sizeof(int));
        in_snapshot.read((char *)&snp_term, sizeof(int));

        int snapshot_size;
        in_snapshot.read((char *)&snapshot_size, sizeof(int));

        snapshot.clear();
        for (int i = 0; i < snapshot_size; ++i)
        {
            char new_char;
            in_snapshot.read((char *)&new_char, sizeof(char));
            snapshot.push_back(new_char);
        }
    }
    else
    {
        snp_index = -1;
        snp_term = 0;
    }
    in_snapshot.close();
}

template <typename command>
raft_storage<command>::~raft_storage()
{
    // Lab3: Your code here
    flush();
}

template <typename command>
void raft_storage<command>::flush()
{
    mtx.lock();
    std::ofstream out(log_file_path, std::ios::trunc | std::ios::out | std::ios::binary);
    if (out.is_open())
    {
        out.write((char *)&current_term, sizeof(int));
        out.write((char *)&vote_for, sizeof(int));
        int log_size = logs.size();
        out.write((char *)&log_size, sizeof(int));

        for (auto log : logs)
        {
            int size = log.cmd.size();
            char *buf = new char[size];

            log.cmd.serialize(buf, size);

            out.write((char *)&log.term, sizeof(int));
            out.write((char *)&size, sizeof(int));
            out.write(buf, size);
            delete[] buf;
        }
    }
    out.close();
    mtx.unlock();
}

template <typename command>
void raft_storage<command>::save_snapshot(int last_log_index, int last_log_term, std::vector<char> snapshot)
{
    mtx.lock();
    this->snapshot = snapshot;
    this->snp_index = last_log_index;
    this->snp_term = last_log_term;

    std::ofstream out_snapshot(snapshot_file_path, std::ios::trunc | std::ios::out | std::ios::binary);
    if (out_snapshot.is_open())
    {
        out_snapshot.write((char *)&snp_index, sizeof(int));
        out_snapshot.write((char *)&snp_term, sizeof(int));
        int size = snapshot.size();
        out_snapshot.write((char *)&size, sizeof(int));
        out_snapshot.write(snapshot.data(), size);
    }
    out_snapshot.close();
    mtx.unlock();
}

#endif // raft_storage_h