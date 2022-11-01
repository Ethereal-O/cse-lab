#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 1024
#define LAB2_PART3

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires.
 *
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command
{
public:
    typedef unsigned long long txid_t;
    enum cmd_type
    {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_GET,
        CMD_GETADDR,
        CMD_REMOVE
    };

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;

    std::string parameters;

    // constructor
    // chfs_command() {}

    chfs_command(cmd_type typeT = CMD_BEGIN, txid_t idT = 0, std::string parametersT = "") : type(typeT), id(idT), parameters(parametersT) {}

    uint64_t size() const
    {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t) + parameters.size();
        return s;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to
 * persist and recover data.
 *
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template <typename command>
class persister
{

public:
    persister(const std::string &file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const command &log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;
    uint32_t tot_log_size;

public:
    // restored log data
    std::vector<command> log_entries;
};

template <typename command>
persister<command>::persister(const std::string &dir)
{
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
    tot_log_size = 0;
}

template <typename command>
persister<command>::~persister()
{
    // Your code here for lab2A
}

template <typename command>
void persister<command>::append_log(const command &log)
{
    // Your code here for lab2A
#ifdef LAB2_PART3
    // check size
    if (tot_log_size + log.size() + sizeof(uint64_t) >= MAX_LOG_SZ)
        checkpoint();
#endif
    mtx.lock();
    // append vector
    log_entries.push_back(log);
    tot_log_size += log.size() + sizeof(uint64_t);
    // append disk
    std::ofstream disk_logs(file_path_logfile, std::ofstream::app);
    uint64_t uint_log_size = log.size();
    disk_logs.write((char *)&uint_log_size, sizeof(uint64_t));
    disk_logs.write((char *)&log.type, sizeof(log.type));
    disk_logs.write((char *)&log.id, sizeof(log.id));
    disk_logs << log.parameters;
    disk_logs.close();
    mtx.unlock();
}

template <typename command>
void persister<command>::checkpoint()
{
    // Your code here for lab2A
    mtx.lock();
    std::ifstream disk_logs(file_path_logfile);
    std::ofstream disk_checkpoints(file_path_checkpoint, std::ofstream::app);
    char *buf = new char[tot_log_size + 1]{0};
    disk_logs.read(buf, tot_log_size);
    disk_checkpoints.write(buf, tot_log_size);
    delete buf;
    disk_logs.close();
    disk_checkpoints.close();
    std::ofstream new_disk_logs(file_path_logfile, std::ofstream::trunc);
    new_disk_logs.close();
    tot_log_size = 0;
    mtx.unlock();
}

template <typename command>
void persister<command>::restore_logdata()
{
    // Your code here for lab2A
#ifdef LAB2_PART3
    restore_checkpoint();
#endif
    std::ifstream disk_logs(file_path_logfile);
    if (!disk_logs)
        return;
    mtx.lock();
    uint64_t log_size;
    command log;
    while (!disk_logs.eof())
    {
        disk_logs.read((char *)&log_size, sizeof(uint64_t));
        disk_logs.read((char *)&log.type, sizeof(log.type));
        disk_logs.read((char *)&log.id, sizeof(log.id));
        char *buf = new char[log_size + 1]{0};
        disk_logs.read(buf, log_size - sizeof(log.type) - sizeof(log.id));
        log.parameters = std::string(buf, log_size - sizeof(log.type) - sizeof(log.id));
        delete buf;
        log_entries.push_back(log);
        tot_log_size += log.size() + sizeof(uint64_t);
        if (disk_logs.peek() == EOF)
            break;
    }
    disk_logs.close();
    mtx.unlock();
};

template <typename command>
void persister<command>::restore_checkpoint()
{
    // Your code here for lab2A
    std::ifstream disk_checkpoints(file_path_checkpoint);
    if (!disk_checkpoints)
        return;
    mtx.lock();
    uint64_t log_size;
    command log;
    while (!disk_checkpoints.eof())
    {
        disk_checkpoints.read((char *)&log_size, sizeof(uint64_t));
        disk_checkpoints.read((char *)&log.type, sizeof(log.type));
        disk_checkpoints.read((char *)&log.id, sizeof(log.id));
        char *buf = new char[log_size + 1]{0};
        disk_checkpoints.read(buf, log_size - sizeof(log.type) - sizeof(log.id));
        log.parameters = std::string(buf, log_size - sizeof(log.type) - sizeof(log.id));
        delete buf;
        log_entries.push_back(log);
        if (disk_checkpoints.peek() == EOF)
            break;
    }
    disk_checkpoints.close();
    mtx.unlock();
};

using chfs_persister = persister<chfs_command>;

#endif // persister_h