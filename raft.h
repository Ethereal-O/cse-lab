#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <set>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

// #define DEBUG_PART1
// #define DEBUG_PART2
// #define DEBUG_PART5

template <typename state_machine, typename command>
class raft
{
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

    // #define RAFT_LOG(fmt, args...) \
//     do                         \
//     {                          \
//     } while (0);

#define RAFT_LOG(fmt, args...)                                                                                   \
    do                                                                                                           \
    {                                                                                                            \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role
    {
        follower,
        candidate,
        leader
    };
    raft_role role;

    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */
    int current_term;
    int vote_for;
    int election_timeout;
    std::set<int> followers;
    clock_t last_heartbeat_time;

    /* ---- Volatile state on all server----  */
    int commit_index;
    int last_applied;

    /* ---- Volatile state on leader----  */
    std::vector<int> next_index;
    std::vector<int> match_index;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes()
    {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    int get_log_term(int index);
    void begin_new_term(int term);
    void become_follower();
    void become_candidate();
    void become_leader();
    bool judge_match(int prev_log_index, int prev_log_term);
    bool can_vote(request_vote_args args);
    int get_commit_index();
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) : stopped(false),
                                                                                                                                               rpc_server(server),
                                                                                                                                               rpc_clients(clients),
                                                                                                                                               my_id(idx),
                                                                                                                                               storage(storage),
                                                                                                                                               state(state),
                                                                                                                                               background_election(nullptr),
                                                                                                                                               background_ping(nullptr),
                                                                                                                                               background_commit(nullptr),
                                                                                                                                               background_apply(nullptr),
                                                                                                                                               current_term(0),
                                                                                                                                               role(follower)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    current_term = storage->current_term;
    vote_for = storage->vote_for;
    commit_index = 0;
    last_applied = 0;
    last_heartbeat_time = clock();
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft()
{
    if (background_ping)
    {
        delete background_ping;
    }
    if (background_election)
    {
        delete background_election;
    }
    if (background_commit)
    {
        delete background_commit;
    }
    if (background_apply)
    {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop()
{
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped()
{
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term)
{
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start()
{
    // Lab3: Your code here
#ifdef DEBUG_PART1
    RAFT_LOG("start");
#endif
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index)
{
    // Lab3: Your code here
    mtx.lock();
    if (role == leader)
    {
        log_entry<command> new_log;
        new_log.cmd = cmd;
        new_log.term = current_term;
#ifdef DEBUG_PART2
        RAFT_LOG("new_command %d %d", current_term, 100 * my_id);
#endif

        storage->logs.push_back(new_log);
        storage->flush();

        term = current_term;
        index = storage->logs.size();

        match_index[my_id] = index;

        mtx.unlock();
        return true;
    }
    mtx.unlock();
    return false;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot()
{
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
#ifdef DEBUG_PART1
    RAFT_LOG("%d request_vote", my_id);
#endif
    if (args.term < current_term)
    {
        reply.term = current_term;
        reply.vote_granted = false;
        goto release;
    }

    if (args.term > current_term)
        begin_new_term(args.term);

    if (can_vote(args))
    {
        storage->vote_for = args.candidate_id;
        storage->flush();

        reply.vote_granted = true;
        vote_for = args.candidate_id;
    }

release:
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
#ifdef DEBUG_PART1
    RAFT_LOG("%d handle_request_vote_reply", my_id);
#endif
    if (reply.term > current_term)
    {
        begin_new_term(reply.term);
        goto release;
    }

    // old message
    if (role != candidate || arg.term != current_term)
        goto release;

    if (reply.vote_granted)
    {
        followers.insert(target);
        if (followers.size() > num_nodes() / 2)
        {
            next_index = std::vector<int>(num_nodes(), storage->logs.size() + 1);
            match_index = std::vector<int>(num_nodes(), 0);
            become_leader();
        }
    }

release:
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
#ifdef DEBUG_PART1
    RAFT_LOG("%d append_entries", my_id);
#endif

    if (arg.term < current_term)
    {
        reply.term = current_term;
        reply.success = false;
        goto release;
    }

    if (arg.term > current_term)
        begin_new_term(arg.term);

    if (arg.entries.size() > 0)
    {

        if (!judge_match(arg.prev_log_index, arg.prev_log_term))
        {
            reply.term = current_term;
            reply.success = false;
            goto release;
        }
#ifdef DEBUG_PART2
        RAFT_LOG("%d success to add entry", my_id);
#endif

        storage->logs.erase(storage->logs.begin() + arg.prev_log_index, storage->logs.end());
        storage->logs.insert(storage->logs.end(), arg.entries.begin(), arg.entries.end());
        storage->flush();
        reply.success = true;
        if (arg.leader_commit > commit_index)
            commit_index = std::min(arg.leader_commit, (int)storage->logs.size());
    }
    else
    {
        last_heartbeat_time = clock();
        if (role == candidate)
            become_follower();
        if (judge_match(arg.prev_log_index, arg.prev_log_term) && arg.prev_log_index == storage->logs.size())
            if (arg.leader_commit > commit_index)
                commit_index = std::min(arg.leader_commit, (int)storage->logs.size());
    }

release:
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
#ifdef DEBUG_PART1
    RAFT_LOG("%d handle_append_entries_reply", my_id);
#endif
    if (role != leader)
        goto release;

    if (arg.entries.empty())
        goto release;

    if (!reply.success)
    {
        if (reply.term > current_term)
            begin_new_term(reply.term);
        else if (next_index[node] > 1)
            --next_index[node];
        goto release;
    }

    match_index[node] = arg.prev_log_index + arg.entries.size();
    next_index[node] = match_index[node] + 1;

    commit_index = get_commit_index();

release:
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply)
{
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply)
{
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg)
{
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0)
    {
        handle_request_vote_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg)
{
    append_entries_reply reply;
    int ret;
    if ((ret = rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply)) == 0)
    {
        handle_append_entries_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg)
{
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0)
    {
        handle_install_snapshot_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election()
{
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

#define sleep_time 10

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here

        mtx.lock();
        if (role == follower || role == candidate)
        {
            if ((1000 * (clock() - last_heartbeat_time)) / CLOCKS_PER_SEC > election_timeout)
            {
                begin_new_term(current_term + 1);
                become_candidate();
                for (int i = 0; i < num_nodes(); i++)
                {
                    if (i == my_id)
                        continue;
                    request_vote_args args;
                    args.term = current_term;
                    args.candidate_id = my_id;
                    args.last_log_index = storage->logs.size();
                    args.last_log_term = get_log_term(args.last_log_index);
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                }
            }
        }

        mtx.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
    }

#undef sleep_time
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit()
{
    // Periodly send logs to the follower.

    // Only work for the leader.

#define sleep_time 10

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here
        mtx.lock();
        if (role == leader)
        {
            for (int i = 0; i < num_nodes(); i++)
            {
                if (i == my_id)
                    continue;
                if (next_index[i] > storage->logs.size())
                    continue;

                append_entries_args<command> args;
                args.term = current_term;
                args.leader_id = my_id;
                args.prev_log_index = next_index[i] - 1;
                args.prev_log_term = get_log_term(args.prev_log_index);
                args.leader_commit = commit_index;
                args.entries = std::vector<log_entry<command>>(storage->logs.begin() + next_index[i] - 1, storage->logs.end());
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }
        mtx.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
    }

#undef sleep_time
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply()
{
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

#define sleep_time 10

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here:
        mtx.lock();
        // if (role==leader)
        //     RAFT_LOG("%d %d %d", my_id, commit_index, last_applied);
        while (last_applied < commit_index)
        {
            state->apply_log(storage->logs[last_applied].cmd);
            last_applied++;
        }
        mtx.unlock();
    }

#undef sleep_time
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping()
{
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.
#define sleep_time 100

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here:
        mtx.lock();
        if (role == leader)
        {
            for (int i = 0; i < num_nodes(); i++)
            {
                if (i == my_id)
                    continue;
                append_entries_args<command> args;
                args.term = current_term;
                args.leader_id = my_id;
                args.leader_commit = commit_index;
                int prev_log_index = next_index[i] - 1;
                args.prev_log_index = prev_log_index;
                args.prev_log_term = get_log_term(prev_log_index);
                args.entries.clear();

                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }
        mtx.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
    }

#undef sleep_time
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

/**
 * @brief get_log_term
 *
 * @tparam state_machine
 * @tparam command
 * @param index **it will start at 1**
 * @return int
 */
template <typename state_machine, typename command>
int raft<state_machine, command>::get_log_term(int index)
{
    if (index == -1 || index == 0)
        return index;
    if (index > storage->logs.size())
        return -1;
    return storage->logs[index - 1].term;
}

/**
 * @brief begin_new_term
 * 
 * @tparam state_machine 
 * @tparam command 
 * @param term 
 */
template <typename state_machine, typename command>
void raft<state_machine, command>::begin_new_term(int term)
{
    storage->current_term = term;
    storage->vote_for = -1;
    storage->flush();

    become_follower();
    current_term = term;
    vote_for = -1;
    last_heartbeat_time = clock();
}

/**
 * @brief become_follower
 * 
 * @tparam state_machine 
 * @tparam command 
 */
template <typename state_machine, typename command>
void raft<state_machine, command>::become_follower()
{
#ifdef DEBUG_PART1
    RAFT_LOG("%d becoming follower", my_id);
#endif
#ifdef DEBUG_PART2
    RAFT_LOG("%d becoming follower", my_id);
#endif
#ifdef DEBUG_PART5
    RAFT_LOG("%d becoming follower", my_id);
#endif
#define follower_timeout_max 600
#define follower_timeout_min 500
    election_timeout = rand() % (follower_timeout_max - follower_timeout_min) + follower_timeout_min;
    followers.clear();
    role = follower;
}

/**
 * @brief become_candidate
 * 
 * @tparam state_machine 
 * @tparam command 
 */
template <typename state_machine, typename command>
void raft<state_machine, command>::become_candidate()
{
#ifdef DEBUG_PART1
    RAFT_LOG("%d becoming candidate", my_id);
#endif
#ifdef DEBUG_PART2
    RAFT_LOG("%d becoming candidate", my_id);
#endif
#ifdef DEBUG_PART5
    RAFT_LOG("%d becoming candidate", my_id);
#endif
#define candidate_timeout_max 2000
#define candidate_timeout_min 800
    election_timeout = rand() % (candidate_timeout_max - candidate_timeout_min) + candidate_timeout_min;
    followers.clear();
    followers.insert(my_id);
    storage->vote_for = my_id;
    vote_for = my_id;
    role = candidate;
}

/**
 * @brief become_leader
 * 
 * @tparam state_machine 
 * @tparam command 
 */
template <typename state_machine, typename command>
void raft<state_machine, command>::become_leader()
{
#ifdef DEBUG_PART1
    RAFT_LOG("%d becoming leader", my_id);
#endif
#ifdef DEBUG_PART2
    RAFT_LOG("%d becoming leader", my_id);
#endif
#ifdef DEBUG_PART5
    RAFT_LOG("%d becoming leader", my_id);
#endif
    next_index = std::vector<int>(num_nodes(), storage->logs.size() + 1);
    match_index = std::vector<int>(num_nodes(), 0);
    match_index[my_id] = storage->logs.size();
    role = leader;
}

/**
 * @brief judge_match
 * 
 * @tparam state_machine 
 * @tparam command 
 * @param prev_log_index 
 * @param prev_log_term 
 * @return true 
 * @return false 
 */
template <typename state_machine, typename command>
bool raft<state_machine, command>::judge_match(int prev_log_index, int prev_log_term)
{
    if (storage->logs.size() < prev_log_index)
        return false;
    return get_log_term(prev_log_index) == prev_log_term;
}

/**
 * @brief can_vote
 * 
 * @tparam state_machine 
 * @tparam command 
 * @param args 
 * @return true 
 * @return false 
 */
template <typename state_machine, typename command>
bool raft<state_machine, command>::can_vote(request_vote_args args)
{
    if (vote_for != -1 && vote_for != args.candidate_id)
        return false;

    return get_log_term(storage->logs.size()) < args.last_log_term || (get_log_term(storage->logs.size()) == args.last_log_term && storage->logs.size() <= args.last_log_index);
}

/**
 * @brief get_commit_index
 * 
 * @tparam state_machine 
 * @tparam command 
 * @return int 
 */
template <typename state_machine, typename command>
int raft<state_machine, command>::get_commit_index()
{
    std::vector<int> copy = match_index;
    std::sort(copy.begin(), copy.end());
    return copy[copy.size() / 2];
}

#endif // raft_h