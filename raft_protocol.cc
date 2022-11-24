#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args)
{
    // Lab3: Your code here
    m << args.term << args.candidate_id << args.last_log_index << args.last_log_term;
    return m;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args)
{
    // Lab3: Your code here
    u >> args.term >> args.candidate_id >> args.last_log_index >> args.last_log_term;
    return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply)
{
    // Lab3: Your code here
    m << reply.term << reply.vote_granted;
    return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply)
{
    // Lab3: Your code here
    u >> reply.term >> reply.vote_granted;
    return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &args)
{
    // Lab3: Your code here
    m << args.term << args.success;
    return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args)
{
    // Lab3: Your code here
    m >> args.term >> args.success;
    return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args)
{
    // Lab3: Your code here
    m << args.term << args.leader_id << args.last_included_index << args.last_included_term << args.offset << args.data << args.done;
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args)
{
    // Lab3: Your code here
    u >> args.term >> args.leader_id >> args.last_included_index >> args.last_included_term >> args.offset >> args.data >> args.done;
    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply)
{
    // Lab3: Your code here
    m << reply.term;
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply)
{
    // Lab3: Your code here
    u >> reply.term;
    return u;
}