// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

#define LAB2A_PART2
#define LAB2B_PART3

/*
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create',
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log
 * to achive all-or-nothing for these transactions.
 */

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    txid = 0;
#ifdef LAB2A_PART2
    // log
    txid++;
    ec->beginLog(txid);
    ec->putLog(txid, 1, "");
    ec->commitLog(txid);
#endif
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum)
{
    // // Oops! is this still correct when you implement symlink?
    // return !isfile(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR)
    {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

int chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx)                                                \
    do                                                             \
    {                                                              \
        if ((xx) != extent_protocol::OK)                           \
        {                                                          \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

#ifdef LAB2B_PART3
    lc->acquire(ino);
#endif

    printf("setattr %016llx\n", ino);
    extent_protocol::attr a;
    std::string buf;
    if (ec->getattr(ino, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    if (ec->get(ino, buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    if (a.size < size)
        buf.append(size - a.size, '\0');

#ifdef LAB2A_PART2
    // log
    txid++;
    ec->beginLog(txid);
    ec->putLog(txid, ino, buf.substr(0, size));
    ec->commitLog(txid);
#endif

    if (ec->put(ino, buf.substr(0, size)) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

release:

#ifdef LAB2B_PART3
    lc->release(ino);
#endif

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found;
    std::string buf;
    ext4_dir_entry newDirOriginInfo;
    std::string newBuf;

#ifdef LAB2B_PART3
    lc->acquire(parent);
#endif

    if ((r = lookup(parent, name, found, ino_out)) != extent_protocol::OK)
        goto release_parent;

    if (found)
    {
        r = EXIST;
        goto release_parent;
    }

    if (ec->get(parent, buf) != extent_protocol::OK)
    {
        printf("error get, return not OK\n");
        r = IOERR;
        goto release_parent;
    }

    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK)
    {
        printf("error create, return not OK\n");
        r = IOERR;
        goto release_parent;
    }

#ifdef LAB2B_PART3
    lc->acquire(ino_out);
#endif

    newDirOriginInfo.inode_number = ino_out;
    newDirOriginInfo.dir_entry_length = EXT4_PREFIX_SIZE + strlen(name);
    newDirOriginInfo.file_name_length = strlen(name);
    newDirOriginInfo.file_type = REGULAR_FILE;
    memcpy(newDirOriginInfo.name, name, newDirOriginInfo.file_name_length);
    newDirOriginInfo.name[newDirOriginInfo.file_name_length] = 0;

    newBuf = std::string((char *)&newDirOriginInfo, newDirOriginInfo.dir_entry_length);
    newBuf = buf + newBuf;

#ifdef LAB2A_PART2
    // log
    txid++;
    ec->beginLog(txid);
    ec->createLog(txid, extent_protocol::T_FILE, ino_out);
    ec->putLog(txid, parent, newBuf);
    ec->commitLog(txid);
#endif

    if (ec->put(parent, newBuf) != extent_protocol::OK)
    {
        printf("error put, return not OK\n");
        r = IOERR;
        goto release;
    }

release:

#ifdef LAB2B_PART3
    lc->release(ino_out);
#endif

release_parent:

#ifdef LAB2B_PART3
    lc->release(parent);
#endif

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found;
    std::string buf;
    ext4_dir_entry newDirOriginInfo;
    std::string newBuf;

#ifdef LAB2B_PART3
    lc->acquire(parent);
#endif

    if ((r = lookup(parent, name, found, ino_out)) != extent_protocol::OK)
        goto release_parent;

    if (found)
    {
        r = EXIST;
        goto release_parent;
    }

    if (ec->get(parent, buf) != extent_protocol::OK)
    {
        printf("error get, return not OK\n");
        r = IOERR;
        goto release_parent;
    }

    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK)
    {
        printf("error create, return not OK\n");
        r = IOERR;
        goto release_parent;
    }

#ifdef LAB2B_PART3
    lc->acquire(ino_out);
#endif

    newDirOriginInfo.inode_number = ino_out;
    newDirOriginInfo.dir_entry_length = EXT4_PREFIX_SIZE + strlen(name);
    newDirOriginInfo.file_name_length = strlen(name);
    newDirOriginInfo.file_type = DIRECTORY;
    memcpy(newDirOriginInfo.name, name, newDirOriginInfo.file_name_length);
    newDirOriginInfo.name[newDirOriginInfo.file_name_length] = 0;

    newBuf = std::string((char *)&newDirOriginInfo, newDirOriginInfo.dir_entry_length);
    newBuf = buf + newBuf;

#ifdef LAB2A_PART2
    // log
    txid++;
    ec->beginLog(txid);
    ec->createLog(txid, extent_protocol::T_DIR, ino_out);
    ec->putLog(txid, parent, newBuf);
    ec->commitLog(txid);
#endif

    if (ec->put(parent, newBuf) != extent_protocol::OK)
    {
        printf("error put, return not OK\n");
        r = IOERR;
        goto release;
    }

release:

#ifdef LAB2B_PART3
    lc->release(ino_out);
#endif

release_parent:

#ifdef LAB2B_PART3
    lc->release(parent);
#endif

    return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    std::list<chfs_client::dirent> entries;
    if ((r = readdir(parent, entries)) != OK)
        return r;

    for (std::list<chfs_client::dirent>::iterator iter = entries.begin(); iter != entries.end(); iter++)
    {
        if (strcmp(name, (*iter).name.c_str()) == 0)
        {
            found = true;
            ino_out = (*iter).inum;
            goto release;
        }
    }

    found = false;

release:

    return r;
}

int chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    extent_protocol::attr a;
    std::string buf;
    std::size_t begin = 0;
    if (ec->getattr(dir, a) != extent_protocol::OK)
    {
        printf("error getting attr, return not OK\n");
        r = IOERR;
        goto release;
    }

    if (a.type != extent_protocol::T_DIR)
    {
        printf("error, not a dir\n");
        r = IOERR;
        goto release;
    }

    if (ec->get(dir, buf) != extent_protocol::OK)
    {
        printf("error get, return not OK\n");
        r = IOERR;
        goto release;
    }

    while (begin < buf.size())
    {
        ext4_dir_entry dirOriginInfo;
        memcpy(&dirOriginInfo, buf.data() + begin, EXT4_PREFIX_SIZE);
        memcpy(dirOriginInfo.name, buf.data() + begin + EXT4_PREFIX_SIZE, dirOriginInfo.file_name_length);
        dirOriginInfo.name[dirOriginInfo.file_name_length] = 0;
        list.push_back({std::string(dirOriginInfo.name), dirOriginInfo.inode_number});
        begin += dirOriginInfo.dir_entry_length;
    }

release:

    return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    extent_protocol::attr a;
    std::string buf;
    unsigned int maxSize;
    if (ec->getattr(ino, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    if (ec->get(ino, buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    maxSize = (a.size - off) > 0 ? (a.size - off - size > 0 ? size : (a.size - off)) : 0;

    data = maxSize > 0 ? std::string(buf.substr(off, maxSize)) : nullptr;

release:

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                       size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

#ifdef LAB2B_PART3
    lc->acquire(ino);
#endif

    extent_protocol::attr a;
    std::string buf;
    std::string dataStr;
    if (ec->getattr(ino, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    if (ec->get(ino, buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    dataStr = std::string(data, size);
    if (a.size >= off)
    {
        if (a.size >= off + size)
            buf.replace(off, size, dataStr, 0, size);
        else
            buf = buf.substr(0, off) + dataStr.substr(0, size);
    }
    else
    {
        buf.append(off - a.size, '\0');
        buf += dataStr.substr(0, size);
    }
    bytes_written = size;

#ifdef LAB2A_PART2
    // log
    txid++;
    ec->beginLog(txid);
    ec->putLog(txid, ino, buf);
    ec->commitLog(txid);
#endif

    if (ec->put(ino, buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

release:

#ifdef LAB2B_PART3
    lc->release(ino);
#endif

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent, const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
#ifdef LAB2B_PART3
    lc->acquire(parent);
#endif

    std::string buf;
    std::string newBuf;
    std::size_t begin = 0;
    bool found = false;
    inum fileIno;
    if (ec->get(parent, buf) != extent_protocol::OK)
    {
        printf("error get, return not OK\n");
        r = IOERR;
        goto release_parent;
    }

    while (begin < buf.size())
    {
        ext4_dir_entry dirOriginInfo;
        memcpy(&dirOriginInfo, buf.data() + begin, EXT4_PREFIX_SIZE);
        memcpy(dirOriginInfo.name, buf.data() + begin + EXT4_PREFIX_SIZE, dirOriginInfo.file_name_length);
        dirOriginInfo.name[dirOriginInfo.file_name_length] = 0;
        if (strcmp(dirOriginInfo.name, name) == 0)
        {
            if (dirOriginInfo.file_type == DIRECTORY)
            {
                printf("error remove, it's a dir\n");
                r = IOERR;
                goto release_parent;
            }
            fileIno = dirOriginInfo.inode_number;
            found = true;
        }
        else
        {
            newBuf = newBuf + std::string((char *)&dirOriginInfo, dirOriginInfo.dir_entry_length);
        }
        begin += dirOriginInfo.dir_entry_length;
    }

    if (!found)
    {
        r = NOENT;
        goto release_parent;
    }

#ifdef LAB2A_PART2
    // log
    txid++;
    ec->beginLog(txid);
    ec->removeLog(txid, fileIno);
    ec->putLog(txid, parent, newBuf);
    ec->commitLog(txid);
#endif

#ifdef LAB2B_PART3
    lc->acquire(fileIno);
#endif

    if (ec->remove(fileIno) != extent_protocol::OK)
    {
        printf("error remove, return not OK\n");
        r = IOERR;
        goto release;
    }

    if (ec->put(parent, newBuf) != extent_protocol::OK)
    {
        printf("error put, return not OK\n");
        r = IOERR;
        goto release;
    }

release:

#ifdef LAB2B_PART3
    lc->release(fileIno);
#endif

release_parent:

#ifdef LAB2B_PART3
    lc->release(parent);
#endif

    return r;
}

int chfs_client::symlink(const char *link, inum parent, const char *name, inum &ino_out)
{
    int r = OK;

    bool found;
    std::string buf;
    ext4_dir_entry newDirOriginInfo;
    std::string newBuf;
    std::string linkBuf;

#ifdef LAB2B_PART3
    lc->acquire(parent);
#endif

    if ((r = lookup(parent, name, found, ino_out)) != extent_protocol::OK)
        goto release_parent;

    if (found)
    {
        r = EXIST;
        goto release_parent;
    }

    if (ec->get(parent, buf) != extent_protocol::OK)
    {
        printf("error get, return not OK\n");
        r = IOERR;
        goto release_parent;
    }

    if (ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK)
    {
        printf("error create, return not OK\n");
        r = IOERR;
        goto release_parent;
    }

#ifdef LAB2B_PART3
    lc->acquire(ino_out);
#endif

#ifndef LAB2A_PART2
    size_t bytes_written, linkSize = strlen(link);
    if (write(ino_out, linkSize, 0, link, bytes_written) != OK || bytes_written != linkSize)
    {
        printf("error write, return not OK\n");
        r = IOERR;
        return r;
    }
#endif

#ifdef LAB2A_PART2
    linkBuf = std::string(link);
    if (ec->put(ino_out, linkBuf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
#endif

    newDirOriginInfo.inode_number = ino_out;
    newDirOriginInfo.dir_entry_length = EXT4_PREFIX_SIZE + strlen(name);
    newDirOriginInfo.file_name_length = strlen(name);
    newDirOriginInfo.file_type = SYMBOLIC_LINK;
    memcpy(newDirOriginInfo.name, name, newDirOriginInfo.file_name_length);
    newDirOriginInfo.name[newDirOriginInfo.file_name_length] = 0;

    newBuf = std::string((char *)&newDirOriginInfo, newDirOriginInfo.dir_entry_length);
    newBuf = buf + newBuf;

#ifdef LAB2A_PART2
    // log
    txid++;
    ec->beginLog(txid);
    ec->createLog(txid, extent_protocol::T_SYMLINK, ino_out);
    ec->putLog(txid, ino_out, linkBuf);
    ec->putLog(txid, parent, newBuf);
    ec->commitLog(txid);
#endif

    if (ec->put(parent, newBuf) != extent_protocol::OK)
    {
        printf("error put, return not OK\n");
        r = IOERR;
        goto release;
    }

release:

#ifdef LAB2B_PART3
    lc->release(ino_out);
#endif

release_parent:

#ifdef LAB2B_PART3
    lc->release(parent);
#endif

    return r;
}

int chfs_client::readlink(inum ino, std::string &link)
{
    int r = OK;

    extent_protocol::attr a;
    std::string buf;
    if (ec->getattr(ino, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    if (ec->get(ino, buf) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    link = buf;

release:

    return r;
}
