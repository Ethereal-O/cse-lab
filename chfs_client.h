#ifndef chfs_client_h
#define chfs_client_h

#include <string>
//#include "chfs_protocol.h"
#include "extent_client.h"
#include <vector>

#define EXT4_NAME_LEN 255
#define EXT4_PREFIX_SIZE (sizeof(uint32_t) + sizeof(uint16_t) + 2 * sizeof(uint8_t))
// #define EXT4_PREFIX_SIZE (sizeof(struct ext4_dir_entry)-EXT4_NAME_LEN*sizeof(char))

class chfs_client
{
  extent_client *ec;

public:
  typedef unsigned long long inum;
  enum xxstatus
  {
    OK,
    RPCERR,
    NOENT,
    IOERR,
    EXIST
  };
  typedef int status;

  struct fileinfo
  {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo
  {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent
  {
    std::string name;
    chfs_client::inum inum;
  };
  struct syminfo {
    std::string slink;
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  }
  struct ext4_dir_entry
  {
    uint32_t inode_number;
    uint16_t dir_entry_length;
    uint8_t file_name_length;
    uint8_t file_type;
    char name[EXT4_NAME_LEN];
  };
  enum types
  {
    UNKNOWN = 1,
    REGULAR_FILE,
    DIRECTORY,
    CHARACTER_DEVICE_FILE,
    BLOCK_DEVICE_FILE,
    FIFO,
    SOCKET,
    SYMBOLIC_LINK
  };

private:
  static std::string filename(inum);
  static inum n2i(std::string);

 public:
  chfs_client(std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int getsymlink(inum, syminfo&);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum, const char *);
  int mkdir(inum, const char *, mode_t, inum &);

  /** you may need to add symbolic link related methods here.*/
  int symlink(const char *, inum, const char *, inum &);
  int readlink(inum, std::string &);
};

#endif
