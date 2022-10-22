#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE * sizeof(char));
}

void disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE * sizeof(char));
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  blockid_t i;
  bool isFree;
  for (i = IBLOCK(sb.ninodes, sb.nblocks) + 1; i <= sb.nblocks; i++)
  {
    isFree = false;
    char buf[BLOCK_SIZE];
    blockid_t bBlockId = BBLOCK(i);
    read_block(bBlockId, buf);
    char bit = 0x1 << ((8 * sizeof(char)) - ((i % BPB) % (8 * sizeof(char))) - 1);
    isFree = !(buf[(i % BPB) / (8 * sizeof(char))] & bit);
    if (isFree)
    {
      buf[(i % BPB) / (8 * sizeof(char))] = buf[(i % BPB) / (8 * sizeof(char))] | bit;
      write_block(bBlockId, buf);
      break;
    }
  }
  if (isFree)
    return i;
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  bool isFree = false;
  char buf[BLOCK_SIZE];
  blockid_t bBlockId = BBLOCK(id);
  read_block(bBlockId, buf);
  char bit = 0x1 << ((8 * sizeof(char)) - ((id % BPB) % (8 * sizeof(char))) - 1);
  isFree = !(buf[(id % BPB) / (8 * sizeof(char))] & bit);
  if (isFree)
    return;
  buf[(id % BPB) / (8 * sizeof(char))] = buf[(id % BPB) / (8 * sizeof(char))] & (bit ^ 0xff);
  write_block(bBlockId, buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  uint32_t i;
  bool isFree;
  for (i = 1; i <= bm->sb.ninodes; i++)
  {
    isFree = false;
    char buf[BLOCK_SIZE];
    blockid_t iBlockId = IBLOCK(i, bm->sb.nblocks);
    blockid_t bBlockId = BBLOCK(iBlockId);
    bm->read_block(bBlockId, buf);
    char bit = 0x1 << ((8 * sizeof(char)) - ((iBlockId % BPB) % (8 * sizeof(char))) - 1);
    isFree = !(buf[(iBlockId % BPB) / (8 * sizeof(char))] & bit);
    if (isFree)
    {
      buf[(iBlockId % BPB) / (8 * sizeof(char))] = buf[(iBlockId % BPB) / (8 * sizeof(char))] | bit;
      bm->write_block(bBlockId, buf);
      break;
    }
  }
  if (isFree)
  {
    struct inode newInode;
    newInode.type = type;
    newInode.size = 0;
    put_inode(i, &newInode);
    return i;
  }
  return 0;
}

void inode_manager::free_inode(uint32_t inum)
{
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode *ino = get_inode(inum);
  ino->type = 0;
  put_inode(inum, ino);
  bool isFree = false;
  char buf[BLOCK_SIZE];
  blockid_t iBlockId = IBLOCK(inum, bm->sb.nblocks);
  blockid_t bBlockId = BBLOCK(iBlockId);
  bm->read_block(bBlockId, buf);
  char bit = 0x1 << ((8 * sizeof(char)) - ((iBlockId % BPB) % (8 * sizeof(char))) - 1);
  isFree = !(buf[(iBlockId % BPB) / (8 * sizeof(char))] & bit);
  if (isFree)
    return;
  buf[(iBlockId % BPB) / (8 * sizeof(char))] = buf[(iBlockId % BPB) / (8 * sizeof(char))] & (bit ^ 0xff);
  bm->write_block(bBlockId, buf);
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  /*
   * your code goes here.
   */
  ino = new struct inode;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  memcpy(ino, (struct inode *)buf + inum % IPB, sizeof(struct inode));
  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  if (buf_out == NULL || size == NULL)
    return;

  struct inode *ino = get_inode(inum);

  if (*buf_out == NULL)
  {
    *buf_out = (char *)malloc(ino->size * sizeof(char));
    *size = ino->size;
  }
  char *begin = *buf_out;
  char buf[BLOCK_SIZE];
  unsigned int remainSize = ino->size;
  for (int i = 0; remainSize > 0 && i < NDIRECT; i++)
  {
    memset(buf, 0, BLOCK_SIZE);
    bm->read_block(ino->blocks[i], buf);
    unsigned int cpySize = MIN(remainSize, BLOCK_SIZE);
    memcpy(begin, buf, cpySize);
    begin += cpySize;
    remainSize -= cpySize;
  }

  if (remainSize > 0)
  {
    blockid_t nIndirect[NINDIRECT] = {0};
    bm->read_block(ino->blocks[NDIRECT], (char *)nIndirect);
    for (int j = 0; remainSize > 0; j++)
    {
      memset(buf, 0, BLOCK_SIZE);
      bm->read_block(nIndirect[j], buf);
      unsigned int cpySize = MIN(remainSize, BLOCK_SIZE);
      memcpy(begin, buf, cpySize);
      begin += cpySize;
      remainSize -= cpySize;
    }
  }

  time_t now;
  time(&now);
  ino->atime=(int)now;
  put_inode(inum, ino);

  delete ino;
  return;
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf
   * is larger or smaller than the size of original inode
   */
  if (buf == NULL)
    return;
  struct inode *ino = get_inode(inum);
  char *begin = (char *)buf;
  char cpyBuf[BLOCK_SIZE];
  int originSize = size;

  int nowBlockNum = ino->size / BLOCK_SIZE + (ino->size % BLOCK_SIZE > 0 ? 1 : 0);
  int willBlockNum = size / BLOCK_SIZE + (size % BLOCK_SIZE > 0 ? 1 : 0);

  if (nowBlockNum < willBlockNum)
  {
    if (nowBlockNum <= NDIRECT && willBlockNum <= NDIRECT)
    {
      for (int i = nowBlockNum; i < willBlockNum; i++)
        ino->blocks[i] = bm->alloc_block();
    }
    if (nowBlockNum <= NDIRECT && willBlockNum > NDIRECT)
    {
      for (int i = nowBlockNum; i < NDIRECT; i++)
        ino->blocks[i] = bm->alloc_block();
      ino->blocks[NDIRECT] = bm->alloc_block();
      blockid_t nIndirect[NINDIRECT] = {0};
      for (int i = 0; i < willBlockNum - NDIRECT; i++)
        nIndirect[i] = bm->alloc_block();
      bm->write_block(ino->blocks[NDIRECT], (const char *)nIndirect);
    }
    if (nowBlockNum > NDIRECT && willBlockNum > NDIRECT)
    {
      blockid_t nIndirect[NINDIRECT] = {0};
      bm->read_block(ino->blocks[NDIRECT], (char *)nIndirect);
      for (int i = nowBlockNum - NDIRECT; i < willBlockNum - NDIRECT; i++)
        nIndirect[i] = bm->alloc_block();
      bm->write_block(ino->blocks[NDIRECT], (const char *)nIndirect);
    }
  }

  if (nowBlockNum > willBlockNum)
  {
    if (willBlockNum <= NDIRECT && nowBlockNum <= NDIRECT)
    {
      for (int i = willBlockNum; i < nowBlockNum; i++)
        bm->free_block(ino->blocks[i]);
    }
    if (willBlockNum <= NDIRECT && nowBlockNum > NDIRECT)
    {
      for (int i = nowBlockNum; i < NDIRECT; i++)
        bm->free_block(ino->blocks[i]);
      blockid_t nIndirect[NINDIRECT] = {0};
      bm->read_block(ino->blocks[NDIRECT], (char *)nIndirect);
      for (int i = 0; i < nowBlockNum - NDIRECT; i++)
        bm->free_block(nIndirect[i]);
      bm->free_block(ino->blocks[NDIRECT]);
    }
    if (willBlockNum > NDIRECT && nowBlockNum > NDIRECT)
    {
      blockid_t nIndirect[NINDIRECT] = {0};
      bm->read_block(ino->blocks[NDIRECT], (char *)nIndirect);
      for (int i = willBlockNum - NDIRECT; i < nowBlockNum - NDIRECT; i++)
        bm->free_block(nIndirect[i]);
    }
  }

  for (int i = 0; size > 0 && i < NDIRECT; i++)
  {
    memset(cpyBuf, 0, BLOCK_SIZE);
    unsigned int cpySize = MIN(size, BLOCK_SIZE);
    memcpy(cpyBuf, begin, cpySize);
    bm->write_block(ino->blocks[i], cpyBuf);
    begin += cpySize;
    size -= cpySize;
  }

  if (size != 0)
  {
    blockid_t nIndirect[NINDIRECT] = {0};
    bm->read_block(ino->blocks[NDIRECT], (char *)nIndirect);
    for (int j = 0; size > 0; j++)
    {
      memset(cpyBuf, 0, BLOCK_SIZE);
      unsigned int cpySize = MIN(size, BLOCK_SIZE);
      memcpy(cpyBuf, begin, cpySize);
      bm->write_block(nIndirect[j], cpyBuf);
      begin += cpySize;
      size -= cpySize;
    }
  }

  ino->size = originSize;
  time_t now;
  time(&now);
  ino->ctime=(int)now;
  ino->mtime=(int)now;
  put_inode(inum, ino);
  delete ino;
  return;
}

void inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode *ino;
  ino = get_inode(inum);
  a.type = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;
  delete ino;
  return;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode *ino = get_inode(inum);
  unsigned int remainSize = ino->size;
  for (int i = 0; remainSize > 0 && i < NDIRECT; i++)
  {
    bm->free_block(ino->blocks[i]);
    unsigned int minusSize = MIN(remainSize, BLOCK_SIZE);
    remainSize -= minusSize;
  }

  if (remainSize > 0)
  {
    blockid_t nIndirect[NINDIRECT] = {0};
    bm->read_block(ino->blocks[NDIRECT], (char *)nIndirect);
    for (int j = 0; remainSize > 0; j++)
    {
      bm->free_block(nIndirect[j]);
      unsigned int minusSize = MIN(remainSize, BLOCK_SIZE);
      remainSize -= minusSize;
    }
    bm->free_block(ino->blocks[NDIRECT]);
  }

  free_inode(inum);
  delete ino;
  return;
}
