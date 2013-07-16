#ifndef HDFS_FILE_INFO_H
#define HDFS_FILE_INFO_H

#include "hdfs.h"
#include "ruby.h"


typedef struct FileInfo {
  char* mName;         /* the name of the file */
  tTime mLastMod;      /* the last modification time for the file in seconds */
  tOffset mSize;       /* the size of the file in bytes */
  short mReplication;  /* the count of replicas */
  tOffset mBlockSize;  /* the block size for the file */
  char* mOwner;        /* the owner of the file */
  char* mGroup;        /* the group associated with the file */
  short mPermissions;  /* the permissions associated with the file */
  tTime mLastAccess;   /* the last access time for the file in seconds */
} FileInfo;


VALUE wrap_hdfsFileInfo(hdfsFileInfo* info);

void init_file_info(VALUE parent);

#endif /* HDFS_FILE_INFO_H */
