#ifndef HDFS_FILE_INFO_H
#define HDFS_FILE_INFO_H

#include "hdfs.h"
#include "ruby.h"


static VALUE c_file_info_file;
static VALUE c_file_info_directory;


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


void init_HDFS_File_Info(VALUE parent);

VALUE wrap_hdfsFileInfo(hdfsFileInfo* info);

#endif /* HDFS_FILE_INFO_H */
