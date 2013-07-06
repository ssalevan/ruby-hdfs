#ifndef HDFS_FILE_H
#define HDFS_FILE_H

#include "hdfs.h"
#include "ruby.h"


static VALUE c_file;


typedef struct FileData {
  hdfsFS fs;
  hdfsFile file;
} FileData;


void init_HDFS_File(VALUE parent);

#endif /* HDFS_FILE_H */
