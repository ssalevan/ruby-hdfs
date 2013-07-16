#ifndef HDFS_FILE_H
#define HDFS_FILE_H

#include "hdfs.h"
#include "ruby.h"


typedef struct FileData {
  hdfsFS fs;
  hdfsFile file;
} FileData;


void init_file(VALUE parent);

#endif /* HDFS_FILE_H */
