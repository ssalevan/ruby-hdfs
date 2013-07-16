#ifndef HDFS_FILE_SYSTEM_H
#define HDFS_FILE_SYSTEM_H

#include "hdfs.h"
#include "ruby.h"


typedef struct FSData {
  hdfsFS fs;
} FSData;


void init_file_system(VALUE parent);

#endif /* HDFS_FILE_SYSTEM_H */
