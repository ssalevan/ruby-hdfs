#ifndef HDFS_FILE_SYSTEM_H
#define HDFS_FILE_SYSTEM_H

#include "hdfs.h"
#include "ruby.h"


static VALUE c_file_system;


typedef struct FSData {
  hdfsFS fs;
} FSData;


void init_HDFS_File_System(VALUE parent);

#endif /* HDFS_FILE_SYSTEM_H */
