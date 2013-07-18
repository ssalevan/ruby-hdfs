#ifndef HDFS_FILE_H
#define HDFS_FILE_H

#include "hdfs.h"
#include "ruby.h"


extern VALUE c_file;

extern VALUE e_file_closed_error;
extern VALUE e_file_error;


VALUE new_HDFS_File(hdfsFile* file, hdfsFS* fs);

void init_file(VALUE parent);

#endif /* HDFS_FILE_H */
