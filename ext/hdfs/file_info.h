#ifndef HDFS_FILE_INFO_H
#define HDFS_FILE_INFO_H

#include "hdfs.h"
#include "ruby.h"


extern VALUE c_file_info;
extern VALUE c_file_info_file;
extern VALUE c_file_info_directory;


VALUE new_HDFS_File_Info(hdfsFileInfo* info);

void init_file_info(VALUE parent);

#endif /* HDFS_FILE_INFO_H */
