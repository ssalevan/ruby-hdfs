#ifndef HDFS_FILE_SYSTEM_H
#define HDFS_FILE_SYSTEM_H

#include "ruby.h"


extern VALUE c_file_system;

extern VALUE e_connect_error;
extern VALUE e_could_not_open;
extern VALUE e_dfs_exception;
extern VALUE e_not_connected;


void init_file_system(VALUE parent);

#endif /* HDFS_FILE_SYSTEM_H */
