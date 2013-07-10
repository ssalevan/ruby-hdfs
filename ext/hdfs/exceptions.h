#ifndef HDFS_EXCEPTIONS_H
#define HDFS_EXCEPTIONS_H

#include "ruby.h"


static VALUE e_connect_error;
static VALUE e_could_not_open;
static VALUE e_dfs_exception;
static VALUE e_does_not_exist;
static VALUE e_file_error;


void init_Exceptions(VALUE parent);

#endif /* HDFS_EXCEPTIONS_H */
