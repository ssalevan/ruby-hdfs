#include "ruby.h"
#include "hdfs.h"

#include "exceptions.h"
#include "file.h"
#include "file_info.h"
#include "file_system.h"


static VALUE m_hdfs;


void Init_hdfs() {
  m_hdfs = rb_define_module("HDFS");

  init_file(m_hdfs);
  init_file_info(m_hdfs);
  init_file_system(m_hdfs);
}
