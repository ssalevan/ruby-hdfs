#include "exceptions.h"


void init_Exceptions(VALUE parent) {
  e_connect_error = rb_define_class_under(m_dfs, "ConnectError",
      e_dfs_exception);
  e_could_not_open = rb_define_class_under(m_dfs, "CouldNotOpenFileError",
      e_file_error);
  e_dfs_exception = rb_define_class_under(m_dfs, "DFSException",
      rb_eStandardError);
  e_does_not_exist = rb_define_class_under(m_dfs, "DoesNotExistError",
      e_file_error);
  e_file_error = rb_define_class_under(m_dfs, "FileError", e_dfs_exception);
}
