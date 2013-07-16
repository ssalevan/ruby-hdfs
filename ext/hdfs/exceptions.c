#include "exceptions.h"


void init_exceptions(VALUE parent) {
  e_dfs_exception = rb_define_class_under(parent, "DFSException",
      rb_eStandardError);
  e_connect_error = rb_define_class_under(parent, "ConnectError",
      e_dfs_exception);

  e_file_error = rb_define_class_under(parent, "FileError", e_dfs_exception);
  e_could_not_open = rb_define_class_under(parent, "CouldNotOpenFileError",
      e_file_error);
  e_does_not_exist = rb_define_class_under(parent, "DoesNotExistError",
      e_file_error);
}
