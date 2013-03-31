#include "ruby.h"
#include "hdfs.h"

#include <assert.h>
#include <string.h>
#include <ctype.h>

static VALUE m_hadoop;
static VALUE m_dfs;
static VALUE c_file;
static VALUE c_file_info;
static VALUE c_file_system;
static VALUE c_file_type;
static VALUE c_file_type_file;
static VALUE c_file_type_directory;
static VALUE e_dfs_exception;
static VALUE e_file_error;
static VALUE e_could_not_open;
static VALUE e_does_not_exist;

static VALUE file_type_file;
static VALUE file_type_directory;

static const HDFS_DEFAULT_BLOCK_SIZE = 134217728;
static const char* HDFS_DEFAULT_HOST = "localhost";
static const int HDFS_DEFAULT_PORT = 9000;

/*
 * Data structs
 */

typedef struct FSData {
  hdfsFS fs;
} FSData;

typedef struct FileData {
  hdfsFS fs;
  hdfsFile file;
} FileData;

typedef struct FileInfo {
  hdfsFileInfo* file_info;
} FileInfo;

void free_fs_data(FSData* data) {
  if (data && data->fs != NULL) {
    hdfsDisconnect(data->fs);
    data->fs = NULL;
  }
}

void free_file_data(FileData* data) {
  if (data && data->file != NULL) {
    hdfsCloseFile(data->fs, data->file);
    data->file = NULL;
  }
}

void free_file_info(FileInfo* file_info) {
  if (file_info) {
    file_info->file_info = NULL;
  }
}

/*
 * File system interface
 */

VALUE HDFS_File_System_alloc(VALUE klass) {
  FSData* data = ALLOC_N(FSData, 1);
  data->fs = NULL;
  VALUE instance = Data_Wrap_Struct(klass, NULL, free_fs_data, data);
  return instance;
}

/**
 * call-seq:
 *    hdfs.new -> hdfs
 *
 * Creates a new HDFS client connection.
 */
VALUE HDFS_File_System_initialize(VALUE self, VALUE host, VALUE port) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);
  data->fs = hdfsConnect(
    RTEST(host) ? RSTRING_PTR(host) : HDFS_DEFAULT_HOST,
    RTEST(port) ? NUM2INT(port) : HDFS_DEFAULT_PORT);  
  return self;
}

/**
 * call-seq:
 *    hdfs.disconnect -> nil
 *
 * Disconnects the client connection.
 */
VALUE HDFS_File_System_disconnect(VALUE self) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);
  if (data->fs != NULL) {
    hdfsDisconnect(data->fs);
    data->fs = NULL;
  }
  return Qnil;
}

VALUE HDFS_File_System_delete(VALUE self, VALUE path) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);
  int value = hdfsDelete(data->fs, RSTRING_PTR(path));
  return value == 0 ? Qtrue : Qfalse;
}

VALUE HDFS_File_System_rename(VALUE self, VALUE current_path, VALUE destination_path) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);
  int value = hdfsRename(data->fs, RSTRING_PTR(current_path), RSTRING_PTR(destination_path));
  return value == 0 ? Qtrue : Qfalse;
}

VALUE HDFS_File_System_exist(VALUE self, VALUE path) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);
  int value = hdfsExists(data->fs, RSTRING_PTR(path));
  return value == 0 ? Qtrue : Qfalse;
}

VALUE HDFS_File_System_create_directory(VALUE self, VALUE path) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);
  int value = hdfsCreateDirectory(data->fs, RSTRING_PTR(path));
  return value == 0 ? Qtrue : Qfalse;
}

VALUE HDFS_File_System_stat(VALUE self, VALUE path) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);
  hdfsFileInfo* file_info = hdfsGetPathInfo(data->fs, RSTRING_PTR(path));
  if (file_info == NULL) {
    rb_raise(e_does_not_exist, "File does not exist: %s", RSTRING_PTR(path));
    return Qnil;
  }
  FileInfo* file_info = ALLOC_N(FileInfo, 1);
  file_info->file_info = file_info;
  return Data_Wrap_Struct(c_file_info, NULL, free_file_info, file_info);
}

/**
 * call-seq:
 *    hdfs.open -> file
 *
 * Opens a file.
 */
VALUE HDFS_File_System_open(VALUE self, VALUE path, VALUE mode, VALUE options) {
  FSData* data = NULL;
  Data_Get_Struct(self, FSData, data);

  int flags = 0;
  if (strcmp("r", StringValuePtr(mode)) == 0) {
    flags = O_RDONLY;
  } else if (strcmp("w", StringValuePtr(mode)) == 0) {
    flags = O_WRONLY;
  } else {
    rb_raise(rb_eArgError, "Mode must be 'r' or 'w'");
    return;
  }
  VALUE r_buffer_size = rb_hash_aref(options, rb_eval_string(":buffer_size"));
  VALUE r_replication = rb_hash_aref(options, rb_eval_string(":replication"));
  VALUE r_block_size = rb_hash_aref(options, rb_eval_string(":block_size"));
  hdfsFile file = hdfsOpenFile(data->fs, RSTRING_PTR(path), flags, 
    RTEST(r_buffer_size) ? NUM2INT(r_buffer_size) : 0, 
    RTEST(r_replication) ? NUM2INT(r_replication) : 0, 
    RTEST(r_block_size) ? NUM2INT(r_block_size) : HDFS_DEFAULT_BLOCK_SIZE);
  if (file == NULL) {
    rb_raise(e_could_not_open, "Could not open file %s", RSTRING_PTR(path));
    return Qnil;
  }

  FileData* file_data = ALLOC_N(FileData, 1);
  file_data->fs = data->fs;
  file_data->file = file;
  VALUE file_instance = Data_Wrap_Struct(c_file, NULL, free_file_data, file_data);
  return file_instance;
}

/*
 * File interface
 */

VALUE HDFS_File_read(VALUE self, VALUE length) {
  FileData* data = NULL;
  Data_Get_Struct(self, FileData, data);
  char* buffer = ALLOC_N(char, length);
  MEMZERO(buffer, char, length);
  tSize bytes_read = hdfsRead(data->fs, data->file, buffer, NUM2INT(length));
  if (bytes_read == -1) {
    rb_raise(e_file_error, "Failed to read data");
  }
  return rb_tainted_str_new2(buffer);
}

VALUE HDFS_File_write(VALUE self, VALUE bytes) {
  FileData* data = NULL;
  Data_Get_Struct(self, FileData, data);
  tSize bytes_written = hdfsWrite(data->fs, data->file, RSTRING_PTR(bytes), RSTRING_LEN(bytes));
  if (bytes_written == -1) {
    rb_raise(e_file_error, "Failed to write data");
  }
  return INT2NUM(bytes_written);
}

VALUE HDFS_File_tell(VALUE self) {
  FileData* data = NULL;
  Data_Get_Struct(self, FileData, data);
  tSize offset = hdfsTell(data->fs, data->file);
  if (offset == -1) {
    rb_raise(e_file_error, "Failed to read position");
  }
  return INT2NUM(offset);
}

VALUE HDFS_File_seek(VALUE self, VALUE offset) {
  FileData* data = NULL;
  Data_Get_Struct(self, FileData, data);
  int result = hdfsSeek(data->fs, data->file, NUM2INT(offset));
  return result == 0 ? Qtrue : Qfalse;
}

VALUE HDFS_File_flush(VALUE self) {
  FileData* data = NULL;
  Data_Get_Struct(self, FileData, data);
  int result = hdfsFlush(data->fs, data->file);
  if (result != 0) {
    rb_raise(e_file_error, "Flush failed");
  }
  return Qnil;
}

VALUE HDFS_File_available(VALUE self) {
  FileData* data = NULL;
  Data_Get_Struct(self, FileData, data);
  int result = hdfsAvailable(data->fs, data->file);
  if (result == -1) {
    rb_raise(e_file_error, "Failed to get available data");
  }
  return INT2NUM(result);
}

VALUE HDFS_File_close(VALUE self) {
  FileData* data = NULL;
  Data_Get_Struct(self, FileData, data);
  if (data->file != NULL) {
    hdfsCloseFile(data->fs, data->file);
    data->file = NULL;
  }
  return Qnil;
}

/*
 * Extension initialization
 */
 
void Init_hdfs() {
  m_hadoop = rb_define_module("Hadoop");
  m_dfs = rb_define_module_under(m_hadoop, "DFS");

  c_file_system = rb_define_class_under(m_dfs, "FileSystem", rb_cObject);
  rb_define_alloc_func(c_file_system, HDFS_File_System_alloc);
  rb_define_method(c_file_system, "initialize", HDFS_File_System_initialize, 2);
  rb_define_method(c_file_system, "disconnect", HDFS_File_System_disconnect, 0);
  rb_define_method(c_file_system, "open", HDFS_File_System_open, 3);
  rb_define_method(c_file_system, "delete", HDFS_File_System_delete, 1);
  rb_define_method(c_file_system, "rename", HDFS_File_System_rename, 2);
  rb_define_method(c_file_system, "exist?", HDFS_File_System_exist, 1);
  rb_define_method(c_file_system, "create_directory", HDFS_File_System_create_directory, 1);
  rb_define_method(c_file_system, "stat", HDFS_File_System_stat, 1);

  c_file = rb_define_class_under(m_dfs, "File", rb_cObject);
  rb_define_method(c_file, "read", HDFS_File_read, 1);
  rb_define_method(c_file, "write", HDFS_File_write, 1);
  rb_define_method(c_file, "<<", HDFS_File_write, 1);
  rb_define_method(c_file, "seek", HDFS_File_seek, 1);
  rb_define_method(c_file, "tell", HDFS_File_tell, 0);
  rb_define_method(c_file, "flush", HDFS_File_flush, 0);
  rb_define_method(c_file, "available", HDFS_File_available, 0);
  rb_define_method(c_file, "close", HDFS_File_close, 0);

  c_file_info = rb_define_class_under(m_dfs, "FileInfo", rb_cObject);
  rb_define_method(c_file_info, "is_directory?", HDFS_File_Info_is_directory, 0);
  rb_define_method(c_file_info, "is_file?", HDFS_File_Info_is_file, 0);
  rb_define_method(c_file_info, "last_modified", HDFS_File_Info_last_modified, 0);
  rb_define_method(c_file_info, "name", HDFS_File_Info_name, 0);
  rb_define_method(c_file_info, "replication", HDFS_File_Info_replication, 0);
  rb_define_method(c_file_info, "size", HDFS_File_Info_size, 0);
  rb_define_method(c_file_info, "type", HDFS_File_Info_type, 0);

  c_file_type = rb_define_class_under(m_dfs, "FileType", rb_cObject);
  c_file_type_file = rb_define_class_under(m_dfs, "File", c_file_type);
  c_file_type_directory = rb_define_class_under(m_dfs, "Directory", c_file_type);

  e_dfs_exception = rb_define_class_under(m_dfs, "DFSException", rb_eStandardError);
  e_file_error = rb_define_class_under(m_dfs, "FileError", e_dfs_exception);  
  e_could_not_open = rb_define_class_under(m_dfs, "CouldNotOpenFileError", e_file_error);
  e_does_not_exist = rb_define_class_under(m_dfs, "DoesNotExistError", e_file_error); 
}
