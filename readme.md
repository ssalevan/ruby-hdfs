# ruby libhdfs client

### requirements
  - ruby    (1.9.2 <=)
  - java    (1.6 <=)
  - libhdfs (2.x)

### installation
```
gem install ruby-hdfs-cdh4
```

this gem provides defaults for installation on machines using cdh4 and the hadoop-libhdfs cloudera package, but the following enviorment variables are available for configuration.

  - HADOOP_ENV
  - JAVA_HOME
  - JAVA_LIB

### usage
to setup your classpath on chd4 machines

```ruby
require 'hdfs/classpath'
```
