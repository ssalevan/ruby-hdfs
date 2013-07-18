# Searches common locations for necessary Hadoop JARs to add to CLASSPATH.
HADOOP_PREFIX = ENV['HADOOP_PREFIX'] || '/opt/hadoop'
COMMON_HADOOP_LOCATIONS = [
  '/usr/lib/hadoop',
  '/usr/lib/hadoop-hdfs',
  '/usr/lib/hadoop-0.20',
  "#{HADOOP_PREFIX}/share/hadoop-common",
  "#{HADOOP_PREFIX}/share/hadoop-hdfs",
]

ENV['CLASSPATH'] = COMMON_HADOOP_LOCATIONS.map do |lib_dir|
  Dir[File.join(lib_dir, '*.jar')] + Dir[File.join(lib_dir, '**', '*.jar')]
end.flatten.uniq.join(':')

require '_hdfs'

module HDFS
  VERSION = '0.0.9'
end # module HDFS
