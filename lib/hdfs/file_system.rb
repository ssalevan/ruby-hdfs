module HDFS
  class FileSystem

    # Recursively scans HDFS::FileInfo objects under the supplied path using
    # the supplied match proc to determine matching files.  If a match is
    # found, calls the supplied block.
    #
    # @param path [String] DFS path to search for files under
    # @param match_proc [Proc] determines if a HDFS::FileInfo object is a match
    # @param block [Block] called if an HDFS::FileInfo object matches
    def find path, match_proc, &block
      dir_listing = ls path
      dir_listing.each do |item|
        if match_proc.call item
          block.call item
        end
        if item.is_directory?
          find item, compare_proc &block
        end
      end
    end # def find path, &block

    # Recursively scans HDFS::FileInfo objects under the supplied path using
    # the supplied match proc to determine matching files, returning a list of
    # all matches.
    #
    # @param path [String] DFS path to search for files underå
    # @param match_proc [Proc] determines if a HDFS::FileInfo object is a match
    def find_all path, match_proc
      dir_listing = ls path
      dir_listing.map do |item|
        found_items = []
        if match_proc.call item
          found_items << item
        end
        if item.is_directory?
          found_items << find_all(item, &block)
        end
      end.flat_map
    end # def find_all path, &block

    # Reads the full contents of a DFS file, returning the contents as a
    # String.  This method is not recommended for large files, as it will be
    # bound by memory on the local system.
    #
    # @param path 
    def read_all path
      file = open path, 'r'
      size = stat(path).size
      contents = ""
      while file.tell < size
        contents << file.read
      end
      file.close
      contents
    end # def read_all path

    # Ensures that a file path exists on a DFS.
    #
    # @param path [String] DFS path to ensure file under.
    def touch path
      if not exist? path
        new_file = open path, 'w'
        new_file.close
      end
      true
    end # def touch path

    def walk path, &block
      dir_listing = ls path
      components = dir_listing.reduce({:files => [],
          :directories => []}) do |memo, item|
        if item.is_directory?
          memo[:directories] << item
          walk item.name, &block
        else
          memo[:files] << item
        end
        memo
      end
      block.call path, components[:directories], components[:files]
    end # def walk path

  end # class FileSystem
end # module HDFS
