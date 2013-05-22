$:.unshift File.join File.dirname(__FILE__), '.lib'
require 'gem_command'
require 'rake'

namespace :gem do
  task :build do
    GemCommand.new 'BuildCommand', 
                   Dir.glob(File.join(File.dirname(__FILE__), '*.gemspec')).first
  end

  task :release do
    Rake::Task['gem:build'].invoke

    # realse latest gem
    GemCommand.new 'PushCommand',
                   Dir.glob(File.join(File.dirname(__FILE__), '*.gem')).sort.last
  end
end