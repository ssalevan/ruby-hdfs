$:.unshift File.join File.dirname(__FILE__), '.lib'
require 'gem_command'
require 'semantic'
require 'rugged'
require 'rake'

namespace :version do
  task :increment, :version_type, :version_types, :version_file do |task, args|
    args.with_defaults version_type:  'patch',
                       version_types: %w[major minor patch],
                       version_file:  File.join(File.dirname(__FILE__), 'VERSION')

    unless args.version_types.include? args.version_type
      abort "invalid version_type: `#{args.version_type}`, valid version types include: #{args.version_types.join(', ')}"
    end

    version = Semantic::Version.new(File.read(args.version_file).chomp).to_hash
    version[args.version_type.to_sym] += 1
    
    File.open args.version_file, 'w' do |file|
      file.write version.values.compact.join '.'
    end
  end
end

namespace :git do
  task :tag do
    repo    = Rugged::Repository.new File.dirname __FILE__
    spec    = eval File.read Dir.glob(File.join(File.dirname(__FILE__), '*.gemspec')).first
    tag     = 'v' + spec.version.version

    puts 'creating tag: ' + tag
    Rugged::Tag.create repo, name:   tag,
                             target: repo.head.target
  end
end

namespace :gem do
  task :build do
    GemCommand.new 'BuildCommand', 
                   Dir.glob(File.join(File.dirname(__FILE__), '*.gemspec')).first
  end

  task :release, :version_type do |task, args|
    Rake::Task['version:increment'].invoke args.version_type
    Rake::Task['git:tag'].invoke
    Rake::Task['gem:build'].invoke

    # realse latest gem
    GemCommand.new 'PushCommand',
                   Dir.glob(File.join(File.dirname(__FILE__), '*.gem')).sort.last
  end
end