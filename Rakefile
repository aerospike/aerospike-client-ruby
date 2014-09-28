require "rspec/core/rake_task"

$LOAD_PATH.unshift File.expand_path("../lib", __FILE__)
require "aerospike/version"

task :gem => :build
task :build do
  system "gem build aerospike.gemspec"
end

task :install => :build do
  system "sudo gem install aerospike-#{Aerospike::VERSION}.gem"
end

task :release => :build do
  system "git tag -a v#{Aerospike::VERSION} -m 'Tagging #{Aerospike::VERSION}'"
  system "git push --tags"
  system "gem push aerospike-#{Aerospike::VERSION}.gem"
  system "rm aerospike-#{Aerospike::VERSION}.gem"
end

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = "spec/**/*_spec.rb"
end

task :default => :spec
