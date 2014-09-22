require "rspec/core/rake_task"

$LOAD_PATH.unshift File.expand_path("../lib", __FILE__)
require "apik/version"

task :gem => :build
task :build do
  system "gem build apik.gemspec"
end

task :install => :build do
  system "sudo gem install apik-#{Apik::VERSION}.gem"
end

task :release => :build do
  system "git tag -a v#{Apik::VERSION} -m 'Tagging #{Apik::VERSION}'"
  system "git push --tags"
  system "gem push apik-#{Apik::VERSION}.gem"
  system "rm apik-#{Apik::VERSION}.gem"
end

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = "spec/**/*_spec.rb"
end

task :default => :spec
