source "https://rubygems.org"

group :test do
  gem 'rspec', '~> 3.4'
  gem 'simplecov', :require => false
end

group :development do
  gem 'rubocop', require: false
end

gem 'rake'
gem "jruby-openssl", :platforms => :jruby
gem 'msgpack-jruby', :require => 'msgpack', :platforms => :jruby
gem 'msgpack', '~> 1.0', :platforms => [:mri, :rbx]
gem 'bcrypt'

gemspec
