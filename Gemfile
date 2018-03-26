source "https://rubygems.org"

group :test do
  gem 'rspec', '~> 3.4'
  gem 'codecov', require: false
end

group :development do
  gem 'rubocop', require: false
end

gem 'rake'
gem 'bcrypt'

platforms :mri, :rbx do
  gem 'msgpack', '~> 1.0'
  gem 'openssl'
end

platforms :jruby do
  gem 'msgpack-jruby', require: 'msgpack'
  gem 'jruby-openssl'
end

gemspec
