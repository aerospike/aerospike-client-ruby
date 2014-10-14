source "https://rubygems.org"

group :test do
  gem "rspec", "~> 2.13"
  if ENV["CI"]
  else
    gem 'simplecov', :require => false
  end
end

gem "rake"
gem "jruby-openssl", :platforms => :jruby
gem 'msgpack-jruby', :require => 'msgpack', :platforms => :jruby
gem 'msgpack', :platforms => [:mri, :rbx]
gem 'atomic', "~> 1.1"

gemspec
