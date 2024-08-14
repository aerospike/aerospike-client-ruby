source "https://rubygems.org"

group :test do
  gem "rspec", "~> 3.4"
  gem "codecov", require: false
end

group :development do
  gem "pry"
  gem "stackprof", "~> 0.2.22"
  gem "rspec-stackprof"
  gem "hexdump", "~> 1.0"
  gem "ruby-lsp", require: false
  gem "rubocop", require: false
  gem "rubocop-performance", require: false
  gem "rubocop-rake", require: false
  gem "rubocop-rspec", require: false
end

gem "base64"
gem "bcrypt"
gem "msgpack", "~> 1.2"
gem "rake"

platforms :mri, :rbx do
  gem "openssl"
end

platforms :jruby do
  gem "jruby-openssl", "~> 0.10", require: "openssl"
end

gemspec
