# encoding: utf-8
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require "aerospike/version"

Gem::Specification.new do |s|
  s.name        = "aerospike"
  s.version     = Aerospike::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Khosrow Afroozeh", "Jan Hecking", "Sachin Venkatesha Murthy"]
  s.email       = [ "khosrow@aerospike.com", "jhecking@aerospike.com", "smurthy@aerospike.com"]
  s.homepage    = "http://www.github.com/aerospike/aerospike-client-ruby"
  s.summary     = "An Aerospike driver for Ruby."
  s.description = "Official Aerospike Client for ruby. Access your Aerospike cluster with ease of Ruby."
  s.license       = "Apache-2.0"
  s.files = Dir.glob("lib/**/*") + %w(CHANGELOG.md LICENSE README.md)
  s.require_path = "lib"
  s.required_ruby_version = '>= 2.3.0'
  s.post_install_message = "Thank you for using Aerospike!\nYou can report issues on github.com/aerospike/aerospike-client-ruby"
  s.add_dependency("msgpack", '~> 1.0')
  s.add_dependency("bcrypt", '~> 3.1')
end
