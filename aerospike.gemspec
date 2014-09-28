# encoding: utf-8
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require "aerospike/version"

Gem::Specification.new do |s|
  s.name        = "aerospike"
  s.version     = Aerospike::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = [ "Khosrow Afroozeh" ]
  s.email       = [ "khosrow@aerospike.com" ]
  s.homepage    = "http://www.aerospike.com"
  s.summary     = "An Aerospike driver for Ruby."
  s.description = s.summary
  s.files = Dir.glob("lib/**/*") + %w(CHANGELOG.md LICENSE README.md)
  s.require_path = "lib"
end
