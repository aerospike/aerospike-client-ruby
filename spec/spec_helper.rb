# encoding: utf-8

require 'rspec'
require "support/utils"

# require 'simplecov'
# SimpleCov.start

$:.unshift((Pathname(__FILE__).dirname.parent + 'lib').to_s)

require 'aerospike'

# Log to a StringIO instance to make sure no exceptions are rasied by our
# logging code.
Aerospike.logger = Logger.new(StringIO.new, Logger::DEBUG)

RSpec.configure do |config|

end
