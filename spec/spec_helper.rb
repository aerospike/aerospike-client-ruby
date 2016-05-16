# encoding: utf-8

require 'rspec'
require "support/utils"

require 'simplecov'
SimpleCov.start

$:.unshift((Pathname(__FILE__).dirname.parent + 'lib').to_s)

require 'aerospike'

# Log to a StringIO instance to make sure no exceptions are rasied by our
# logging code.
Aerospike.logger = Logger.new(StringIO.new, Logger::DEBUG)
# Aerospike.logger = Logger.new(STDOUT, Logger::DEBUG)

RSpec.configure do |config|
	# skip security tests; they are only available on enterprise edition
	config.filter_run_excluding :skip_security => true

  config.after(:suite) do
    Support.client && Support.client.close
  end
end
