# encoding: utf-8

require 'rspec'
require "support/utils"

require 'simplecov'
SimpleCov.start

if ENV["CODECOV_ENABLED"] == "true"
  require 'codecov'
  SimpleCov.formatter = SimpleCov::Formatter::Codecov
end

$:.unshift((Pathname(__FILE__).dirname.parent + 'lib').to_s)

require 'aerospike'

# Log to a StringIO instance to make sure no exceptions are rasied by our
# logging code.
Aerospike.logger = Logger.new(StringIO.new, Logger::DEBUG)
# Aerospike.logger = Logger.new(STDOUT, Logger::DEBUG)

RSpec.configure do |config|
  # skip security tests; they are only available on enterprise edition
  config.filter_run_excluding skip_security: true
  config.filter_run_including focus: true
  config.run_all_when_everything_filtered = true

  config.after(:suite) do
    Support.client && Support.client.close
  end
end
