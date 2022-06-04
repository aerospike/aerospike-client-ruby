# Copyright 2014-2020 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'rspec'
require 'support/utils'
require 'support/matchers'

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
  # skip tests that require security features of Enterprise edition server
  # config.filter_run_excluding security: true
  config.filter_run_including focus: true
  config.run_all_when_everything_filtered = true

  config.after(:suite) do
    Support.client && Support.client.close
  end
end
