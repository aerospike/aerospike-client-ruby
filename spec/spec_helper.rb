if ENV['CI']
  require 'simplecov'
  require 'coveralls'
  SimpleCov.formatter = Coveralls::SimpleCov::Formatter
  SimpleCov.start do
    add_filter 'spec'
  end
end

require 'rspec'
require "support/utils"

$:.unshift((Pathname(__FILE__).dirname.parent + 'lib').to_s)

require 'apik'

# Log to a StringIO instance to make sure no exceptions are rasied by our
# logging code.
Apik.logger = Logger.new(StringIO.new, Logger::DEBUG)

RSpec.configure do |config|

end
