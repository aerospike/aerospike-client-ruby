# encoding: utf-8
module Aerospike

  module Loggable

    def self.log_operations(prefix, ops, runtime)
      indent  = " "*prefix.length
      if ops.length == 1
        Aerospike.logger.debug([ prefix, ops.first.log_inspect, "runtime: #{runtime}" ].join(' '))
      else
        first, *middle, last = ops
        Aerospike.logger.debug([ prefix, first.log_inspect ].join(' '))
        middle.each { |m| Aerospike.logger.debug([ indent, m.log_inspect ].join(' ')) }
        Aerospike.logger.debug([ indent, last.log_inspect, "runtime: #{runtime}" ].join(' '))
      end
    end

    def self.debug(prefix, payload, runtime)
      Aerospike.logger.debug([ prefix, payload, "runtime: #{runtime}" ].join(' '))
    end

    def self.warn(prefix, payload, runtime)
      Aerospike.logger.warn([ prefix, payload, "runtime: #{runtime}" ].join(' '))
    end

    def self.info(prefix, payload, runtime)
      Aerospike.logger.info([ prefix, payload, "runtime: #{runtime}" ].join(' '))
    end

    def self.error(prefix, payload, runtime)
      Aerospike.logger.error([ prefix, payload, "runtime: #{runtime}" ].join(' '))
    end

    def logger
      return @logger if defined?(@logger)
      @logger = rails_logger || default_logger
    end

    def rails_logger
      defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger
    end

    def default_logger
      logger = Logger.new(STDOUT)
      logger.level = Logger::ERROR
      logger
    end

    def logger=(logger)
      @logger = logger
    end

  end # module

end # module
