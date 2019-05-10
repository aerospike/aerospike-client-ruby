# Copyright 2012-2017 Aerospike, Inc.#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

require 'optparse'

require 'rubygems'
require 'aerospike'

module Shared

  attr_accessor :write_policy, :policy, :client, :logger

  def init

    @@options = {
      # setting host as localhost returns an error on Macs, so check env variable first
      :host => ENV['AEROSPIKE_HOST'] || 'localhost',
      :port => ENV['AEROSPIKE_PORT']&.to_i || 3000,
      :namespace => 'test',
      :set => 'examples',
    }

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: benchmark [@@options]"

      opts.on("-h", "--host HOST", "Aerospike server seed hostnames or IP addresses") do |v|
        @@options[:host] = v
      end

      opts.on("-p", "--port PORT", "Aerospike server seed hostname or IP address port number.") do |v|
        @@options[:port] = v.to_i
      end

      opts.on("-n", "--namespace NAMESPACE", "Aerospike namespace.") do |v|
        @@options[:namespace] = v
      end

      opts.on("-s", "--set SET", "Aerospike set name.") do |v|
        @@options[:set] = v
      end

      opts.on("-u", "--usage", "Show usage information.") do |v|
        puts opts
        exit
      end
    end # opt_parser

    opt_parser.parse!

    @write_policy = WritePolicy.new
    @policy = Policy.new
    @logger = Logger.new(STDOUT, Logger::INFO)
    @client = host ? Client.new(Host.new(host, port)) : Client.new
  end

  def host
    @@options[:host]
  end

  def port
    @@options[:port]
  end

  def namespace
    @@options[:namespace]
  end

  def set_name
    @@options[:set]
  end

  def print_params
    @logger.info("hosts:\t\t#{@@options[:host]}")
    @logger.info("port:\t\t#{@@options[:port]}")
    @logger.info("namespace:\t#{@@options[:namespace]}")
    @logger.info("set:\t\t#{@@options[:set]}")
    @logger.info
  end

  def validate_bin(key, bin, record)
    if record.key.digest != key.digest
      @logger.fatal("key `#{key}` is not the same as key `#{record.key}`.")
      exit
    end

    if record.bins[bin.name] != bin.value
      @logger.fatal("bin `#{bin.name}: #{bin.value}` is not the same as bin `#{record.bins[bin.name]}`.")
      exit
    end
  end

end
