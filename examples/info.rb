# Copyright 2013-2014 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'rubygems'
require 'aerospike'
require './shared/shared'

include Aerospike

def main
  options = {
    :port => 3000,
    :value => nil,
    :showUsage => false,
  }

  opt_parser = OptionParser.new do |opts|
    opts.banner = "Usage: asinfo [options]"

    opts.on("-h", "--host HOST", "Aerospike server seed hostnames or IP addresses") do |v|
      options[:host] = v
    end
    opts.on("-p", "--port PORT", "Aerospike server seed hostname or IP address port number.") do |v|
      options[:port] = v.to_i
    end

    opts.on("-v", "--value VALUE", "(fetch single value - default all)") do |v|
      options[:value] = v
    end

    opts.on("-u", "--usage", "Show usage information.") do |v|
      puts opts
      exit
    end
  end

  opt_parser.parse!
  client = options[:host] ? Client.new(Host.new(options[:host], options[:port])) : Client.new

  info_map = options[:value].nil? ? client.request_info : client.request_info(options[:value])
  info_map.each_with_index do |vals, i|
    k, v = vals
    puts("#{i} :  #{k}\n     #{v}\n\n")
  end

  client.close
end

main
