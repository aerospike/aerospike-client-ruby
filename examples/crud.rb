#!/usr/bin/env ruby

require 'rubygems'
require 'aerospike'
require './shared/shared'

include Aerospike
include Shared

def main
  Shared.init

  key = Key.new('test', 'test', 'key value')
  bin_map = {
    'bin1' => 'value1',
    'bin2' => 2,
    'bin4' => ['value4', {'map1' => 'map val'}],
    'bin5' => {'value5' => [124, "string value"]},
  }

  Shared.client.put(key, bin_map)
  record = Shared.client.get(key)
  record.bins['bin1'] = 'other value'

  Shared.client.put(key, record.bins)
  record = Shared.client.get(key)
  puts record.bins

  Shared.client.delete(key)
  puts Shared.client.exists(key)

  Shared.client.close
end

main