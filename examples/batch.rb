# Copyright 2012-2014 Aerospike, Inc.#
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
require 'rubygems'
require 'aerospike'
require './shared/shared'

include Aerospike
include Shared


def main
  Shared.init
  key_prefix = "batchkey"
  value_prefix = "batchvalue"
  bin_name = "batchbin"
  size = 8

  write_records(Shared.client, key_prefix, bin_name, value_prefix, size)
  batch_exists(Shared.client, key_prefix, size)
  batch_reads(Shared.client, key_prefix, bin_name, size)
  batch_read_headers(Shared.client, key_prefix, size)

  Shared.logger.info("Example finished successfully.")
end

#
# Write records individually.
#
def write_records(client, key_prefix,	bin_name, value_prefix, size)
  (1..size).each do |i|
    key = Key.new(Shared.namespace, Shared.set_name, key_prefix+i.to_s)
    bin = Bin.new(bin_name, value_prefix+i.to_s)

    Shared.logger.info("Put: ns=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin.name} value=#{bin.value}")

    client.put(key, [bin], Shared.write_policy)
  end
end

#
# Check existence of records in one batch.
#
def batch_exists(client,	key_prefix, size)
  # Batch into one call.
  keys = []
  (0...size).each do |i|
    keys << Key.new(Shared.namespace, Shared.set_name, key_prefix + (i+1).to_s)
  end

  exists_array = client.batch_exists(keys)

  (0...exists_array.length).each do |i|
    key = keys[i]
    exists = exists_array[i]
    Shared.logger.info("Record: ns=#{key.namespace} set=#{key.set_name} key=#{key.user_key} exists=#{exists}")
  end
end

#*
# Read records in one batch.
#
def batch_reads(client, key_prefix, bin_name,	size)
  # Batch gets into one call.
  keys = []
  (0...size).each do |i|
    keys << Key.new(Shared.namespace, Shared.set_name, key_prefix+(i+1).to_s)
  end

  records = client.batch_get(keys, [bin_name])


  (0...records.length).each do |i|
    key = keys[i]
    record = records[i]

    level = :err
    if record
      level = :info
      value = record.bins[bin_name]
    end

    log(level, "Record: ns=#{key.namespace} set=#{key.set_name} key=#{key.user_key} bin=#{bin_name} value=#{value}")
  end

  if records.length != size
    Shared.logger.fatal("Record size mismatch. Expected #{size}. Received #{records.length}.")
    exit
  end
end

#*
# Read record header data in one batch.
#
def batch_read_headers(client, key_prefix,	size)
  # Batch gets into one call.
  keys = []
  (0...size).each do |i|
    keys[i] = Key.new(Shared.namespace, Shared.set_name, key_prefix+(i+1).to_s)
  end

  records = client.batch_get_header(keys)

  (0...records.length).each do |i|
    key = keys[i]
    record = records[i]

    level = :err
    generation = 0
    expiration = 0

    if record && (record.generation > 0 || record.expiration > 0)
      level = :info
      generation = record.generation
      expiration = record.expiration
    end
    log(level, "Record: ns=#{key.namespace} set=#{key.set_name} key=#{key.user_key} generation=#{generation} expiration=#{expiration}")
  end

  if records.length != size
    Shared.logger.fatal("Record size mismatch. Expected #{size}. Received #{records.length}")
    exit
  end
end

def log(level, msg)
  case level
  when :err
    Shared.logger.error(msg)
  when :info
    Shared.logger.info(msg)
  else
    raise Exception.new("Log Level Not recognized.")
  end
end

main
