# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
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

require 'apik/command/batch_command'

module Apik

  protected

  class BatchCommandGet < BatchCommand

    def initialize(node, batch_namespace, policy, key_map, bin_names, records, read_attr)
      super(node)

      @batch_namespace = batch_namespace
      @policy = policy
      @key_map = key_map
      @bin_names = bin_names
      @records = records
      @read_attr = read_attr
    end

    def write_buffer
      set_batch_get(@batch_namespace, @bin_names, @read_attr)
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parse_record_results(receive_size)
      #Parse each message response and add it to the result array
      @data_offset = 0

      while @data_offset < receive_size
        read_bytes(MSG_REMAINING_HEADER_SIZE)
        result_code = @data_buffer.read(5).ord & 0xFF

        # The only valid server return codes are "ok" and "not found".
        # If other return codes are received, then abort the batch.
        if result_code != 0 && result_code != Apik::ResultCode::KEY_NOT_FOUND_ERROR
          raise Apik::Exceptions::Aerospike(result_code)
        end

        info3 = @data_buffer.read(3).ord

        # If cmd is the end marker of the response, do not proceed further
        return false if (info3 & INFO3_LAST) == INFO3_LAST

        generation = @data_buffer.read_int32(6).ord
        expiration = @data_buffer.read_int32(10).ord
        field_count = @data_buffer.read_int16(18).ord
        op_count = @data_buffer.read_int16(20).ord
        key = parse_key(field_count)
        item = @key_map[key.digest]

        if item
          if result_code == 0
            index = item.index
            @records[index] = parse_record(key, op_count, generation, expiration)
          end
        else
          Apik.logger.debug("Unexpected batch key returned: #{key.namespace}, #{key.digest}")
        end

      end # while

      true
    end

    # Parses the given byte buffer and populate the result object.
    # Returns the number of bytes that were parsed from the given buffer.
    def parse_record(key, op_count, generation, expiration)
      bins = nil
      duplicates = nil

      for i in 0...op_count
        raise Apik::Exceptions::QueryTerminated.new unless cmd.valid?

        read_bytes(8)

        op_size = @data_buffer.read_int32(0).ord
        particle_type = @data_buffer.read(5).ord
        version = @data_buffer.read(6).ord
        name_size = @data_buffer.read(75).ord

        read_bytes(name_size)
        name = @data_buffer.read(0, name_size).force_encoding('utf-8')

        particle_bytes_size = op_size - (4 + name_size)
        read_bytes(particle_bytes_size)
        value = Value.bytes_to_particle(particle_type, @data_buffer, 0, particle_bytes_size)

        # Currently, the batch command returns all the bins even if a subset of
        # the bins are requested. We have to filter it on the client side.
        # TODO: Filter batch bins on server!
        if !bin_names || @bin_names.any?{|bn| bn == name}
          vmap = nil

          if version > 0 || duplicates
            unless duplicates
              duplicates = []
              duplicates << bins
              bins = nil

              for j in 0...version
                duplicates << nil
              end
            else
              for j in duplicates.length..version
                duplicates << nil
              end
            end

            vmap = duplicates[version]
            unless vmap
              vmap = {}
              duplicates[version] = vmap
            end
          else
            unless bins
              bins = {}
            end
            vmap = bins
          end
          vmap[name] = value
        end
      end

      # Remove nil duplicates just in case there were holes in the version number space.
      # TODO: this seems to be a bad idea; O(n) algorithm after another O(n) algorithm
      duplicates.compact! if duplicates

      Record.new(@node, key, bins, duplicates, generation, expiration)
    end

  end # class

end # module
