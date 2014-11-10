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

require 'thread'

require 'aerospike/record'

require 'aerospike/command/command'

module Aerospike

  private

  class BatchCommand < Command

    def initialize(node)
      super(node)

      @valid = true
      @mutex = Mutex.new
      @records = Queue.new

      self
    end

    def parse_result
      # Read socket into receive buffer one record at a time.  Do not read entire receive size
      # because the receive buffer would be too big.
      status = true

      while status
        # Read header.
        read_bytes(8)

        size = @data_buffer.read_int64(0)
        receive_size = size & 0xFFFFFFFFFFFF

        if receive_size > 0
          status = parse_record_results(receive_size)
        else
          status = false
        end
      end
    end

    def parse_key(field_count)
      digest = nil
      namespace = nil
      set_name = nil
      user_key = nil

      for i in 0...field_count
        read_bytes(4)

        fieldlen = @data_buffer.read_int32(0)
        read_bytes(fieldlen)

        fieldtype = @data_buffer.read(0).ord
        size = fieldlen - 1

        case fieldtype
        when Aerospike::FieldType::DIGEST_RIPE
          digest = @data_buffer.read(1, size)
        when Aerospike::FieldType::NAMESPACE
          namespace = @data_buffer.read(1, size).force_encoding('utf-8')
        when Aerospike::FieldType::TABLE
          set_name = @data_buffer.read(1, size).force_encoding('utf-8')
        when Aerospike::FieldType::KEY
          user_key = Aerospike::bytes_to_key_value(@data_buffer.read(1).ord, @data_buffer, 2, size-1)
        end
      end

      Aerospike::Key.new(namespace, set_name, user_key, digest)
    end

    # Parses the given byte buffer and populate the result object.
    # Returns the number of bytes that were parsed from the given buffer.
    def parse_record(key, op_count, generation, expiration)
      bins = nil
      duplicates = nil

      for i in 0...op_count
        raise Aerospike::Exceptions::QueryTerminated.new unless valid?

        read_bytes(8)

        op_size = @data_buffer.read_int32(0).ord
        particle_type = @data_buffer.read(5).ord
        version = @data_buffer.read(6).ord
        name_size = @data_buffer.read(7).ord

        read_bytes(name_size)
        name = @data_buffer.read(0, name_size).force_encoding('utf-8')

        particle_bytes_size = op_size - (4 + name_size)
        read_bytes(particle_bytes_size)
        value = Aerospike.bytes_to_particle(particle_type, @data_buffer, 0, particle_bytes_size)

        # Currently, the batch command returns all the bins even if a subset of
        # the bins are requested. We have to filter it on the client side.
        # TODO: Filter batch bins on server!
        if !@bin_names || @bin_names.any?{|bn| bn == name}
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

    def read_bytes(length)
      if length > @data_buffer.length
        # Corrupted data streams can result in a huge length.
        # Do a sanity check here.
        if length > Aerospike::Buffer::MAX_BUFFER_SIZE
          raise Aerospike::Exceptions::Parse.new("Invalid read_bytes length: #{length}")
        end
        @data_buffer = Buffer.new(length)
      end

      @conn.read(@data_buffer, length)
      @data_offset += length
    end

    def stop
      @mutex.synchronize do
        @valid = false
      end
    end

    def valid?
      res = nil
      @mutex.synchronize do
        res = @valid
      end

      res
    end

  end # class

end # module
