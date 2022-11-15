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

require 'thread'

require 'aerospike/record'

require 'aerospike/command/command'

module Aerospike

  private

  class StreamCommand < MultiCommand #:nodoc:

    def parse_group(receive_size)
      @data_offset = 0

      while @data_offset < receive_size
        read_bytes(MSG_REMAINING_HEADER_SIZE)
        result_code = @data_buffer.read(5).ord & 0xFF

        # The only valid server return codes are "ok" and "not found".
        # If other return codes are received, then abort the batch.
        case result_code
        when Aerospike::ResultCode::OK
          # noop
        when Aerospike::ResultCode::PARTITION_UNAVAILABLE
          # noop
        when Aerospike::ResultCode::KEY_NOT_FOUND_ERROR
          # consume the rest of the input buffer from the socket
          read_bytes(receive_size - @data_offset) if @data_offset < receive_size
          return nil
        else
          raise Aerospike::Exceptions::Aerospike.new(result_code)
        end

        info3 = @data_buffer.read(3).ord

        # If cmd is the end marker of the response, do not proceed further
        return false if (info3 & INFO3_LAST) == INFO3_LAST

        generation = @data_buffer.read_int32(6)
        expiration = @data_buffer.read_int32(10)
        field_count = @data_buffer.read_int16(18)
        op_count = @data_buffer.read_int16(20)
        key = parse_key(field_count)

        # If cmd is the end marker of the response, do not proceed further
        if (info3 & INFO3_PARTITION_DONE) != 0
          # When an error code is received, mark partition as unavailable
          # for the current round. Unavailable partitions will be retried
          # in the next round. Generation is overloaded as partitionId.
          if result_code != 0
            @tracker&.partition_unavailable(@node_partitions, generation)
          end

          next
        end

        if result_code == 0
          if @recordset.active?
            @recordset.records.enq(parse_record(key, op_count, generation, expiration))
          else
            expn = @recordset.is_scan? ? SCAN_TERMINATED_EXCEPTION : QUERY_TERMINATED_EXCEPTION
            raise expn
          end

          # UDF results do not return a key
          @tracker&.set_last(@node_partitions, key, key.bval) if key
        end
      end # while

      true
    end

  end # class

end # module
