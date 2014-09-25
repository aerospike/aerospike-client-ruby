# Copyright 2012-2014 Aerospike, Inc.
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

  class BatchCommandExists < BatchCommand

    def initialize(node, batchNamespace, policy, keyMap, existsArray)
      super(node)

      @batchNamespace = batchNamespace
      @policy = policy
      @keyMap = keyMap
      @existsArray = existsArray

      self
    end

    def writeBuffer
      setBatchExists(@batchNamespace)
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parseRecordResults(receiveSize)
      #Parse each message response and add it to the result array
      @dataOffset = 0

      while @dataOffset < receiveSize
        if !valid?
          raise Apik::Exceptions::QueryTerminated.new
        end

        readBytes(MSG_REMAINING_HEADER_SIZE)

        resultCode = @dataBuffer.read(5).ord & 0xFF

        # The only valid server return codes are "ok" and "not found".
        # If other return codes are received, then abort the batch.
        if resultCode != 0 && resultCode != Apik::ResultCode::KEY_NOT_FOUND_ERROR
          raise Apik::Exceptions::Aerospike.new(resultCode)
        end

        info3 = @dataBuffer.read(3).ord

        # If cmd is the end marker of the response, do not proceed further
        if info3 & INFO3_LAST == INFO3_LAST
          return false
        end

        fieldCount = @dataBuffer.read_int16(18)
        opCount = @dataBuffer.read_int16(20)

        if opCount > 0
          raise Apik::Exceptions::Parse('Received bins that were not requested!')
        end

        key = parseKey(fieldCount)
        item = @keyMap[key.digest]

        if item
          index = item.get_index

          # only set the results to true; as a result, no synchronization is needed
          @existsArray[index] = (resultCode == 0)
        else
          Apik::logger.debug("Unexpected batch key returned: #{key.namespace}, #{key.digest}")
        end

      end # while

      return true
    end


  end # class

end # module
