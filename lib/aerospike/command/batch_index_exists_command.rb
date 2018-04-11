# Copyright 2018 Aerospike, Inc.
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

require 'aerospike/command/batch_index_command'

module Aerospike

  class BatchIndexExistsCommand < BatchIndexCommand #:nodoc:

    def initialize(node, batch, policy, results)
      super(node, batch, policy, nil, results, INFO1_READ | INFO1_NOBINDATA)
    end

    # Parse all results in the batch.  Add records to shared list.
    # If the record was not found, the bins will be nil.
    def parse_row(result_code)
      batch_index = @data_buffer.read_int32(14)
      field_count = @data_buffer.read_int16(18)
      op_count = @data_buffer.read_int16(20)

      if op_count > 0
        raise Aerospike::Exceptions::Parse.new('Received bins that were not requested!')
      end

      parse_key(field_count)
      results[batch_index] = (result_code == 0)
    end

  end # class

end # module
