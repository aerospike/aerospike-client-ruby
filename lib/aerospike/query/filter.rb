# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

module Aerospike

  class Filter

    def self.Equal(bin_name, value)
      Filter.new(bin_name, value, value)
    end

    def self.Range(bin_name, from, to)
      Filter.new(bin_name, from, to)
    end

    def estimate_size
      return @name.bytesize + @begin.estimate_size + @end.estimate_size + 10
    end

    def write(buf, offset)
      # Write name.
      len = buf.write_binary(@name, offset+1)
      buf.write_byte(len, offset)
      offset += len + 1

      # Write particle type.
      buf.write_byte(@begin.type, offset)
      offset+=1

      # Write filter begin.
      len = @begin.write(buf, offset+4)
      buf.write_int32(len, offset)
      offset += len + 4

      # Write filter end.
      len = @end.write(buf, offset+4)
      buf.write_int32(len, offset)
      offset += len + 4

      offset
    end

    private

    def initialize(bin_name, begin_value, end_value)
      @name = bin_name
      @begin = Aerospike::Value.of(begin_value)
      @end = Aerospike::Value.of(end_value)
    end

  end # class

end
