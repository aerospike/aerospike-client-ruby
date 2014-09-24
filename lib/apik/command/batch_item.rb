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

require 'thread'

require 'apik/record'

require 'apik/command/command'

module Apik

  protected

  class BatchItem

    def self.generate_map(keys)
      keyMap = {}
      keys.each_with_index do |key, i|
        item = keyMap[key.digest]
        unless item
          item = BatchItem.new(i)
          keyMap[key.digest] = item
        else
          item.add_duplicate(i)
        end
      end

      keyMap
    end


    def initialize(index)
      @index = index
    end

    def add_duplicate(idx)
      unless @duplicates
        @duplicates = []
        @duplicates << @index
        @index = 0
      end

      @duplicates << idx
    end

    def get_index
      return @index unless @duplicates

      r = @duplicates[@index]
      @index+=1
      return r
    end

  end # class

end # module
