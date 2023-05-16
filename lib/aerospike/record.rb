# Copyright 2014-2023 Aerospike, Inc.
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

module Aerospike

  private

  class Record

    attr_reader :key, :bins, :generation, :ttl, :node

    alias expiration ttl # for backwards compatibility

    def initialize(node, rec_key, rec_bins, rec_gen, rec_exp)
      @key = rec_key
      @bins = rec_bins
      @generation = rec_gen
      @ttl = expiration_to_ttl(rec_exp)
      @node = node
    end



    def to_s
      "key: `#{key}` bins: `#{bins}` generation: `#{generation}`, ttl: `#{ttl}`"
    end

    private

    CITRUSLEAF_EPOCH = 1262304000

    # Arguments:
    #   value: the key to retrieve the value for
    #
    # Returns:
    #   the value of the specified key, or `nil` if `@bins` is `nil`
    def get_value(value)
      unless @bins.nil?
        return @bins[value]
      end
      nil
    end

    # Converts an absolute expiration time (in seconds from citrusleaf epoch)
    # to relative time-to-live (TTL) in seconds
    def expiration_to_ttl(secs_from_epoc)
      if secs_from_epoc == 0
        Aerospike::TTL::NEVER_EXPIRE
      else
        now = Time.now.to_i - CITRUSLEAF_EPOCH
        # Record was not expired at server but if it looks expired at client
        # because of delay or clock differences, present it as not-expired.
        secs_from_epoc > now ? secs_from_epoc - now : 1
      end
    end

  end
end
