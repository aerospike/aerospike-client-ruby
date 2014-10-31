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

require 'time'

module Aerospike

  CITRUSLEAF_EPOCH = 1262304000

  # Converts an Expiration time to TTL in seconds
  def self.TTL(secs_from_citrus_leaf_epoc)
    if secs_from_citrus_leaf_epoc == 0
      0xFFFFFFFF
    else
      now = Time.now.to_i - CITRUSLEAF_EPOCH
      # Record was not expired at server but if it looks expired at client
      # because of delay or clock differences, present it as not-expired.
      secs_from_citrus_leaf_epoc > now ? secs_from_citrus_leaf_epoc - now : 1
    end
  end

end # module
