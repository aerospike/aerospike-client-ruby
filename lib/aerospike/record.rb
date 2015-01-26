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

  private

  class Record

    attr_reader :key, :bins, :generation, :expiration, :node

    def initialize(node, rec_key, rec_bins, rec_gen, rec_exp)
      @key = rec_key
      @bins = rec_bins
      @generation = rec_gen
      @expiration = rec_exp
      @node = node
    end

    def to_s
      'key: `' + key.to_s + '` ' +
         'bins: `' + bins.to_s + '` ' +
         'generation: `' + generation.to_s + '` ' +
         'expiration: `' + expiration.to_s + '` '
    end

  end
end
