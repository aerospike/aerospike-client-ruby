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

module Aerospike

  # Policy attributes used in batch read commands.
  class BatchReadPolicy

    attr_accessor :filter_exp

    def initialize(opt={})
      # Optional expression filter. If filter_exp exists and evaluates to false, the specific batch key
      # request is not performed and {BatchRecord#result_code} is set to
      # {ResultCode#FILTERED_OUT}.
      #
      # If exists, this filter overrides the batch parent filter {Policy#filter_exp}
      # for the specific key in batch commands that allow a different policy per key.
      # Otherwise, this filter is ignored.
      #
      # Default: nil
      @filter_exp = opt[:filter_exp]
    end
  end
end