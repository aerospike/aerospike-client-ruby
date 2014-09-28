# encoding: utf-8
# Copyright 2014 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Aerospike

  # Container object for client policy command.
  class ClientPolicy

    attr_accessor :Timeout, :ConnectionQueueSize, :FailIfNotConnected

    def initialize(timeout=nil, connection_queue_size=nil, fail_if_not_connected=nil)
      # Initial host connection timeout in seconds. The timeout when opening a connection
      # to the server host for the first time.
      @Timeout = timeout || 1.0 # 1 second

      # Size of the Connection Queue cache.
      @ConnectionQueueSize = connection_queue_size || 64

      # Throw exception if host connection fails during add_host.
      @FailIfNotConnected = fail_if_not_connected || true
    end

  end # class

end # module
