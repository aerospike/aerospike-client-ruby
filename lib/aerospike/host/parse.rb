# frozen_string_literal: true

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

module Aerospike
  class Host
    module Parse
      INTEGER_REGEX = /\A\d+\z/.freeze

      class << self
        ##
        #  Parse hosts from string format: hostname1[:tlsname1][:port1],...
        #
        #  Hostname may also be an IP address in the following formats:
        #  - xxx.xxx.xxx.xxx
        #  - [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]
        #  - [xxxx::xxxx]
        #
        def call(hosts, default_port = 3000)
          case hosts
          when Host
            [hosts]
          when Array
            hosts
          when String
            hosts.split(?,).map { |host|
              addr, tls_name, port = components(host)
              if port.nil? && tls_name && tls_name.match(INTEGER_REGEX)
                port = tls_name
                tls_name = nil
              end
              port ||= default_port
              Host.new(addr, port.to_i, tls_name)
            }
          else
            fail TypeError, "hosts should be a Host object, an Array of Host objects, or a String"
          end
        end

        # Extract addr, tls_name and port components from a host strin
        def components(host_string)
          host_string = host_string.strip

          # IPv6
          if host_string.start_with?('[')
            end_idx = host_string.index(']')
            raise ::Aerospike::Exceptions::Parse, 'Invalid IPv6 host' if end_idx.nil?

            # Slice away brackets and what's inside them, then split on : and
            # replace first entry with string inside brackets
            host_string.slice(end_idx+1..-1).split(':').tap do |result|
              result[0] = host_string[1...end_idx]
            end
          else
            host_string.split(?:)
          end
        end
      end
    end
  end
end
