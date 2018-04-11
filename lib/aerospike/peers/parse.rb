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
  class Peers
    # Parse the response from peers command
    module Parse
      # Object representing the parsed response from peers command
      Object = ::Struct.new(:generation, :port_default, :peers)

      class << self

        BASE_REGEX = /(\d+),(\d*),\[(.*)\]/.freeze

        def call(response)
          gen, port, peers = parse_base(response)

          ::Aerospike::Peers::Parse::Object.new.tap do |obj|
            obj.generation = gen.to_i
            obj.port_default = port.empty? ? nil : port.to_i
            obj.peers = parse_peers(peers, obj)
          end
        end

        def parse_base(response)
         BASE_REGEX.match(response).to_a.last(3).tap do |parsed|
          # Expect three pieces parsed from the Regex
          raise ::Aerospike::Exceptions::Parse if parsed.size != 3
         end
        end

        def parse_peers(response, obj)
          return [] if response.empty?
          parser = ::Aerospike::Utils::StringParser.new(response)
          [].tap do |result|
            loop do
              result << parse_peer(parser, obj)
              break unless parser.current == ','
              parser.step
            end
          end
        end

        def parse_peer(parser, obj)
          ::Aerospike::Peer.new.tap do |peer|
            parser.expect('[')
            peer.node_name = parser.read_until(',')
            peer.tls_name = parser.read_until(',')
            peer.hosts = parse_hosts(parser, peer)
            # Assign default port if missing
            peer.hosts.each do |host|
              host.port ||= obj.port_default
            end
            parser.expect(']')
          end
        end

        def parse_hosts(parser, peer)
          result = []
          parser.expect('[')
          return result if parser.current == ']'

          loop do
            result << parse_host(parser, peer)
            break if parser.current == ']'
          end

          result
        end

        def parse_host(parser, peer)
          if parser.current == '[' # IPv6
            parser.step
            host = parser.read_until(']')
            # skip one extra if port is detected, to match read behavior below
            parser.step if parser.current == ':'
          else
            host = parser.read_until(',', ':', ']')
          end

          port = parser.prev == ':' ? parser.read_until(',', ']').to_i : nil

          if parser.prev == ',' || parser.prev == ']'
            return ::Aerospike::Host.new(host, port, peer.tls_name)
          end

          raise ::Aerospike::Exceptions::Parse, "Unterminated host in response"
        end
      end
    end
  end
end
