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

require 'atomic'

module Apik

  class Node

    attr_reader :referenceCount, :responded, :name

    PARTITIONS = 4096
    FULL_HEALTH = 100

    # Initialize server node with connection parameters.
    def initialize(cluster, nv)
      @cluster = cluster
      @name = nv.name
      @aliases = Atomic.new(nv.aliases)
      @host = nv.host
      @useNewInfo = Atomic.new(nv.useNewInfo)

      # Assign host to first IP alias because the server identifies nodes
      # by IP address (not hostname).
      @host =                nv.aliases[0]
      @connections =         Queue.new
      @health =              Atomic.new(FULL_HEALTH)
      @partitionGeneration = Atomic.new(-1)
      @referenceCount =      Atomic.new(0)
      @responded =           Atomic.new(false)
      @active =              Atomic.new(true)

    end

    # Request current status from server node, and update node with the result
    def refresh
      friends = []
      conn = get_connection(1)

      begin
        infoMap = Info.request(conn, "node", "partition-generation", "services")
      rescue Exception => e
        conn.close
        decrease_health

        raise e
      end

      verify_node_name(infoMap)
      restore_health

      @responded.update{|v| true}

      friends = add_friends(infoMap)
      update_partitions(conn, infoMap)
      put_connection(conn)
      friends
    end

    # Get a connection to the node. If no cached connection is not available,
    # a new connection will be created
    def get_connection(timeout)
      while true
        begin
          conn = @connections.pop(true) # non-blocking pop
          break unless conn
          if conn.connected?
            conn.set_timeout(timeout.to_f)
            return conn
          end
          conn.close
        rescue ThreadError => e
          break
        end
      end

      conn = Connection.new(@host.name, @host.port, @cluster.connection_timeout)
      conn.set_timeout(timeout)
      conn
    end

    # Put back a connection to the cache. If cache is full, the connection will be
    # closed and discarded
    def put_connection(conn)
      if !@active.value || (@connections.length >= @cluster.connection_queue_size)
        conn.close
      end

      @connections << conn
    end

    # Mark the node as healthy
    def restore_health
      # There can be cases where health is full, but active is false.
      # Once a node has been marked inactive, it stays inactive.
      @health.value = FULL_HEALTH
    end

    # Decrease node Health as a result of bad connection or communication
    def decrease_health
      @health.update {|v| v -= 1 }
    end

    # Check if the node is unhealthy
    def unhealthy?
      @health.value <= 0
    end

    # Retrieves host for the node
    def get_host
      @host
    end

    # Checks if the node is active
    def active?
      @active.value
    end

    # Returns node name
    def get_name
      @name
    end

    # Returns node aliases
    def get_aliases
      @aliases.value.dup
    end

    # Adds an alias for the node
    def add_alias(aliasToAdd)
      # Aliases are only referenced in the cluster tend threads,
      # so synchronization is not necessary.
      aliases = get_aliases
      if aliases
        aliases = []
      end

      aliases << aliasToAdd
      set_aliases(aliases)
    end

    # Marks node as inactice and closes all cached connections
    def close
      @active.value = false
      close_connections
    end

    # Implements stringer interface
    def to_s
      "#{@name} : #{@host}"
    end

    def ==(other)
      other && other.is_a?(Node) && (@name == other.name)
    end

    def use_new_info?
      @useNewInfo.value
    end

    private

    def close_connections
      while conn = @connections.pop(true) # non-blocking
        conn.close
      end
    end


    # Sets node aliases
    def set_aliases(aliases)
      @aliases.value = aliases
    end

    def verify_node_name(infoMap)
      info_name = infoMap['node']

      if !info_name
        decrease_health
        raise Apik::Exceptions.Aerospike.new("Node name is empty")
      end

      if !(@name == info_name)
        # Set node to inactive immediately.
        @active.update{|v| false}
        raise Apik::Exceptions.Aerospike.new("Node name has changed. Old=#{@name} New= #{info_name}")
      end
    end

    def add_friends(infoMap)
      friendString = infoMap['services']
      friends = []

      if !friendString
        return friends
      end

      friendNames = friendString.split(';')

      friendNames.each do |friend|
        friendInfo = friend.split(':')
        host = friendInfo[0]
        port = friendInfo[1].to_i
        aliass = Host.new(host, port)
        node = @cluster.find_alias(aliass)

        if node
          node.referenceCount.update{|v| v + 1}
        else
          if !find_alias(friends, aliass)
            if !friends
              friends = []
            end

            friends << aliass
          end
        end
      end

      friends
    end

    def find_alias(friends, aliass)
      friends.any? {|host| host == aliass}
    end

    def update_partitions(conn, infoMap)
      genString = infoMap['partition-generation']

      raise Apik::Exceptions.Aerospike.new("partition-generation is empty") if !genString

      generation = genString.to_i

      if @partitionGeneration.value != generation
        Apik.logger.info("Node #{get_name} partition generation #{generation} changed")
        @cluster.update_partitions(conn, self)
        @partitionGeneration.update{|v| generation}
      end
    end

  end # class Node

end # module
