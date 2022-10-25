# frozen_string_literal: true

# Copyright 2014-2020 Aerospike, Inc.
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

module Aerospike
  class PartitionTracker
    attr_reader :partitions, :partitions_capacity, :partition_begin, :node_capacity,
                :node_filter, :partition_filter, :node_partitions_list, :max_records,
                :sleep_between_retries, :socket_timeout, :total_timeout, :iteration, :deadline

    def initialize(policy, nodes, partition_filter = nil)
      if partition_filter.nil?
        return init_for_node(policy, nodes[0]) if nodes.length == 1
        return init_for_nodes(policy, nodes)
      end

      # Validate here instead of initial PartitionFilter constructor because total number of
      # cluster partitions may change on the server and PartitionFilter will never have access
      # to Cluster instance.  Use fixed number of partitions for now.
      unless partition_filter.partition_begin.between?(0, Node::PARTITIONS - 1)
        raise Aerospike::Exceptions::Aerospike.new(
          Aerospike::ResultCode::PARAMETER_ERROR,
          "Invalid partition begin #{partition_filter.partition_begin}. Valid range: 0-#{Aerospike::Node::PARTITIONS - 1}"
        )
      end

      if partition_filter.count <= 0
        raise Aerospike::Exceptions::Aerospike.new(
          Aerospike::ResultCode::PARAMETER_ERROR,
          "Invalid partition count #{partition_filter.count}"
        )
      end

      if partition_filter.partition_begin + partition_filter.count > Node::PARTITIONS
        raise Aerospike::Exceptions::Aerospike.new(
          Aerospike::ResultCode::PARAMETER_ERROR,
          "Invalid partition range (#{partition_filter.partition_begin}, #{partition_filter.partition_begin + partition_filter.count}"
        )
      end

      @partition_begin = partition_filter.partition_begin
      @node_capacity = nodes.length
      @node_filter = nil
      @partitions_capacity = partition_filter.count
      @max_records = policy.max_records
      @iteration = 1

      if partition_filter.partitions.nil?  then
        partition_filter.partitions = init_partitions(policy, partition_filter.count, partition_filter.digest)
      elsif policy.max_records <= 0
        # Retry all partitions when max_records not specified.
        partition_filter.partitions.each do |ps|
          ps.retry = true
        end
      end

      @partitions = partition_filter.partitions
      @partition_filter = partition_filter
      init_timeout(policy)
    end

    def assign_partitions_to_nodes(cluster, namespace)
      list = []

      pmap = cluster.partitions
      replica_array = pmap[namespace]
      raise Aerospike::Exceptions::InvalidNamespace("namespace not found in the partition map") if !replica_array

      master = (replica_array.get)[0]
      master = master.get

      @partitions.each do |part|
        if part&.retry
          node = master[part.id]

          unless node
            raise Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_NAMESPACE, "Invalid Partition Id #{part.id} for namespace `#{namespace}` in Partition Scan")
          end

        part.retry = false


        # Use node name to check for single node equality because
        # partition map may be in transitional state between
        # the old and new node with the same name.
        next if @node_filter && @node_filter.name != node.name

        np = find_node(list, node)

        unless np
          # If the partition map is in a transitional state, multiple
          # node_partitions instances (each with different partitions)
          # may be created for a single node.
          np = NodePartitions.new(node)
          list << np
        end
        np.add_partition(part)
      end
    end

    if @max_records.positive?
      # Distribute max_records across nodes.
      node_size = list.length

      if @max_records < node_size
        # Only include nodes that have at least 1 record requested.
        node_size = @max_records
        list = list[0...node_size]
      end

      max = 0
      max = @max_records / node_size if node_size.positive?
      rem = @max_records - (max * node_size)

      list[0...node_size].each_with_index do |np, i|
        np.record_max = (i < rem ? max + 1 : max)
      end
    end

      @node_partitions_list = list
      list
    end

    def init_timeout(policy)
      @sleep_between_retries = policy.sleep_between_retries
      @socket_timeout = policy.socket_timeout
      @total_timeout = policy.timeout
      if @total_timeout.positive?
        @deadline = Time.now + @total_timeout
        if !@socket_timeout || @socket_timeout > @total_timeout
          @socket_timeout = @total_timeout
        end
      end
    end

    def init_partitions(policy, partition_count, digest)
      parts_all = Array.new(partition_count)

      (0...partition_count).each do |i|
        parts_all[i] = Aerospike::PartitionStatus.new(@partition_begin + i)
      end

      parts_all[0].digest = digest if digest

      @sleep_between_retries = policy.sleep_between_retries
      @socket_timeout = policy.socket_timeout
      @total_timeout = policy.timeout

      if @total_timeout.positive?
        @deadline = Time.now + @total_timeout

        if @socket_timeout == 0 || @socket_timeout > @total_timeout
          @socket_timeout = @total_timeout
        end
      end

      parts_all
    end

    attr_writer :sleep_between_retries


    def find_node(list, node)
      list.each do |node_partition|
          # Use pointer equality for performance.
          return node_partition if node_partition.node == node
      end
      nil
    end

    def partition_unavailable(node_partitions, partition_id)
      @partitions[partition_id-@partition_begin].retry = true
      node_partitions.parts_unavailable+=1
    end

    def set_digest(node_partitions, key)
      partition_id = key.partition_id
      @partitions[partition_id-@partition_begin].digest = key.digest
      node_partitions.record_count+=1
    end

    def set_last(node_partitions, key, bval)
      partition_id = key.partition_id()
      if partition_id-@partition_begin < 0
        raise "key.partition_id: #{@partition_id}, partition_begin: #{@partition_begin}"
      end
      ps = @partitions[partition_id-@partition_begin]
      ps.digest = key.digest
      ps.bval = bval
      node_partitions.record_count+=1
    end

    def complete?(cluster, policy)
      record_count = 0
      parts_unavailable = 0

      @node_partitions_list.each do |np|
        record_count += np.record_count
        parts_unavailable += np.parts_unavailable
      end

      if parts_unavailable == 0
        if @max_records <= 0
          @partition_filter&.done = true
        else
          if cluster.supports_partition_query.get()
           done = true

           @node_partitions_list.each do |np|
             if np.record_count >= np.record_max
               mark_retry(np)
               done = false
             end
           end

            @partition_filter&.done = done
          else
            # Server version >= 6.0 will return all records for each node up to
            # that node's max. If node's record count reached max, there stilthen
            # may be records available for that node.
            @node_partitions_list.each do |np|
              mark_retry(np) if np.record_count > 0
            end
            # Servers version < 6.0 can return less records than max and still
            # have more records for each node, so the node is only done if nthen
            # records were retrieved for that node.

            @partition_filter&.done = (record_count == 0)
          end
        end
        return true
      end

      return true if @max_records&.positive? && record_count >= @max_records

      # Check if limits have been reached
      if policy.max_retries.positive? && @iteration > policy.max_retries
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::MAX_RETRIES_EXCEEDED, "Max retries exceeded: #{policy.max_retries}")
      end

      if policy.total_timeout > 0
        # Check for total timeout.
        remaining = @deadline - Time.now - @sleep_between_retries

        raise Aerospike::Exceptions::Timeout.new(policy.totle_timeout, @iteration) if remaining <= 0

        if remaining < @total_timeout
          @total_timeout = remaining

          if @socket_timeout > @total_timeout
            @socket_timeout = @total_timeout
          end
        end
      end

      # Prepare for next iteration.
      if @max_records > 0
        @max_records -= record_count
      end
      @iteration+=1
      false
    end

    def should_retry(node_partitions, err)
      case err
      when Aerospike::Exceptions::Aerospike
        case err.result_code
        when Aerospike::ResultCode::TIMEOUT,
          Aerospike::ResultCode::NETWORK_ERROR,
          Aerospike::ResultCode::SERVER_NOT_AVAILABLE,
          Aerospike::ResultCode::INDEX_NOTFOUND
            mark_retry(node_partitions)
            node_partitions.parts_unavailable = node_partitions.parts_full.length + node_partitions.parts_partial.length
            true
        end
      else
        false
      end
    end

    def mark_retry(node_partitions)
      node_partitions.parts_full.each do |ps|
        ps.retry = true
      end

      node_partitions.parts_partial.each do |ps|
        ps.retry = true
      end
    end

    def to_s
      sb = StringIO.new
      @partitions.each_with_index do |ps, i|
        sb << ps.to_s
        sb << if (i+1)%16 == 0
          "\n"
              else
          "\t"
              end
      end
      sb.string
    end

    private

    def init_for_nodes(policy, nodes)
      ppn = Aerospike::Node::PARTITIONS / nodes.length
      ppn += ppn / 4

      @partition_begin = 0
      @node_capacity = nodes.length
      @node_filter = nil
      @partitions_capacity = ppn
      @max_records = policy.max_records
      @iteration = 1

      @partitions = init_partitions(policy, Aerospike::Node::PARTITIONS, nil)
      init_timeout(policy)
    end

    def init_for_node(policy, node)
      @partition_begin = 0
      @node_capacity = 1
      @node_filter = node
      @partitions_capacity = Aerospike::Node::PARTITIONS
      @max_records = policy.max_records
      @iteration = 1

      @partitions = init_partitions(policy, Aerospike::Node::PARTITIONS, nil)
      init_timeout(policy)
    end

  end
end
