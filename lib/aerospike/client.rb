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

require "digest"
require "base64"

module Aerospike

  ##
  # Client class manages the Aerospike cluster nodes under the hood, and
  # provides methods to access the database.
  #
  # All internal code is thread-safe, and can be used from multiple threads
  # without any need for synchronization.

  # Examples:
  #
  # # connect to the database
  # client = Client.new('192.168.0.1')
  #
  # #=> raises Aerospike::Exceptions::Timeout if a +:timeout+ is specified and
  # +:fail_if_not_connected+ set to true

  class Client
    attr_accessor :default_admin_policy, :default_batch_policy, :default_info_policy, :default_query_policy, :default_read_policy, :default_scan_policy, :default_write_policy, :default_operate_policy, :cluster

    def initialize(hosts = nil, policy: ClientPolicy.new, connect: true)
      hosts = ::Aerospike::Host::Parse.(hosts || ENV["AEROSPIKE_HOSTS"] || "localhost")
      policy = create_policy(policy, ClientPolicy)
      set_default_policies(policy.policies)
      @cluster = Cluster.new(policy, hosts)
      @cluster.add_cluster_config_change_listener(self)

      self.connect if connect
      self
    end

    ##
    #  Connect to the cluster.

    def connect
      @cluster.connect
    end

    ##
    #  Closes all client connections to database server nodes.

    def close
      @cluster.close
    end

    ##
    #  Determines if there are active connections to the database server cluster.
    #  Returns +true+ if connections exist.

    def connected?
      @cluster.connected?
    end

    ##
    #  Returns array of active server nodes in the cluster.

    def nodes
      @cluster.nodes
    end

    ##
    #  Returns list of active server node names in the cluster.

    def node_names
      @cluster.nodes.map(&:name)
    end

    def supports_feature?(feature)
      @cluster.supports_feature?(feature)
    end

    #-------------------------------------------------------
    # Write Record Operations
    #-------------------------------------------------------

    ##
    #  Writes record bin(s).
    #  The policy options specifiy the transaction timeout, record expiration
    #  and how the transaction is handled when the record already exists.
    #
    #  If no policy options are provided, +@default_write_policy+ will be used.

    #  Examples:
    #
    #  client.put key, {'bin', 'value string'}, :timeout => 0.001

    def put(key, bins, options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::WRITE)
      execute_command(command)
    end

    #-------------------------------------------------------
    # Operations string
    #-------------------------------------------------------

    ##
    #  Appends bin values string to existing record bin values.
    #  The policy specifies the transaction timeout, record expiration and
    #  how the transaction is handled when the record already exists.
    #
    #  This call only works for string values.
    #
    #  If no policy options are provided, +@default_write_policy+ will be used.

    #  Examples:
    #
    #  client.append key, {'bin', 'value to append'}, :timeout => 0.001

    def append(key, bins, options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::APPEND)
      execute_command(command)
    end

    ##
    #  Prepends bin values string to existing record bin values.
    #  The policy specifies the transaction timeout, record expiration and
    #  how the transaction is handled when the record already exists.
    #
    #  This call works only for string values.
    #
    #  If no policy options are provided, +@default_write_policy+ will be used.

    #  Examples:
    #
    #  client.prepend key, {'bin', 'value to prepend'}, :timeout => 0.001

    def prepend(key, bins, options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::PREPEND)
      execute_command(command)
    end

    #-------------------------------------------------------
    # Arithmetic Operations
    #-------------------------------------------------------

    ##
    #  Adds integer bin values to existing record bin values.
    #  The policy specifies the transaction timeout, record expiration and
    #  how the transaction is handled when the record already exists.
    #
    #  This call only works for integer values.
    #
    #  If no policy options are provided, +@default_write_policy+ will be used.

    #  Examples:
    #
    #  client.add key, {'bin', -1}, :timeout => 0.001

    def add(key, bins, options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::ADD)
      execute_command(command)
    end

    #-------------------------------------------------------
    # Delete Operations
    #-------------------------------------------------------

    ##
    #  Deletes record for specified key.
    #
    #  The policy specifies the transaction timeout.
    #
    #  If no policy options are provided, +@default_write_policy+ will be used.
    #  Returns +true+ if a record with corresponding +key+ existed.

    #  Examples:
    #
    #  existed = client.delete key, :timeout => 0.001

    def delete(key, options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)
      command = DeleteCommand.new(@cluster, policy, key)
      execute_command(command)
      command.existed
    end

    ##
    # Removes records in the specified namespace/set efficiently.
    #
    # This method is orders of magnitude faster than deleting records one at a
    # time. It requires Aerospike Server version 3.12 or later. See
    # https://www.aerospike.com/docs/reference/info#truncate for further
    # information.
    #
    # This asynchronous server call may return before the truncate is complete.
    # The user can still write new records after the server call returns
    # because new records will have last update times greater than the truncate
    # cut-off (set at the time of the truncate call.)
    #
    # If no policy options are provided, +@default_info_policy+ will be used.

    def truncate(namespace, set_name = nil, before_last_update = nil, options = {})
      policy = create_policy(options, Policy, default_info_policy)

      node = @cluster.random_node

      if set_name && !set_name.to_s.strip.empty?
        str_cmd = "truncate:namespace=#{namespace}"
        str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      else
        str_cmd = if node.supports_feature?(Aerospike::Features::TRUNCATE_NAMESPACE)
          "truncate-namespace:namespace=#{namespace}"
                  else
          "truncate:namespace=#{namespace}"
                  end
      end

      if before_last_update
        lut_nanos = (before_last_update.to_f * 1_000_000_000.0).round
        str_cmd << ";lut=#{lut_nanos}"
      elsif supports_feature?(Aerospike::Features::LUT_NOW)
        # Servers >= 4.3.1.4 require lut argument
        str_cmd << ";lut=now"
      end

      response = send_info_command(policy, str_cmd, node).upcase
      return if response == "OK"
      raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_ERROR, "Truncate failed: #{response}")
    end

    #-------------------------------------------------------
    # Touch Operations
    #-------------------------------------------------------

    ##
    #  Creates record if it does not already exist.  If the record exists,
    #  the record's time to expiration will be reset to the policy's expiration.
    #
    #  If no policy options are provided, +@default_write_policy+ will be used.

    #  Examples:
    #
    #  client.touch key, :timeout => 0.001

    def touch(key, options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)
      command = TouchCommand.new(@cluster, policy, key)
      execute_command(command)
    end

    #-------------------------------------------------------
    # Existence-Check Operations
    #-------------------------------------------------------

    ##
    #  Determines if a record key exists.
    #  The policy can be used to specify timeouts.
    def exists(key, options = nil)
      policy = create_policy(options, Policy, default_read_policy)
      command = ExistsCommand.new(@cluster, policy, key)
      execute_command(command)
      command.exists
    end

    #-------------------------------------------------------
    # Read Record Operations
    #-------------------------------------------------------

    #  Read record header and bins for specified key.
    #  The policy can be used to specify timeouts.
    def get(key, bin_names = nil, options = nil)
      policy = create_policy(options, Policy, default_read_policy)

      command = ReadCommand.new(@cluster, policy, key, bin_names)
      execute_command(command)
      command.record
    end

    #  Read record generation and expiration only for specified key.  Bins are not read.
    #  The policy can be used to specify timeouts.
    def get_header(key, options = nil)
      policy = create_policy(options, Policy, default_read_policy)
      command = ReadHeaderCommand.new(@cluster, policy, key)
      execute_command(command)
      command.record
    end

    #-------------------------------------------------------
    # Batch Read Operations
    #-------------------------------------------------------

    #  Read multiple record headers and bins for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts and protocol type.
    def batch_get(keys, bin_names = nil, options = nil)
      policy = create_policy(options, BatchPolicy, default_batch_policy)
      results = Array.new(keys.length)
      info_flags = INFO1_READ

      case bin_names
      when :all, nil, []
        info_flags |= INFO1_GET_ALL
        bin_names = nil
      when :none
        info_flags |= INFO1_NOBINDATA
        bin_names = nil
      end

      execute_batch_index_commands(policy, keys) do |node, batch|
        BatchIndexCommand.new(node, batch, policy, bin_names, results, info_flags)
      end

      results
    end

    #  Read multiple record header data for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts and protocol type.
    def batch_get_header(keys, options = nil)
      batch_get(keys, :none, options)
    end

    #  Check if multiple record keys exist in one batch call.
    #  The returned boolean array is in positional order with the original key array order.
    #  The policy can be used to specify timeouts and protocol type.
    def batch_exists(keys, options = nil)
      policy = create_policy(options, BatchPolicy, default_batch_policy)
      results = Array.new(keys.length)

      execute_batch_index_commands(policy, keys) do |node, batch|
        BatchIndexExistsCommand.new(node, batch, policy, results)
      end

      results
    end

    #-------------------------------------------------------
    # Generic Database Operations
    #-------------------------------------------------------

    #  Perform multiple read/write operations on a single key in one batch call.
    #  An example would be to add an integer value to an existing record and then
    #  read the result, all in one database call. Operations are executed in
    #  the order they are specified.
    def operate(key, operations, options = nil)
      policy = create_policy(options, OperatePolicy, default_operate_policy)

      args = OperateArgs.new(cluster, policy, default_write_policy, default_operate_policy, key, operations)
      command = OperateCommand.new(@cluster, key, args)
      execute_command(command)
      command.record
    end

    #---------------------------------------------------------------
    # User defined functions (Supported by Aerospike 3 servers only)
    #---------------------------------------------------------------

    #  Register package containing user defined functions with server.
    #  This asynchronous server call will return before command is complete.
    #  The user can optionally wait for command completion by using the returned
    #  RegisterTask instance.
    #
    #  This method is only supported by Aerospike 3 servers.
    def register_udf_from_file(client_path, server_path, language, options = nil)
      udf_body = File.read(client_path)
      register_udf(udf_body, server_path, language, options)
    end

    #  Register package containing user defined functions with server.
    #  This asynchronous server call will return before command is complete.
    #  The user can optionally wait for command completion by using the returned
    #  RegisterTask instance.
    #
    #  This method is only supported by Aerospike 3 servers.
    def register_udf(udf_body, server_path, language, options = nil)
      policy = create_policy(options, Policy, default_info_policy)

      content = Base64.strict_encode64(udf_body).force_encoding("binary")
      str_cmd = "udf-put:filename=#{server_path};content=#{content};"
      str_cmd << "content-len=#{content.length};udf-type=#{language};"

      # Send UDF to one node. That node will distribute the UDF to other nodes.
      response_map = @cluster.request_info(policy, str_cmd)

      res = {}
      response_map.each do |k, response|
        vals = response.to_s.split(";")
        vals.each do |pair|
          k, v = pair.split("=", 2)
          res[k] = v
        end
      end

      if res["error"]
        raise Aerospike::Exceptions::CommandRejected.new("Registration failed: #{res['error']}\nFile: #{res['file']}\nLine: #{res['line']}\nMessage: #{res['message']}")
      end

      UdfRegisterTask.new(@cluster, server_path)
    end

    #  RemoveUDF removes a package containing user defined functions in the server.
    #  This asynchronous server call will return before command is complete.
    #  The user can optionally wait for command completion by using the returned
    #  RemoveTask instance.
    #
    #  This method is only supported by Aerospike 3 servers.
    def remove_udf(udf_name, options = nil)
      policy = create_policy(options, Policy, default_info_policy)

      str_cmd = "udf-remove:filename=#{udf_name};"

      # Send command to one node. That node will distribute it to other nodes.
      # Send UDF to one node. That node will distribute the UDF to other nodes.
      response_map = @cluster.request_info(policy, str_cmd)
      _, response = response_map.first

      if response == "ok"
        UdfRemoveTask.new(@cluster, udf_name)
      else
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_ERROR, response)
      end
    end

    #  ListUDF lists all packages containing user defined functions in the server.
    #  This method is only supported by Aerospike 3 servers.
    def list_udf(options = nil)
      policy = create_policy(options, Policy, default_info_policy)

      str_cmd = "udf-list"

      # Send command to one node. That node will distribute it to other nodes.
      response_map = @cluster.request_info(policy, str_cmd)
      _, response = response_map.first

      vals = response.split(";")

      vals.map do |udf_info|
        next if udf_info.strip! == ""

        udf_parts = udf_info.split(",")
        udf = UDF.new
        udf_parts.each do |values|
          k, v = values.split("=", 2)
          case k
          when "filename"
            udf.filename = v
          when "hash"
            udf.hash = v
          when "type"
            udf.language = v
          end
        end
        udf
      end
    end

    #  Execute user defined function on server and return results.
    #  The function operates on a single record.
    #  The package name is used to locate the udf file location:
    #
    #  udf file = <server udf dir>/<package name>.lua
    #
    #  This method is only supported by Aerospike 3 servers.
    def execute_udf(key, package_name, function_name, args = [], options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)

      command = ExecuteCommand.new(@cluster, policy, key, package_name, function_name, args)
      execute_command(command)

      record = command.record

      return nil if !record || record.bins.empty?

      result_map = record.bins

      # User defined functions don't have to return a value.
      key, obj = result_map.detect { |k, _| k.include?("SUCCESS") }
      return obj if key

      key, obj = result_map.detect { |k, _| k.include?("FAILURE") }
      message = key ? obj.to_s : "Invalid UDF return value"
      raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::UDF_BAD_RESPONSE, message)
    end

    # execute_udf_on_query applies user defined function on records that match the statement filter.
    # Records are not returned to the client.
    # This asynchronous server call will return before command is complete.
    # The user can optionally wait for command completion by using the returned
    # ExecuteTask instance.
    #
    # This method is only supported by Aerospike 3 servers.
    # If the policy is nil, the default relevant policy will be used.
    def execute_udf_on_query(statement, package_name, function_name, function_args = [], options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)

      nodes = @cluster.nodes
      if nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Executing UDF failed because cluster is empty.")
      end

      statement = statement.clone
      statement.set_aggregate_function(package_name, function_name, function_args, false)
      # Use a thread per node
      nodes.each do |node|
        Thread.new do
          Thread.current.abort_on_exception = true
          begin
            command = ServerCommand.new(@cluster, node, policy, statement, true, statement.task_id)
            execute_command(command)
          rescue => e
            Aerospike.logger.error(e)
            raise e
          end
        end
      end

      ExecuteTask.new(@cluster, statement)
    end

    #  Create secondary index.
    #  This asynchronous server call will return before command is complete.
    #  The user can optionally wait for command completion by using the returned
    #  IndexTask instance.
    #
    #  This method is only supported by Aerospike 3 servers.
    #  index_type should be :string, :numeric or :geo2dsphere (requires server version 3.7 or later)
    #  collection_type should be :list, :mapkeys or :mapvalues
    #  ctx is an optional list of context. Supported on server v6.1+.
    def create_index(namespace, set_name, index_name, bin_name, index_type, collection_type = nil, options = nil, ctx: nil)
      if options.nil? && collection_type.is_a?(Hash)
        options = collection_type
        collection_type = nil
      end
      policy = create_policy(options, Policy, default_info_policy)

      str_cmd = "sindex-create:ns=#{namespace}"
      str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      str_cmd << ";indexname=#{index_name};numbins=1"
      str_cmd << ";context=#{CDT::Context.base64(ctx)}" unless ctx.to_a.empty?
      str_cmd << ";indextype=#{collection_type.to_s.upcase}" if collection_type
      str_cmd << ";indexdata=#{bin_name},#{index_type.to_s.upcase}"
      str_cmd << ";priority=normal"

      # Send index command to one node. That node will distribute the command to other nodes.
      response = send_info_command(policy, str_cmd).upcase
      if response == "OK"
        # Return task that could optionally be polled for completion.
        return IndexTask.new(@cluster, namespace, index_name)
      end

      if response.start_with?("FAIL:200")
        # Index has already been created.  Do not need to poll for completion.
        return IndexTask.new(@cluster, namespace, index_name, true)
      end

      raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INDEX_GENERIC, "Create index failed: #{response}")
    end

    #  Delete secondary index.
    #  This method is only supported by Aerospike 3 servers.
    def drop_index(namespace, set_name, index_name, options = nil)
      policy = create_policy(options, Policy, default_info_policy)

      str_cmd = "sindex-delete:ns=#{namespace}"
      str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      str_cmd << ";indexname=#{index_name}"

      # Send index command to one node. That node will distribute the command to other nodes.
      response = send_info_command(policy, str_cmd).upcase
      return if response == "OK"

      # Index did not previously exist. Return without error.
      return if response.start_with?("FAIL:201")

      raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INDEX_GENERIC, "Drop index failed: #{response}")
    end

    def request_info(*commands, policy: nil)
      policy = create_policy(policy, Policy, default_info_policy)
      @cluster.request_info(policy, *commands)
    end

    #-------------------------------------------------------
    # Scan Operations
    #-------------------------------------------------------

    # Reads records in specified namespace and set using partition filter.
    # If the policy's concurrent_nodes is specified, each server node will be read in
    # parallel. Otherwise, server nodes are read sequentially.
    # If partition_filter is nil, all partitions will be scanned.
    # If the policy is nil, the default relevant policy will be used.
    # This method is only supported by Aerospike 4.9+ servers.
    def scan_partitions(partition_filter, namespace, set_name, bin_names = nil, options = nil)
      policy = create_policy(options, ScanPolicy, default_scan_policy)

      # Retry policy must be one-shot for scans.
      # copy on write for policy
      new_policy = policy.clone

      nodes = @cluster.nodes
      if nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
      end

      tracker = Aerospike::PartitionTracker.new(policy, nodes, partition_filter)
      recordset = Recordset.new(policy.record_queue_size, 1, :scan)
      Thread.new do
        Thread.current.abort_on_exception = true
        ScanExecutor.scan_partitions(policy, @cluster, tracker, namespace, set_name, recordset, bin_names)
      end

      recordset
    end

    # Reads all records in specified namespace and set for one node only.
    # If the policy is nil, the default relevant policy will be used.
    def scan_node_partitions(node, namespace, set_name, bin_names = nil, options = nil)
      policy = create_policy(options, ScanPolicy, default_scan_policy)

      # Retry policy must be one-shot for scans.
      # copy on write for policy
      new_policy = policy.clone

      unless node.active?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
      end

      tracker = Aerospike::PartitionTracker.new(policy, [node])
      recordset = Recordset.new(policy.record_queue_size, 1, :scan)
      Thread.new do
        Thread.current.abort_on_exception = true
        ScanExecutor.scan_partitions(policy, @cluster, tracker, namespace, set_name, recordset, bin_names)
      end

      recordset
    end

    # Reads all records in specified namespace and set from all nodes.
    # If the policy's concurrent_nodes is specified, each server node will be read in
    # parallel. Otherwise, server nodes are read sequentially.
    # If the policy is nil, the default relevant policy will be used.
    def scan_all(namespace, set_name, bin_names = nil, options = nil)
      scan_partitions(Aerospike::PartitionFilter.all, namespace, set_name, bin_names, options)
    end

    # ScanNode reads all records in specified namespace and set, from one node only.
    # The policy can be used to specify timeouts.
    def scan_node(node, namespace, set_name, bin_names = nil, options = nil)
      scan_node_partitions(node, namespace, set_name, bin_names, options)
    end

    #--------------------------------------------------------
    # Query functions (Supported by Aerospike 3 servers only)
    #--------------------------------------------------------

    # Executes a query for specified partitions and returns a recordset.
    # The query executor puts records on the queue from separate threads.
    # The caller can concurrently pop records off the queue through the
    # recordset.records API.
    #
    # This method is only supported by Aerospike 4.9+ servers.
    # If the policy is nil, the default relevant policy will be used.
    def query_partitions(partition_filter, statement, options = nil)
      policy = create_policy(options, QueryPolicy, default_query_policy)

      nodes = @cluster.nodes
      if nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.")
      end

      # result recordset
      recordset = Recordset.new(policy.record_queue_size, 1, :query)
      tracker = PartitionTracker.new(policy, nodes, partition_filter)
      Thread.new do
        Thread.current.abort_on_exception = true
        QueryExecutor.query_partitions(@cluster, policy, tracker, statement, recordset)
      end

      recordset
    end

    # Query executes a query and returns a recordset.
    # The query executor puts records on a channel from separate threads.
    # The caller can concurrently pops records off the channel through the
    # record channel.
    #
    # This method is only supported by Aerospike 3 servers.
    # If the policy is nil, a default policy will be generated.
    def query(statement, options = nil)
      query_partitions(Aerospike::PartitionFilter.all, statement, options)
    end

    #----------------------------------------------------------
    # Query/Execute (Supported by Aerospike 3+ servers only)
    #----------------------------------------------------------

    # QueryExecute applies operations on records that match the statement filter.
    # Records are not returned to the client.
    # This asynchronous server call will return before the command is complete.
    # The user can optionally wait for command completion by using the returned
    # ExecuteTask instance.
    #
    # This method is only supported by Aerospike 3+ servers.
    # If the policy is nil, the default relevant policy will be used.
    #
    # @param statement [Aerospike::Statement] The query or batch read statement.
    # @param operations [Array<Aerospike::Operation>] An optional list of operations.
    # @param options [Hash] An optional hash of policy options.
    # @return [Aerospike::ExecuteTask] An ExecuteTask instance that can be used to wait for command completion.
    #
    # @raise [Aerospike::Exceptions::Aerospike] if an error occurs during the operation.
    def query_execute(statement, operations = [], options = nil)
      policy = create_policy(options, WritePolicy, default_write_policy)

      if statement.nil?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INVALID_COMMAND, "Query failed of invalid statement.")
      end

      statement = statement.clone
      unless operations.empty?
        statement.operations = operations
      end

      task_id = statement.task_id
      nodes = @cluster.nodes
      if nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.")
      end

      # Use a thread per node
      nodes.each do |node|
        Thread.new do
          Thread.current.abort_on_exception = true
          begin
            command = ServerCommand.new(@cluster, node, policy, statement, true, task_id)
            execute_command(command)
          rescue => e
            Aerospike.logger.error(e)
            raise e
          end
        end
      end
      ExecuteTask.new(@cluster, statement)
    end

    #-------------------------------------------------------
    # User administration
    #-------------------------------------------------------

    # Create user with password and roles. Clear-text password will be hashed using bcrypt
    # before sending to server.
    def create_user(user, password, roles, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      hash = LoginCommand.hash_password(password)
      command = AdminCommand.new
      command.create_user(@cluster, policy, user, hash, roles)
    end

    # Remove user from cluster.
    def drop_user(user, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.drop_user(@cluster, policy, user)
    end

    # Change user's password. Clear-text password will be hashed using bcrypt before sending to server.
    def change_password(user, password, options = nil)
      raise Aerospike::Exceptions::Aerospike.new(INVALID_USER) unless @cluster.user && @cluster.user != ""
      policy = create_policy(options, AdminPolicy, default_admin_policy)

      hash = LoginCommand.hash_password(password)
      command = AdminCommand.new

      if user == @cluster.user
        # Change own password.
        command.change_password(@cluster, policy, user, hash)
      else
        # Change other user's password by user admin.
        command.set_password(@cluster, policy, user, hash)
      end

      @cluster.change_password(user, hash)
    end

    # Add roles to user's list of roles.
    def grant_roles(user, roles, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.grant_roles(@cluster, policy, user, roles)
    end

    # Remove roles from user's list of roles.
    def revoke_roles(user, roles, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.revoke_roles(@cluster, policy, user, roles)
    end

    # Retrieve roles for a given user.
    def query_user(user, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.query_user(@cluster, policy, user)
    end

    # Retrieve all users and their roles.
    def query_users(options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.query_users(@cluster, policy)
    end

    # Retrieve privileges for a given role.
    def query_role(role, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.query_role(@cluster, policy, role)
    end

    # Retrieve all roles and their privileges.
    def query_roles(options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.query_roles(@cluster, policy)
    end

    # Create a user-defined role.
    # Quotas require server security configuration "enable-quotas" to be set to true.
    # Pass 0 for quota values for no limit.
    def create_role(role_name, privileges = [], allowlist = [], read_quota = 0, write_quota = 0, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.create_role(@cluster, policy, role_name, privileges, allowlist, read_quota, write_quota)
    end

    # Remove a user-defined role.
    def drop_role(role_name, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.drop_role(@cluster, policy, role_name)
    end

    # Grant privileges to a user-defined role.
    def grant_privileges(role_name, privileges, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.grant_privileges(@cluster, policy, role_name, privileges)
    end

    # Revoke privileges from a user-defined role.
    def revoke_privileges(role_name, privileges, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.revoke_privileges(@cluster, policy, role_name, privileges)
    end

    # Set or update quota for a role.
    def set_quotas(role_name, read_quota, write_quota, options = nil)
      policy = create_policy(options, AdminPolicy, default_admin_policy)
      command = AdminCommand.new
      command.set_quotas(@cluster, policy, role_name, read_quota, write_quota)
    end

    private

    def set_default_policies(policies)
      self.default_info_policy = create_policy(policies[:info], Policy)
      self.default_read_policy = create_policy(policies[:read], Policy)
      self.default_admin_policy = create_policy(policies[:admin], AdminPolicy)
      self.default_batch_policy = create_policy(policies[:batch], BatchPolicy)
      self.default_query_policy = create_policy(policies[:query], QueryPolicy)
      self.default_scan_policy = create_policy(policies[:scan], ScanPolicy)
      self.default_write_policy = create_policy(policies[:write], WritePolicy)
      self.default_operate_policy = create_policy(policies[:operate], OperatePolicy)
    end

    def send_info_command(policy, command, node = nil)
      Aerospike.logger.debug { "Sending info command: #{command}" }
      if node
        _, response = @cluster.request_node_info(node, policy, command).first
      else
        _, response = @cluster.request_info(policy, command).first
      end
      response.to_s
    end

    def hash_to_bins(hash)
      if hash.is_a?(Bin)
        [hash]
      elsif hash.is_a?(Array)
        hash # it is a list of bins
      else
        hash.map do |k, v|
          raise Aerospike::Exceptions::Parse.new("bin name `#{k}` is not a string.") unless k.is_a?(String)
          Bin.new(k, v)
        end
      end
    end

    def create_policy(policy, policy_klass, default_policy = nil)
      case policy
      when nil
        default_policy || policy_klass.new
      when policy_klass
        policy
      when Hash
        policy_klass.new(policy)
      else
        raise TypeError, "policy should be a #{policy_klass.name} instance or a Hash"
      end
    end

    attr_writer :cluster

    def cluster_config_changed(cluster)
      Aerospike.logger.debug { "Cluster config change detected; active nodes: #{cluster.nodes.map(&:name)}" }
      setup_command_validators
    end

    def setup_command_validators
      Aerospike.logger.debug { "Cluster features: #{@cluster.features.get.to_a}" }
      validators = []

      # guard against unsupported particle types
      unsupported_particle_types = []
      unsupported_particle_types << Aerospike::ParticleType::DOUBLE unless supports_feature?(Aerospike::Features::FLOAT)
      unsupported_particle_types << Aerospike::ParticleType::GEOJSON unless supports_feature?(Aerospike::Features::GEO)
      validators << UnsupportedParticleTypeValidator.new(*unsupported_particle_types) unless unsupported_particle_types.empty?

      @command_validators = validators
    end

    def validate_command(command)
      return unless @command_validators
      @command_validators.each do |validator|
        validator.call(command)
      end
    end

    def execute_command(command)
      validate_command(command)
      command.execute
    end

    def execute_batch_index_commands(policy, keys)
      if @cluster.nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Executing Batch Index command failed because cluster is empty.")
      end

      batch_nodes = BatchIndexNode.generate_list(@cluster, policy.replica, keys)
      threads = []

      batch_nodes.each do |batch|
        threads << Thread.new do
          command = yield batch.node, batch
          execute_command(command)
        end
      end

      threads.each(&:join)
    end
  end # class
end # module
