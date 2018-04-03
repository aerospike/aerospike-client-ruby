# encoding: utf-8
# Copyright 2014-2017 Aerospike, Inc.
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

require 'digest'
require 'base64'

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

    attr_accessor :default_policy, :default_write_policy,
      :default_scan_policy, :default_query_policy, :default_admin_policy

    def initialize(hosts = nil, policy: ClientPolicy.new, connect: true)
      @default_policy = Policy.new
      @default_write_policy = WritePolicy.new
      @default_scan_policy = ScanPolicy.new
      @default_query_policy = QueryPolicy.new
      @default_admin_policy = QueryPolicy.new

      hosts = parse_hosts(hosts || ENV["AEROSPIKE_HOSTS"] || "localhost")
      policy = create_policy(policy, ClientPolicy)
      @cluster = Cluster.new(policy, *hosts)
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
      @cluster.nodes.map(&:get_name)
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

    def put(key, bins, options={})
      policy = create_policy(options, WritePolicy)
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

    def append(key, bins, options={})
      policy = create_policy(options, WritePolicy)
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

    def prepend(key, bins, options={})
      policy = create_policy(options, WritePolicy)
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

    def add(key, bins, options={})
      policy = create_policy(options, WritePolicy)
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

    def delete(key, options={})
      policy = create_policy(options, WritePolicy)
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
      policy = create_policy(options, WritePolicy)
      str_cmd = "truncate:namespace=#{namespace}"
      str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      str_cmd << ";lut=#{(before_last_update.to_f * 1_000_000_000.0).round}" if before_last_update

      # Send index command to one node. That node will distribute the command to other nodes.
      response = send_info_command(policy, str_cmd).upcase
      return if response == 'OK'
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

    def touch(key, options={})
      policy = create_policy(options, WritePolicy)
      command = TouchCommand.new(@cluster, policy, key)
      execute_command(command)
    end

    #-------------------------------------------------------
    # Existence-Check Operations
    #-------------------------------------------------------

    ##
    #  Determines if a record key exists.
    #  The policy can be used to specify timeouts.
    def exists(key, options={})
      policy = create_policy(options, Policy)
      command = ExistsCommand.new(@cluster, policy, key)
      execute_command(command)
      command.exists
    end

    #  Check if multiple record keys exist in one batch call.
    #  The returned array bool is in positional order with the original key array order.
    #  The policy can be used to specify timeouts.
    def batch_exists(keys, options={})
      policy = create_policy(options, Policy)

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be marked true
      exists_array = Array.new(keys.length)

      key_map = BatchItem.generate_map(keys)

      batch_execute(keys) do |node, bns|
        BatchCommandExists.new(node, bns, policy, key_map, exists_array)
      end
      exists_array
    end

    #-------------------------------------------------------
    # Read Record Operations
    #-------------------------------------------------------

    #  Read record header and bins for specified key.
    #  The policy can be used to specify timeouts.
    def get(key, bin_names=[], options={})
      policy = create_policy(options, Policy)

      command = ReadCommand.new(@cluster, policy, key, bin_names)
      execute_command(command)
      command.record
    end

    #  Read record generation and expiration only for specified key.  Bins are not read.
    #  The policy can be used to specify timeouts.
    def get_header(key, options={})
      policy = create_policy(options, Policy)
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


    #-------------------------------------------------------
    # Batch Read Operations
    #-------------------------------------------------------

    #  Read multiple record headers and bins for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts.

    def batch_get(keys, bin_names=[], options={})
      policy = create_policy(options, BatchPolicy)
      records = Array.new(keys.length)
      # Use old batch direct protocol where batch reads are handled by direct low-level batch server 
      # database routines.  The batch direct protocol can be faster when there is a single namespace, 
      # but there is one important drawback.  The batch direct protocol will not proxy to a different 
      # server node when the mapped node has migrated a record to another node (resulting in not
      # found record).  

      # This can happen after a node has been added/removed from the cluster and there is a lag 
      # between records being migrated and client partition map update (once per second).

      # The new batch index protocol will perform this record proxy when necessary.
      # Default: false (use new batch index protocol if server supports it)    
      if policy.use_batch_direct 
        key_map = BatchItem.generate_map(keys)

        batch_execute(keys) do |node, bns|
          BatchDirectCommandGet.new(node, bns, policy, key_map, bin_names.uniq, records, INFO1_READ)
        end
      else
        policy = create_policy(options, Policy)
        records = Array.new(keys.length)        
        batch_execute_index(keys) do |bn|
          BatchIndexCommandGet.new(bn, policy, bin_names, records, bin_names.length == 0 ? (INFO1_READ | INFO1_GET_ALL) : INFO1_READ)
        end      
      end
      records
    end

    #  Read multiple record header data for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts.
    def batch_get_header(keys, options={})
      policy = create_policy(options, BatchPolicy)

      # wait until all migrations are finished
      # TODO: Fix this and implement
      # @cluster.WaitUntillMigrationIsFinished(policy.timeout)

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be set to record
      records = Array.new(keys.length)

      key_map = BatchItem.generate_map(keys)

      batch_execute(keys) do |node, bns|
        BatchDirectCommandGet.new(node, bns, policy, key_map, nil, records, INFO1_READ | INFO1_NOBINDATA)
      end

      records
    end

    #-------------------------------------------------------
    # Generic Database Operations
    #-------------------------------------------------------

    #  Perform multiple read/write operations on a single key in one batch call.
    #  An example would be to add an integer value to an existing record and then
    #  read the result, all in one database call. Operations are executed in
    #  the order they are specified.
    def operate(key, operations, options={})
      policy = create_policy(options, WritePolicy)

      command = OperateCommand.new(@cluster, policy, key, operations)
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
    def register_udf_from_file(client_path, server_path, language, options={})
      udf_body = File.read(client_path)
      register_udf(udf_body, server_path, language, options)
    end

    #  Register package containing user defined functions with server.
    #  This asynchronous server call will return before command is complete.
    #  The user can optionally wait for command completion by using the returned
    #  RegisterTask instance.
    #
    #  This method is only supported by Aerospike 3 servers.
    def register_udf(udf_body, server_path, language, options={})
      content = Base64.strict_encode64(udf_body).force_encoding('binary')

      str_cmd = "udf-put:filename=#{server_path};content=#{content};"
      str_cmd << "content-len=#{content.length};udf-type=#{language};"
      # Send UDF to one node. That node will distribute the UDF to other nodes.
      response_map = @cluster.request_info(@default_policy, str_cmd)

      res = {}
      response_map.each do |k, response|
        vals = response.to_s.split(';')
        vals.each do |pair|
          k, v = pair.split("=", 2)
          res[k] = v
        end
      end

      if res['error']
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
    def remove_udf(udf_name, options={})
      str_cmd = "udf-remove:filename=#{udf_name};"

      # Send command to one node. That node will distribute it to other nodes.
      # Send UDF to one node. That node will distribute the UDF to other nodes.
      response_map = @cluster.request_info(@default_policy, str_cmd)
      _, response = response_map.first

      if response == 'ok'
        UdfRemoveTask.new(@cluster, udf_name)
      else
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_ERROR, response)
      end
    end

    #  ListUDF lists all packages containing user defined functions in the server.
    #  This method is only supported by Aerospike 3 servers.
    def list_udf(options={})
      str_cmd = 'udf-list'

      # Send command to one node. That node will distribute it to other nodes.
      response_map = @cluster.request_info(@default_policy, str_cmd)
      _, response = response_map.first

      vals = response.split(';')

      vals.map do |udf_info|
        next if udf_info.strip! == ''

        udf_parts = udf_info.split(',')
        udf = UDF.new
        udf_parts.each do |values|
          k, v = values.split('=', 2)
          case k
          when 'filename'
            udf.filename = v
          when 'hash'
            udf.hash = v
          when 'type'
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
    def execute_udf(key, package_name, function_name, args=[], options={})
      policy = create_policy(options, WritePolicy)

      command = ExecuteCommand.new(@cluster, policy, key, package_name, function_name, args)
      execute_command(command)

      record = command.record

      return nil if !record || record.bins.empty?

      result_map = record.bins

      # User defined functions don't have to return a value.
      key, obj = result_map.detect{ |k, _| k.include?('SUCCESS') }
      return obj if key

      key, obj = result_map.detect{ |k, _| k.include?('FAILURE') }
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
    def execute_udf_on_query(statement, package_name, function_name, function_args=[], options={})
      policy = create_policy(options, QueryPolicy)

      nodes = @cluster.nodes
      if nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Executing UDF failed because cluster is empty.")
      end

      # TODO: wait until all migrations are finished
      statement.set_aggregate_function(package_name, function_name, function_args, false)

      # Use a thread per node
      nodes.each do |node|
        Thread.new do
          Thread.current.abort_on_exception = true
          begin
            command = QueryCommand.new(node, policy, statement, nil)
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
    def create_index(namespace, set_name, index_name, bin_name, index_type, collection_type=nil, options={})
      if options.nil? && collection_type.is_a?(Hash)
        options, collection_type = collection_type, nil
      end
      policy = create_policy(options, WritePolicy)
      str_cmd = "sindex-create:ns=#{namespace}"
      str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      str_cmd << ";indexname=#{index_name};numbins=1"
      str_cmd << ";indextype=#{collection_type.to_s.upcase}" if collection_type
      str_cmd << ";indexdata=#{bin_name},#{index_type.to_s.upcase}"
      str_cmd << ";priority=normal"

      # Send index command to one node. That node will distribute the command to other nodes.
      response = send_info_command(policy, str_cmd).upcase
      if response == 'OK'
        # Return task that could optionally be polled for completion.
        return IndexTask.new(@cluster, namespace, index_name)
      end

      if response.start_with?('FAIL:200')
        # Index has already been created.  Do not need to poll for completion.
        return IndexTask.new(@cluster, namespace, index_name, true)
      end

      raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INDEX_GENERIC, "Create index failed: #{response}")
    end

    #  Delete secondary index.
    #  This method is only supported by Aerospike 3 servers.
    def drop_index(namespace, set_name, index_name, options={})
      policy = create_policy(options, WritePolicy)
      str_cmd = "sindex-delete:ns=#{namespace}"
      str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      str_cmd << ";indexname=#{index_name}"

      # Send index command to one node. That node will distribute the command to other nodes.
      response = send_info_command(policy, str_cmd).upcase
      return if response == 'OK'

      # Index did not previously exist. Return without error.
      return if response.start_with?('FAIL:201')

      raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INDEX_GENERIC, "Drop index failed: #{response}")
    end

    def request_info(*commands)
      @cluster.request_info(@default_policy, *commands)
    end

    #-------------------------------------------------------
    # Scan Operations
    #-------------------------------------------------------

    def scan_all(namespace, set_name, bin_names=[], options={})
      policy = create_policy(options, ScanPolicy)

      # wait until all migrations are finished
      # TODO: implement
      # @cluster.WaitUntillMigrationIsFinished(policy.timeout)

      # Retry policy must be one-shot for scans.
      # copy on write for policy
      new_policy = policy.clone

      nodes = @cluster.nodes
      if nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
      end

      recordset = Recordset.new(policy.record_queue_size, nodes.length, :scan)

      if policy.concurrent_nodes
        # Use a thread per node
        nodes.each do |node|
          Thread.new do
            Thread.current.abort_on_exception = true
            command = ScanCommand.new(node, new_policy, namespace, set_name, bin_names, recordset)
            begin
              execute_command(command)
            rescue => e
              Aerospike.logger.error(e.backtrace.join("\n")) unless e == SCAN_TERMINATED_EXCEPTION
              recordset.cancel(e)
            ensure
              recordset.thread_finished
            end
          end
        end
      else
        Thread.new do
          Thread.current.abort_on_exception = true
          nodes.each do |node|
            command = ScanCommand.new(node, new_policy, namespace, set_name, bin_names, recordset)
            begin
              execute_command(command)
            rescue => e
              Aerospike.logger.error(e.backtrace.join("\n")) unless e == SCAN_TERMINATED_EXCEPTION
              recordset.cancel(e)
            ensure
              recordset.thread_finished
            end
          end
        end
      end

      recordset
    end

    # ScanNode reads all records in specified namespace and set, from one node only.
    # The policy can be used to specify timeouts.
    def scan_node(node, namespace, set_name, bin_names=[], options={})
      policy = create_policy(options, ScanPolicy)
      # wait until all migrations are finished
      # TODO: implement
      # @cluster.WaitUntillMigrationIsFinished(policy.timeout)

      # Retry policy must be one-shot for scans.
      # copy on write for policy
      new_policy = policy.clone
      new_policy.max_retries = 0

      node = @cluster.get_node_by_name(node) unless node.is_a?(Aerospike::Node)

      recordset = Recordset.new(policy.record_queue_size, 1, :scan)

      Thread.new do
        Thread.current.abort_on_exception = true
        command = ScanCommand.new(node, new_policy, namespace, set_name, bin_names, recordset)
        begin
          execute_command(command)
        rescue => e
          Aerospike.logger.error(e.backtrace.join("\n")) unless e == SCAN_TERMINATED_EXCEPTION
          recordset.cancel(e)
        ensure
          recordset.thread_finished
        end
      end

      recordset
    end

    #--------------------------------------------------------
    # Query functions (Supported by Aerospike 3 servers only)
    #--------------------------------------------------------

    # Query executes a query and returns a recordset.
    # The query executor puts records on a channel from separate goroutines.
    # The caller can concurrently pops records off the channel through the
    # record channel.
    #
    # This method is only supported by Aerospike 3 servers.
    # If the policy is nil, a default policy will be generated.
    def query(statement, options={})
      policy = create_policy(options, QueryPolicy)
      new_policy = policy.clone

      nodes = @cluster.nodes
      if nodes.empty?
        raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
      end

      recordset = Recordset.new(policy.record_queue_size, nodes.length, :query)

      # Use a thread per node
      nodes.each do |node|
        Thread.new do
          Thread.current.abort_on_exception = true
          command = QueryCommand.new(node, new_policy, statement, recordset)
          begin
            execute_command(command)
          rescue => e
            Aerospike.logger.error(e.backtrace.join("\n")) unless e == QUERY_TERMINATED_EXCEPTION
            recordset.cancel(e)
          ensure
            recordset.thread_finished
          end
        end
      end

      recordset
    end

    #-------------------------------------------------------
    # User administration
    #-------------------------------------------------------

    # Create user with password and roles. Clear-text password will be hashed using bcrypt
    # before sending to server.
    def create_user(user, password, roles, options={})
      policy = create_policy(options, AdminPolicy)
      hash = AdminCommand.hash_password(password)
      command = AdminCommand.new
      command.create_user(@cluster, policy, user, hash, roles)
    end

    # Remove user from cluster.
    def drop_user(user, options={})
      policy = create_policy(options, AdminPolicy)
      command = AdminCommand.new
      command.drop_user(@cluster, policy, user)
    end

    # Change user's password. Clear-text password will be hashed using bcrypt before sending to server.
    def change_password(user, password, options={})
      raise Aerospike::Exceptions::Aerospike.new(INVALID_USER) unless @cluster.user && @cluster.user != ""
      policy = create_policy(options, AdminPolicy)

      hash = AdminCommand.hash_password(password)
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
    def grant_roles(user, roles, options={})
      policy = create_policy(options, AdminPolicy)
      command = AdminCommand.new
      command.grant_roles(@cluster, policy, user, roles)
    end

    # Remove roles from user's list of roles.
    def revoke_roles(user, roles, options={})
      policy = create_policy(options, AdminPolicy)
      command = AdminCommand.new
      command.revoke_roles(@cluster, policy, user, roles)
    end

    # Retrieve roles for a given user.
    def query_user(user, options={})
      policy = create_policy(options, AdminPolicy)
      command = AdminCommand.new
      command.query_user(@cluster, policy, user)
    end

    # Retrieve all users and their roles.
    def query_users(options={})
      policy = create_policy(options, AdminPolicy)
      command = AdminCommand.new
      command.query_users(@cluster, policy)
    end

    private

    def send_info_command(policy, command)
      policy ||= default_policy
      Aerospike.logger.debug { "Sending info command: #{command}" }
      _, response = @cluster.request_info(policy, command).first
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

    def create_policy(policy, policy_klass)
      case policy
      when nil
        policy_klass.new
      when policy_klass
        policy
      when Hash
        policy_klass.new(policy)
      else
        fail TypeError, "policy should be a #{policy_klass.name} instance or a Hash"
      end
    end

    def parse_hosts(hosts)
      case hosts
      when Host
        [hosts]
      when Array
        hosts
      when String
        hosts.split(?,).map { |host|
          (addr, port) = host.split(?:)
          port ||= 3000
          Host.new(addr, port.to_i)
        }
      else
        fail TypeError, "hosts should be a Host object, an Array of Host objects, or a String"
      end
    end

    def cluster=(cluster)
      @cluster = cluster
    end

    def cluster_config_changed(cluster)
      Aerospike.logger.debug { "Cluster config change detected; active nodes: #{cluster.nodes.map(&:name)}" }
      setup_command_validators
    end

    def setup_command_validators
      Aerospike.logger.debug { "Cluster features: #{@cluster.features.get.to_a}" }
      validators = []

      # guard against unsupported particle types
      unsupported_particle_types = []
      unsupported_particle_types << Aerospike::ParticleType::DOUBLE unless @cluster.supports_feature?("float")
      unsupported_particle_types << Aerospike::ParticleType::GEOJSON unless @cluster.supports_feature?("geo")
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


    def batch_execute_index(keys)
      batch_nodes = BatchIndexNode.generate_list(@cluster, keys)
      threads = []

      # Use a thread per  node
      batch_nodes.each do |batch_node|
        # copy to avoid race condition
        bn = batch_node
        threads << Thread.new do
          Thread.current.abort_on_exception = true
          command = yield bn          
          execute_command(command)
        end
      end
      threads.each do |thr| thr.join end
    end

    def batch_execute(keys)
      batch_nodes = BatchDirectNode.generate_list(@cluster, keys)
      threads = []

      # Use a thread per namespace per node
      batch_nodes.each do |batch_node|
        # copy to avoid race condition
        bn = batch_node
        bn.batch_namespaces.each do |bns|
          threads << Thread.new do
            Thread.current.abort_on_exception = true
            command = yield bn.node, bns
            execute_command(command)
          end
        end
      end

      threads.each(&:join)
    end

  end # class

end # module
