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
  # client = Client.new('192.168.0.1', 3000)
  #
  # #=> raises Aerospike::Exceptions::Timeout if a +:timeout+ is specified and
  # +:fail_if_not_connected+ set to true

  class Client

    attr_accessor :default_policy, :default_write_policy

    def initialize(host, port, options={})
      @default_policy = Policy.new
      @default_write_policy = WritePolicy.new

      policy = opt_to_client_policy(options)

      @cluster = Cluster.new(policy, Host.new(host, port))

      self
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
      @cluster.nodes.map do |node|
        node.get_name
      end
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
      policy = opt_to_write_policy(options)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::WRITE)
      command.execute
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
      policy = opt_to_write_policy(options)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::APPEND)
      command.execute
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
      policy = opt_to_write_policy(options)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::PREPEND)
      command.execute
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
      policy = opt_to_write_policy(options)
      command = WriteCommand.new(@cluster, policy, key, hash_to_bins(bins), Aerospike::Operation::ADD)
      command.execute
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
      policy = opt_to_write_policy(options)
      command = DeleteCommand.new(@cluster, policy, key)
      command.execute
      command.existed
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
      policy = opt_to_write_policy(options)
      command = TouchCommand.new(@cluster, policy, key)
      command.execute
    end

    #-------------------------------------------------------
    # Existence-Check Operations
    #-------------------------------------------------------

    ##
    #  Determines if a record key exists.
    #  The policy can be used to specify timeouts.
    def exists(key, options={})
      policy = opt_to_policy(options)
      command = ExistsCommand.new(@cluster, policy, key)
      command.execute
      command.exists
    end

    #  Check if multiple record keys exist in one batch call.
    #  The returned array bool is in positional order with the original key array order.
    #  The policy can be used to specify timeouts.
    def batch_exists(keys, options={})
      policy = opt_to_policy(options)

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be marked true
      exists_array = Array.new(keys.length)

      key_map = BatchItem.generate_map(keys)

      cmd_gen = Proc.new do |node, bns|
        BatchCommandExists.new(node, bns, policy, key_map, exists_array)
      end

      batch_execute(keys, &cmd_gen)
      exists_array
    end

    #-------------------------------------------------------
    # Read Record Operations
    #-------------------------------------------------------

    #  Read record header and bins for specified key.
    #  The policy can be used to specify timeouts.
    def get(key, bin_names=[], options={})
      policy = opt_to_policy(options)

      command = ReadCommand.new(@cluster, policy, key, bin_names)
      command.execute
      command.record
    end

    #  Read record generation and expiration only for specified key.  Bins are not read.
    #  The policy can be used to specify timeouts.
    def get_header(key, options={})
      policy = opt_to_policy(options)
      command = ReadHeaderCommand.new(@cluster, policy, key)
      command.execute
      command.record
    end

    #-------------------------------------------------------
    # Batch Read Operations
    #-------------------------------------------------------

    #  Read multiple record headers and bins for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts.
    def batch_get(keys, bin_names=[], options={})
      policy = opt_to_policy(options)

      # wait until all migrations are finished
      # TODO: implement
      # @cluster.WaitUntillMigrationIsFinished(policy.timeout)

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be set to record
      records = Array.new(keys.length)

      key_map = BatchItem.generate_map(keys)

      cmd_gen = Proc.new do |node, bns|
        BatchCommandGet.new(node, bns, policy, key_map, bin_names.uniq, records, INFO1_READ)
      end

      batch_execute(keys, &cmd_gen)
      records
    end

    #  Read multiple record header data for specified keys in one batch call.
    #  The returned records are in positional order with the original key array order.
    #  If a key is not found, the positional record will be nil.
    #  The policy can be used to specify timeouts.
    def batch_get_header(keys, options={})
      policy = opt_to_policy(options)

      # wait until all migrations are finished
      # TODO: Fix this and implement
      # @cluster.WaitUntillMigrationIsFinished(policy.timeout)

      # same array can be used without sychronization;
      # when a key exists, the corresponding index will be set to record
      records = Array.new(keys.length)

      key_map = BatchItem.generate_map(keys)

      cmd_gen = Proc.new do |node, bns|
        BatchCommandGet.new(node, bns, policy, key_map, nil, records, INFO1_READ | INFO1_NOBINDATA)
      end

      batch_execute(keys, &cmd_gen)
      records
    end

    #-------------------------------------------------------
    # Generic Database Operations
    #-------------------------------------------------------

    #  Perform multiple read/write operations on a single key in one batch call.
    #  An example would be to add an integer value to an existing record and then
    #  read the result, all in one database call.
    #
    #  Write operations are always performed first, regardless of operation order
    #  relative to read operations.
    def operate(key, operations, options={})
      policy = opt_to_write_policy(options)

      command = OperateCommand.new(@cluster, policy, key, operations)
      command.execute
      command.record
    end

    #-------------------------------------------------------------------
    # Large collection functions (Supported by Aerospike 3 servers only)
    #-------------------------------------------------------------------

    #  Initialize large list operator.  This operator can be used to create and manage a list
    #  within a single bin.
    #
    #  This method is only supported by Aerospike 3 servers.
    def get_large_list(key, bin_name, user_module=nil, options={})
      LargeList.new(self, opt_to_write_policy(options), key, bin_name, user_module)
    end

    #  Initialize large map operator.  This operator can be used to create and manage a map
    #  within a single bin.
    #
    #  This method is only supported by Aerospike 3 servers.
    def get_large_map(key, bin_name, user_module=nil, options={})
      LargeMap.new(self, opt_to_write_policy(options), key, bin_name, user_module)
    end

    #  Initialize large set operator.  This operator can be used to create and manage a set
    #  within a single bin.
    #
    #  This method is only supported by Aerospike 3 servers.
    def get_large_set(key, bin_name, user_module=nil, options={})
      LargeSet.new(self, opt_to_write_policy(options), key, bin_name, user_module)
    end

    #  Initialize large stack operator.  This operator can be used to create and manage a stack
    #  within a single bin.
    #
    #  This method is only supported by Aerospike 3 servers.
    def get_large_stack(key, bin_name, user_module=nil, options={})
      LargeStack.new(self, opt_to_write_policy(options), key, bin_name, user_module)
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
      register_udf(udf_body, server_path, language, options={})
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
      response, _ = response_map.first

      res = {}
      vals = response.split(';')
      vals.each do |pair|
        k, v = pair.split("=", 2)
        res[k] = v
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
      policy = opt_to_write_policy(options)

      command = ExecuteCommand.new(@cluster, policy, key, package_name, function_name, args)
      command.execute

      record = command.record

      return nil if !record || record.bins.length == 0

      result_map = record.bins

      # User defined functions don't have to return a value.
      key, obj = result_map.detect{|k, v| k.include?('SUCCESS')}
      if key
        return obj
      end

      key, obj = result_map.detect{|k, v| k.include?('FAILURE')}
      if key
        raise Aerospike::Exceptions::Aerospike.new(UDF_BAD_RESPONSE, "#{obj}")
      end

      raise Aerospike::Exception::Aerospike.new(UDF_BAD_RESPONSE, "Invalid UDF return value")
    end

    #  Create secondary index.
    #  This asynchronous server call will return before command is complete.
    #  The user can optionally wait for command completion by using the returned
    #  IndexTask instance.
    #
    #  This method is only supported by Aerospike 3 servers.
    #  index_type should be between :string or :numeric
    def create_index(namespace, set_name, index_name, bin_name, index_type, options={})
      policy = opt_to_write_policy(options)
      str_cmd = "sindex-create:ns=#{namespace}"
      str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      str_cmd << ";indexname=#{index_name};numbins=1;indexdata=#{bin_name},#{index_type.to_s.upcase}"
      str_cmd << ";priority=normal"

      # Send index command to one node. That node will distribute the command to other nodes.
      response_map = send_info_command(policy, str_cmd)
      _, response = response_map.first
      response = response.to_s.upcase

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
      policy = opt_to_write_policy(options)
      str_cmd = "sindex-delete:ns=#{namespace}"
      str_cmd << ";set=#{set_name}" unless set_name.to_s.strip.empty?
      str_cmd << ";indexname=#{index_name}"

      # Send index command to one node. That node will distribute the command to other nodes.
      response_map = send_info_command(policy, str_cmd)
      _, response = response_map.first
      response = response.to_s.upcase

      return if response == 'OK'

      # Index did not previously exist. Return without error.
      return if response.start_with?('FAIL:201')

      raise Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::INDEX_GENERICINDEX_GENERIC, "Drop index failed: #{response}")
    end

    def request_info(*commands)
      @cluster.request_info(@default_policy, *commands)
    end

    private

    def send_info_command(policy, command)
      @cluster.request_info(@default_policy, command)
    end

    def hash_to_bins(hash)
      if hash.is_a?(Bin)
        [hash]
      elsif hash.is_a?(Array)
        hash # it is a list of bins
      else
        hash.map do |k, v|
          raise Aerospike::Exceptions::Parse("bin name `#{k}` is not a string.") unless k.is_a?(String)
          Bin.new(k, v)
        end
      end
    end

    def opt_to_client_policy(options)
      if options == {} || options.nil?
        ClientPolicy.new
      elsif options.is_a?(ClientPolicy)
        options
      elsif options.is_a?(Hash)
        ClientPolicy.new(
          options[:timeout],
          options[:connection_queue_size],
          options[:fail_if_not_connected],
        )
      end
    end

    def opt_to_policy(options)
      if options == {} || options.nil?
        @default_policy
      elsif options.is_a?(Policy)
        options
      elsif options.is_a?(Hash)
        Policy.new(
          options[:priority],
          options[:timeout],
          options[:max_retiries],
          options[:sleep_between_retries],
        )
      end
    end

    def opt_to_write_policy(options)
      if options == {} || options.nil?
        @default_write_policy
      elsif options.is_a?(WritePolicy)
        options
      elsif options.is_a?(Hash)
        WritePolicy.new(
          options[:record_exists_action],
          options[:gen_policy],
          options[:generation],
          options[:expiration],
          options[:send_key]
        )
      end
    end

    def batch_execute(keys, &cmd_gen)
      batch_nodes = BatchNode.generate_list(@cluster, keys)
      threads = []

      # Use a thread per namespace per node
      batch_nodes.each do |batch_node|
        # copy to avoid race condition
        bn = batch_node
        bn.batch_namespaces.each do |bns|
          threads << Thread.new do
            abort_on_exception=true
            command = cmd_gen.call(bn.node, bns)
            command.execute
          end
        end
      end

      threads.each { |thr| thr.join }
    end

  end # class

end #module
