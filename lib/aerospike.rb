# encoding: utf-8
require "logger"
require "stringio"
require "monitor"
require "timeout"
require 'resolv'
require 'msgpack'
require 'bcrypt'

require 'aerospike/atomic/atomic'

require 'aerospike/client'
require 'aerospike/utils/pool'
require 'aerospike/utils/epoc'
require 'aerospike/utils/buffer'
require 'aerospike/host'
require 'aerospike/loggable'
require 'aerospike/record'
require 'aerospike/result_code'
require 'aerospike/version'
require 'aerospike/value/particle_type'
require 'aerospike/value/value'
require 'aerospike/command/single_command'
require 'aerospike/command/batch_node'
require 'aerospike/command/field_type'
require 'aerospike/command/command'
require 'aerospike/command/execute_command'
require 'aerospike/command/write_command'
require 'aerospike/command/batch_item'
require 'aerospike/command/operate_command'
require 'aerospike/command/exists_command'
require 'aerospike/command/batch_command_get'
require 'aerospike/command/batch_command'
require 'aerospike/command/read_header_command'
require 'aerospike/command/touch_command'
require 'aerospike/command/batch_command_exists'
require 'aerospike/command/read_command'
require 'aerospike/command/delete_command'
require 'aerospike/command/admin_command'
require 'aerospike/key'
require 'aerospike/operation'

require 'aerospike/policy/client_policy'
require 'aerospike/policy/priority'
require 'aerospike/policy/record_exists_action'
require 'aerospike/policy/generation_policy'
require 'aerospike/policy/policy'
require 'aerospike/policy/write_policy'
require 'aerospike/policy/scan_policy'
require 'aerospike/policy/query_policy'
require 'aerospike/policy/consistency_level'
require 'aerospike/policy/commit_level'
require 'aerospike/policy/admin_policy'

require 'aerospike/cluster/connection'
require 'aerospike/cluster/cluster'
require 'aerospike/cluster/node_validator'
require 'aerospike/cluster/partition'
require 'aerospike/cluster/node'
require 'aerospike/cluster/partition_tokenizer_new'
require 'aerospike/cluster/partition_tokenizer_old'
require 'aerospike/ldt/large_map'
require 'aerospike/ldt/large_set'
require 'aerospike/ldt/large_stack'
require 'aerospike/ldt/large'
require 'aerospike/ldt/large_list'
require 'aerospike/info'
require 'aerospike/udf'
require 'aerospike/bin'
require 'aerospike/aerospike_exception'
require 'aerospike/user_role'

require 'aerospike/task/index_task'
require 'aerospike/task/execute_task'
require 'aerospike/task/udf_remove_task'
require 'aerospike/task/udf_register_task'
require 'aerospike/task/task'
require 'aerospike/language'

require 'aerospike/query/recordset'
require 'aerospike/query/filter'
require 'aerospike/query/stream_command'
require 'aerospike/query/query_command'
require 'aerospike/query/scan_command'
require 'aerospike/query/statement'

module Aerospike
  extend Loggable

  # Show Tend info in logger
  @@tend_info = true

  #
  # Return the value of tend_info
  #
  # == Returns:
  # Booleand indicating if tend_info must be showed
  #
  def self.tend_info
    @@tend_info
  end

  #
  # Set the value for tend_info
  #
  # == Parameters:
  # tend::
  #   Boolean to enable or disable tend_info logger
  #
  def self.tend_info=(tend)
    @@tend_info = tend
  end

  #
  # Initialize the config of Aerospike Ruby gem. Available config:
  #   - tend_info
  #
  def self.setup
    yield self
  end
end
