# encoding: utf-8
# Copyright 2014-2018 Aerospike, Inc.
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
require 'aerospike/utils/packer'
require 'aerospike/utils/unpacker'
require 'aerospike/utils/buffer'
require 'aerospike/utils/string_parser'
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
require 'aerospike/command/unsupported_particle_type_validator'
require 'aerospike/key'
require 'aerospike/operation'
require 'aerospike/cdt/list_operation'
require 'aerospike/cdt/map_operation'
require 'aerospike/cdt/map_order'
require 'aerospike/cdt/map_return_type'
require 'aerospike/cdt/map_write_mode'
require 'aerospike/cdt/map_policy'
require 'aerospike/geo_json'
require 'aerospike/ttl'

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

require 'aerospike/cluster'
require 'aerospike/cluster/connection'
require 'aerospike/cluster/partition'
require 'aerospike/cluster/find_node'
require 'aerospike/cluster/partition_tokenizer_new'
require 'aerospike/cluster/partition_tokenizer_old'
require 'aerospike/node'
require 'aerospike/node/generation'
require 'aerospike/node/refresh/failed'
require 'aerospike/node/refresh/friends'
require 'aerospike/node/refresh/info'
require 'aerospike/node/refresh/partitions'
require 'aerospike/node/refresh/peers'
require 'aerospike/node/refresh/reset'
require 'aerospike/node/verify/cluster_name'
require 'aerospike/node/verify/name'
require 'aerospike/node/verify/partition_generation'
require 'aerospike/node/verify/peers_generation'
require 'aerospike/node_validator'
require 'aerospike/peer'
require 'aerospike/peers'
require 'aerospike/peers/fetch'
require 'aerospike/peers/parse'
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
end
