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

  class BatchAttr

    attr_reader :filter_exp, :read_attr, :write_attr, :info_attr, :expiration, :generation, :has_write, :send_key

    def initialize(ops = nil, opt = {})
      rp = create_policy(opt, BatchPolicy, nil)
      wp = create_policy(opt, BatchWritePolicy, nil)

      read_all_bins = false
      read_header = false
      has_read = false
      has_write_op = false

      ops&.each do |op|
        case op.op_type
        when Operation::BIT_READ, Operation::EXP_READ, Operation::HLL_READ, Operation::CDT_READ, Operation::READ	# Read all bins if no bin is specified.
          read_all_bins = op.bin_name.nil?
          has_read = true

        when Operation::READ_HEADER
          read_header = true
          has_read = true

        else
          has_write_op = true
        end
      end

      if has_write_op
        set_batch_write(wp)

        if has_read
          @read_attr |= Aerospike::INFO1_READ

          if read_all_bins
            @read_attr |= Aerospike::INFO1_GET_ALL
          elsif read_header
            @read_attr |= Aerospike::INFO1_NOBINDATA
          end
        end
      else
        set_batch_read(rp)

        if read_all_bins
          @read_attr |= Aerospike::INFO1_GET_ALL
        elsif read_header
          @read_attr |= Aerospike::INFO1_NOBINDATA
        end
      end
    end

    def set_read(rp)
      @filter_exp = nil
      @read_attr = Aerospike::INFO1_READ

      @write_attr = 0
      @info_attr = 0

      @expiration = 0
      @generation = 0
      @has_write = false
      @send_key = false
    end

    def set_batch_read(rp)
      @filter_exp = rp.filter_exp
      @read_attr = Aerospike::INFO1_READ

      @write_attr = 0
      @info_attr = 0

      @expiration = 0
      @generation = 0
      @has_write = false
      @send_key = false
    end

    def adjust_read(ops)
        read_all_bins = false
        read_header = false

        ops.each do |op|
       case op.op_type
       when Operation::BIT_READ, Operation::EXP_READ, Operation::HLL_READ, Operation::CDT_READ, Operation::READ	# Read all bins if no bin is specified.
              read_all_bins = op.bin_name.nil?
       when Operation::READ_HEADER
            read_header = true
       end
        end

        if read_all_bins
          @read_attr |= Aerospike::INFO1_GET_ALL
        elsif read_header
          @read_attr |= Aerospike::INFO1_NOBINDATA
        end
    end

    def adjust_read_all_bins(read_all_bins)
      @read_attr |= read_all_bins ? Aerospike::INFO1_GET_ALL : Aerospike::INFO1_NOBINDATA
    end

    def set_write(wp)
      @filter_exp = nil
      @read_attr = 0
      @write_attr = Aerospike::INFO2_WRITE | Aerospike::INFO2_RESPOND_ALL_OPS
      @info_attr = 0
      @expiration = 0
      @generation = 0
      @has_write = true
      @send_key = wp.send_key
    end

    def set_batch_write(wp)
      @filter_exp = wp.filter_exp
      @read_attr = 0
      @write_attr = Aerospike::INFO2_WRITE | Aerospike::INFO2_RESPOND_ALL_OPS
      @info_attr = 0
      @expiration = wp.expiration
      @has_write = true
      @send_key = wp.send_key

      case wp.generation_policy
      when GenerationPolicy::NONE
        @generation = 0
      when GenerationPolicy::EXPECT_GEN_EQUAL
        @generation = wp.generation
        @write_attr |= Aerospike::INFO2_GENERATION
      when GenerationPolicy::EXPECT_GEN_GT
        @generation = wp.generation
        @write_attr |= Aerospike::INFO2_GENERATION_GT
      else
        @generation = 0
      end

      case wp.record_exists_action
      when RecordExistsAction::UPDATE
      # NOOP
      when RecordExistsAction::UPDATE_ONLY
        @info_attr |= Aerospike::INFO3_UPDATE_ONLY
      when RecordExistsAction::REPLACE
        @info_attr |= Aerospike::INFO3_CREATE_OR_REPLACE
      when RecordExistsAction::REPLACE_ONLY
        @info_attr |= Aerospike::INFO3_REPLACE_ONLY
      when RecordExistsAction::CREATE_ONLY
        @write_attr |= Aerospike::INFO2_CREATE_ONLY
      end

      if wp.durable_delete
        @write_attr |= Aerospike::INFO2_DURABLE_DELETE
      end

      if wp.commit_level == CommitLevel::COMMIT_MASTER
        @info_attr |= Aerospike::INFO3_COMMIT_MASTER
      end
    end

    def adjust_write(ops)
      read_all_bins = false
      read_header = false
      has_read = false

      ops.each do |op|
        case op.op_type
        when Operation::BIT_READ, Operation::EXP_READ, Operation::HLL_READ, Operation::CDT_READ, Operation::READ	# Read all bins if no bin is specified.
          read_all_bins = op.bin_name.nil?
          has_read = true

        when Operation::READ_HEADER
          read_header = true
          has_read = true

        end
      end

      if has_read
        @read_attr |= Aerospike::INFO1_READ

        if read_all_bins
          @read_attr |= Aerospike::INFO1_GET_ALL
        elsif read_header
          @read_attr |= Aerospike::INFO1_NOBINDATA
        end
      end
    end

    def set_udf(up)
      @filter_exp = nil
      @read_attr = 0
      @write_attr = Aerospike::INFO2_WRITE
      @info_attr = 0
      @expiration = 0
      @generation = 0
      @has_write = true
      @send_key = up.send_key
    end

    def set_batch_udf(up)
      @filter_exp = up.filter_exp
      @read_attr = 0
      @write_attr = Aerospike::INFO2_WRITE
      @info_attr = 0
      @expiration = up.expiration
      @generation = 0
      @has_write = true
      @send_key = up.send_key

      if up.durable_delete
        @write_attr |= Aerospike::INFO2_DURABLE_DELETE
      end

      if up.commit_level == CommitLevel::COMMIT_MASTER
        @info_attr |= Aerospike::INFO3_COMMIT_MASTER
      end
    end

    def set_delete(dp)
      @filter_exp = nil
      @read_attr = 0
      @write_attr = Aerospike::INFO2_WRITE | Aerospike::INFO2_RESPOND_ALL_OPS | Aerospike::INFO2_DELETE
      @info_attr = 0
      @expiration = 0
      @generation = 0
      @has_write = true
      @send_key = dp.send_key
    end

    def set_batch_delete(dp)
      @filter_exp = dp.filter_exp
      @read_attr = 0
      @write_attr = Aerospike::INFO2_WRITE | Aerospike::INFO2_RESPOND_ALL_OPS | Aerospike::INFO2_DELETE
      @info_attr = 0
      @expiration = 0
      @has_write = true
      @send_key = dp.send_key

      case dp.generation_policy
      when GenerationPolicy::NONE
        @generation = 0
      when GenerationPolicy::EXPECT_GEN_EQUAL
        @generation = dp.generation
        @write_attr |= Aerospike::INFO2_GENERATION
      when GenerationPolicy::EXPECT_GEN_GT
        @generation = dp.generation
        @write_attr |= Aerospike::INFO2_GENERATION_GT
      else
       @generation = 0
      end

      if dp.durable_delete
        @write_attr |= Aerospike::INFO2_DURABLE_DELETE
      end

      if dp.commit_level == CommitLevel::COMMIT_MASTER
        @info_attr |= Aerospike::INFO3_COMMIT_MASTER
      end
    end

    private

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
  end
end
