# encoding: utf-8
# Copyright 2016-2020 Aerospike, Inc.
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

include Aerospike::CDT
include Aerospike::ResultCode

describe "client.operate() - HLL Operations", skip: !Support.min_version?("4.9") do

  let(:client) { Support.client }
  let(:key) { Support.gen_random_key(0, key_val: "ophkey") }
  let(:key0) { Support.gen_random_key(0, key_val: "ophkey0") }
  let(:key1) { Support.gen_random_key(0, key_val: "ophkey1") }
  let(:key2) { Support.gen_random_key(0, key_val: "ophkey2") }
  let(:keys) { [key0, key1, key2] }
  let(:operate_policy) { Aerospike::OperatePolicy.new(record_bin_multiplicity: Aerospike::RecordBinMultiplicity::ARRAY) }

  let(:n_entries) { 1 << 18 }
  let(:min_index_bits) { 4 }
  let(:max_index_bits) { 16 }
  let(:min_minhash_bits) { 4 }
  let(:max_minhash_bits) { 51 }

  let(:hll_bin) { "ophbin" }
  let(:hll_value) { [] }
  let(:hll_policy) { HLLPolicy.new(write_flags: write_flags) }
  let(:entries) { [] }

  legal_index_bits = []
  legal_descriptions = []
  illegal_descriptions = []

  before(:each) do

    legal_zero = []
    legal_min = []
    legal_mid = []
    legal_max = []
    illegal_zero = []
    illegal_min = []
    illegal_mid = []
    illegal_max = []
    illegal_max1 = []

    (0...n_entries).each do |i|
      entries << Aerospike::StringValue.new("key #{i}")
    end

    index_bits = min_index_bits
    begin
      combined_bits = max_minhash_bits + index_bits
      max_allowed_minhash_bits = max_minhash_bits
      max_allowed_minhash_bits -= combined_bits - 64 if combined_bits > 64
      mid_minhash_bits = (max_allowed_minhash_bits + index_bits) / 2

      legal_index_bits << index_bits
      legal_zero << index_bits
      legal_zero << 0

      legal_min << index_bits
      legal_min << min_minhash_bits

      legal_mid << index_bits
      legal_mid << mid_minhash_bits

      legal_max << index_bits
      legal_max << max_allowed_minhash_bits

      legal_descriptions << legal_zero
      legal_descriptions << legal_min
      legal_descriptions << legal_mid
      legal_descriptions << legal_max

      index_bits += 4
    end while index_bits <= max_index_bits

    index_bits = min_index_bits - 1
    begin
      if index_bits < min_index_bits || index_bits > max_index_bits
        illegal_zero << index_bits
        illegal_min << index_bits
        illegal_max << index_bits

        illegal_zero << 0
        illegal_min << (min_minhash_bits-1)
        illegal_max << max_minhash_bits

        illegal_descriptions << illegal_zero
        illegal_descriptions << illegal_min
        illegal_descriptions << illegal_max
      else
        illegal_min << index_bits
        illegal_max << index_bits

        illegal_min << (min_minhash_bits-1)
        illegal_max << (max_minhash_bits+1)

        illegal_descriptions << illegal_min
        illegal_descriptions << illegal_max

        if index_bits+max_minhash_bits > 64
          illegal_max1 << index_bits
          illegal_max1 << (1+max_minhash_bits-(64-(index_bits+max_minhash_bits)))
          illegal_descriptions << illegal_max1
        end
      end
      index_bits += 4
    end while index_bits <= max_index_bits+5
  end

  it "Init should work" do
    client.delete(key)

    legal_descriptions.each do |desc|
      expect_init(desc[0], desc[1], true)
    end

    illegal_descriptions.each do |desc|
      expect_init(desc[0], desc[1], false)
    end
  end

  it "Init HLL Flags should work" do
    index_bits = 4

    # Keep record around win hll_bin is removed.
    expect_success(key,
      [Aerospike::Operation::delete,
      HLLOperation::init("#{hll_bin}other", index_bits, -1)])

    # create_only
    c = HLLPolicy.new(write_flags: HLLWriteFlags::CREATE_ONLY)

    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1, c)])
    expect_errors(key, Aerospike::ResultCode::BIN_EXISTS_ERROR,
      [HLLOperation::init(hll_bin, index_bits, -1, c)])

    # update_only
    u = HLLPolicy.new(write_flags: HLLWriteFlags::UPDATE_ONLY)

    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1, u)])
    expect_success(key, [Aerospike::Operation::put(Aerospike::Bin.new(hll_bin, nil))])
    expect_errors(key, Aerospike::ResultCode::BIN_NOT_FOUND,
      [HLLOperation::init(hll_bin, index_bits, -1, u)])

    # create_only no_fail
    cn = HLLPolicy.new(write_flags: HLLWriteFlags::CREATE_ONLY | HLLWriteFlags::NO_FAIL)

    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1, cn)])
    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1, cn)])

    # update_only no_fail
    un = HLLPolicy.new(write_flags: HLLWriteFlags::UPDATE_ONLY | HLLWriteFlags::NO_FAIL)

    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1, un)])
    expect_success(key, [Aerospike::Operation::put(Aerospike::Bin.new(hll_bin, nil))])
    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1, un)])

    # fold
    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1, c)])

    f = HLLPolicy.new(write_flags: HLLWriteFlags::ALLOW_FOLD)

    expect_errors(key, Aerospike::ResultCode::PARAMETER_ERROR,
      [HLLOperation::init(hll_bin, index_bits, -1, f)])
  end

  it "Bad Init should NOT work" do
    expect_success(key, [Aerospike::Operation::delete, HLLOperation::init(hll_bin, max_index_bits, 0)])
    expect_errors(key, Aerospike::ResultCode::OP_NOT_APPLICABLE,
      [HLLOperation::init(hll_bin, -1, max_minhash_bits)])
  end

  it "Add Init should work" do
    legal_descriptions.each do |desc|
      expect_add_init(desc[0], desc[1])
    end
  end

  it "Add Flags should work" do
    index_bits = 4

    # Keep record around win hll_bin is removed.
    expect_success(key,
      [Aerospike::Operation::delete,
      HLLOperation::init("#{hll_bin}other", index_bits, -1)])

    # create_only
    c = HLLPolicy.new(write_flags: HLLWriteFlags::CREATE_ONLY)

    expect_success(key, [HLLOperation::add(hll_bin, *entries, index_bit_count: index_bits, policy: c)])
    expect_errors(key, Aerospike::ResultCode::BIN_EXISTS_ERROR,
      [HLLOperation::add( hll_bin, entries, index_bits, -1, c)])

    # update_only
    u = HLLPolicy.new(write_flags: HLLWriteFlags::UPDATE_ONLY)

    expect_errors(key, Aerospike::ResultCode::PARAMETER_ERROR,
      [HLLOperation::add(hll_bin, entries, index_bit_count: index_bits, policy: u)])

    # create_only no_fail
    cn = HLLPolicy.new(write_flags: HLLWriteFlags::CREATE_ONLY | HLLWriteFlags::NO_FAIL)

    expect_success(key, [HLLOperation::add(hll_bin, *entries, index_bit_count: index_bits, policy: cn)])
    expect_success(key, [HLLOperation::add(hll_bin, *entries, index_bit_count: index_bits, policy: cn)])

    # fold
    expect_success(key, [HLLOperation::init(hll_bin, index_bits, -1)])

    f = HLLPolicy.new(write_flags: HLLWriteFlags::ALLOW_FOLD)

    expect_errors(key, Aerospike::ResultCode::PARAMETER_ERROR,
      [HLLOperation::add(hll_bin, entries, index_bit_count: index_bits, policy: f)])
  end

  it "Fold should work" do
    vals0 = []
    vals1 = []

    (0...(n_entries/2)).each do |i|
      vals0 << "key #{i}"
    end

    ((n_entries / 2)...n_entries).each do |i|
      vals1 << "key #{i}"
    end

    (4...max_index_bits).each do |index_bits|
      expect_fold(vals0, vals1, index_bits)
    end
  end

  it "Fold Exists should work" do
    index_bits = 10
    fold_down = 4
    fold_up = 16

    # Keep record around win hll_bin is removed.
    expect_success(key,
      [Aerospike::Operation::delete,
      HLLOperation::init("#{hll_bin}other", index_bits, -1),
      HLLOperation::init(hll_bin, index_bits, -1)])

    # Exists.
    expect_success(key, [HLLOperation::fold(hll_bin, fold_down)])
    expect_errors(key, Aerospike::ResultCode::OP_NOT_APPLICABLE,
      [HLLOperation::fold(hll_bin, fold_up)])

    # Does not exist.
    expect_success(key, [Aerospike::Operation::put(Aerospike::Bin.new(hll_bin, nil))])

    expect_errors(key, Aerospike::ResultCode::BIN_NOT_FOUND,
      [HLLOperation::fold(hll_bin, fold_down)])
  end

  it "Set Union should work" do
    vals = []

   (0...keys.length).each do |i|
      sub_vals = []

      (0...n_entries/3).each do |j|
        sub_vals << "key#{i} #{j}"
      end

      vals << sub_vals
    end

    legal_index_bits.each do |index_bits|
      expect_set_union(vals, index_bits, false, false)
      expect_set_union(vals, index_bits, false, true)
      expect_set_union(vals, index_bits, true, false)
      expect_set_union(vals, index_bits, true, true)
    end
  end

  it "Set Union Flags should work" do
    index_bits = 6
    low_n_bits = 4
    high_n_bits = 8
    other_name = "#{hll_bin}o"

    # Keep record around win hll_bin is removed.
    hlls = []
    record = expect_success(key,
      [Aerospike::Operation::delete,
      HLLOperation::add(other_name, *entries, index_bit_count: index_bits),
      Aerospike::Operation::get(other_name)])
    result_list = record.bins[other_name]
    hll = result_list[1]

    hlls << hll

    # create_only
    c = HLLPolicy.new(write_flags: HLLWriteFlags::CREATE_ONLY)

    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: c)])
    expect_errors(key, Aerospike::ResultCode::BIN_EXISTS_ERROR,
      [HLLOperation::set_union(hll_bin, *hlls, c)])

    # update_only
    u = HLLPolicy.new(write_flags: HLLWriteFlags::UPDATE_ONLY)

    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: u)])
    expect_success(key, [Aerospike::Operation::put(Aerospike::Bin.new(hll_bin, nil))])
    expect_errors(key, Aerospike::ResultCode::BIN_NOT_FOUND,
      [HLLOperation::set_union(hll_bin, *hlls, u)])

    # create_only no_fail
    cn = HLLPolicy.new(write_flags: HLLWriteFlags::CREATE_ONLY | HLLWriteFlags::NO_FAIL)

    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: cn)])
    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: cn)])

    # update_only no_fail
    un = HLLPolicy.new(write_flags: HLLWriteFlags::UPDATE_ONLY | HLLWriteFlags::NO_FAIL)

    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: un)])
    expect_success(key, [Aerospike::Operation::put(Aerospike::Bin.new(hll_bin, nil))])
    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: un)])

    # fold
    f = HLLPolicy.new(write_flags: HLLWriteFlags::ALLOW_FOLD)

    # fold down
    expect_success(key, [HLLOperation::init(hll_bin, high_n_bits, -1)])
    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: f)])

    # fold up
    expect_success(key, [HLLOperation::init(hll_bin, low_n_bits, -1)])
    expect_success(key, [HLLOperation::set_union(hll_bin, *hlls, policy: f)])
  end

  it "Refresh Count should work" do
    index_bits = 6

    # Keep record around win hll_bin is removed.
    expect_success(key,
      [Aerospike::Operation::delete,
      HLLOperation::init("#{hll_bin}other", index_bits, -1),
      HLLOperation::init(hll_bin, index_bits, -1)])

    # Exists.
    expect_success(key, [HLLOperation::refresh_count(hll_bin),
            HLLOperation::refresh_count(hll_bin)])
    expect_success(key, [HLLOperation::add(hll_bin, *entries)])
    expect_success(key, [HLLOperation::refresh_count(hll_bin),
            HLLOperation::refresh_count(hll_bin)])

    # Does not exist.
    expect_success(key, [Aerospike::Operation::put(Aerospike::Bin.new(hll_bin, nil))])
    expect_errors(key, Aerospike::ResultCode::BIN_NOT_FOUND,
      [HLLOperation::refresh_count(hll_bin)])
  end

  it "Get Count should work" do
    index_bits = 6

    # Keep record around win hll_bin is removed.
    expect_success(key,
      [Aerospike::Operation::delete,
      HLLOperation::init("#{hll_bin}other", index_bits, -1),
      HLLOperation::add(hll_bin, *entries, index_bit_count: index_bits)])

    # Exists.
    record = expect_success(key, [HLLOperation::get_count(hll_bin)])
    count = record.bins[hll_bin]
    expect_hll_count(index_bits, count, entries.length)

    # Does not exist.
    expect_success(key, [Aerospike::Operation::put(Aerospike::Bin.new(hll_bin, nil))])
    record = expect_success(key, [HLLOperation::get_count(hll_bin)])
    expect(record.bins).to be nil
  end

  it "Get Union should work" do
    index_bits = 14
    expected_union_count = 0
    vals = []
    hlls = []

    (0...keys.length).each do |i|
      sub_vals = []

      (0...(n_entries/3)).each do |j|
        sub_vals << "key#{i} #{j}"
      end

      record = expect_success(keys[i],
        [Aerospike::Operation::delete,
        HLLOperation::add(hll_bin, *sub_vals, index_bit_count: index_bits),
        Aerospike::Operation::get(hll_bin)])

      result_list = record.bins[hll_bin]
      hlls << result_list[1]
      expected_union_count += sub_vals.length
      vals << sub_vals
    end

    # Keep record around win hll_bin is removed.
    expect_success(key,
      [Aerospike::Operation::delete,
      HLLOperation::init("#{hll_bin}other", index_bits, -1),
      HLLOperation::add(hll_bin, *vals[0], index_bit_count: index_bits)])

    record = expect_success(key,
      [HLLOperation::get_union(hll_bin, *hlls),
      HLLOperation::get_union_count(hll_bin, *hlls)])
    result_list = record.bins[hll_bin]
    union_count = result_list[1]

    expect_hll_count(index_bits, union_count, expected_union_count)

    union_hll = result_list[0]

    bin = Aerospike::Bin.new(hll_bin, union_hll)

    record = expect_success(key,
      [Aerospike::Operation::put(bin),
      HLLOperation::get_count(hll_bin)])
    result_list = record.bins[hll_bin]
    union_count_2 = result_list[1]

    expect(union_count).to eq union_count_2
  end

  it "Put should work" do
    legal_descriptions.each do |desc|
      index_bits = desc[0]
      minhash_bits = desc[1]

      expect_success(key,
        [Aerospike::Operation::delete,
                  HLLOperation::init(hll_bin, index_bits, minhash_bits)])

      record = client.get(key)
      hll = record.bins[hll_bin]

      client.delete(key)
      client.put(key, Aerospike::Bin.new(hll_bin, hll))

      record = expect_success(key,
        [HLLOperation::get_count(hll_bin),
                  HLLOperation::describe(hll_bin)])

      result_list = record.bins[hll_bin]
      count = result_list[0]
      description = result_list[1]

      expect(count).to eq 0
      expect_description(description, index_bits, minhash_bits)
    end
  end

  it "Similarity should work" do
    overlaps = [0.0001, 0.001, 0.01, 0.1, 0.5]

    overlaps.each do |overlap|
      expected_intersect_count =(n_entries * overlap).floor
      common = []

      (0...expected_intersect_count).each do |i|
        common << "common#{i}"
      end

      vals = []
      unique_entries_per_node = (n_entries - expected_intersect_count) / 3

      (0...keys.length).each do |i|
        sub_vals = []

        (0...unique_entries_per_node).each do |j|
          sub_vals << "key#{i} #{j}"
        end

        vals << sub_vals
      end

      legal_descriptions.each do |desc|
        index_bits = desc[0]
        minhash_bits = desc[1]

        next if minhash_bits == 0

        expect_similarity(overlap, common, vals, index_bits, minhash_bits)
      end
    end
  end

  it "Empty Similarity should work" do
    legal_descriptions.each do |desc|
      n_index_bits = desc[0]
      n_minhash_bits = desc[1]

      record = expect_success(key,
        [Aerospike::Operation::delete,
        HLLOperation::init(hll_bin, n_index_bits, n_minhash_bits),
        Aerospike::Operation::get(hll_bin)])

      result_list = record.bins[hll_bin]
      hlls = []

      hlls << result_list[1]

      record = expect_success(key,
        [HLLOperation::get_similarity(hll_bin, *hlls),
        HLLOperation::get_intersect_count(hll_bin, *hlls)])

      result_list = record.bins[hll_bin]

      sim = result_list[0]
      intersect_count = result_list[1]

      expect(0).to eq intersect_count
      expect(sim.to_f.nan?).to be true
    end
  end

  it "Intersect should work" do
    other_bin_name = "#{hll_bin}other"

    legal_descriptions.each do |desc|
      index_bits = desc[0]
      minhash_bits = desc[1]

      break if minhash_bits != 0

      record = expect_success(key,
        [Aerospike::Operation::delete,
        HLLOperation::add(hll_bin, *entries, index_bit_count: index_bits, minhash_bit_count: minhash_bits),
        Aerospike::Operation::get(hll_bin),
        HLLOperation::add(other_bin_name, *entries, index_bit_count: index_bits, minhash_bit_count: 4),
        Aerospike::Operation::get(other_bin_name)])

      hlls = []
      hmhs = []
      result_list = record.bins[hll_bin]

      hlls << result_list[1]
      hlls << hlls[0]

      result_list = record.bins[other_bin_name]
      hmhs << result_list[1]
      hmhs << hmhs[0]

      record = expect_success(key,
        [HLLOperation::get_intersect_count(hll_bin, *hlls),
                  HLLOperation::get_similarity(hll_bin, *hlls)])
      result_list = record.bins[hll_bin]

      intersect_count = result_list[0]

      expect(intersect_count < 1.8*entries.length).to be true

      hlls << hlls[0]

      expect_errors(key, Aerospike::ResultCode::PARAMETER_ERROR,
        [HLLOperation::get_intersect_count(hll_bin, *hlls)])
      expect_errors(key, Aerospike::ResultCode::PARAMETER_ERROR,
        [HLLOperation::get_similarity(hll_bin, *hlls)])

      record = expect_success(key,
        [HLLOperation::get_intersect_count(hll_bin, *hmhs),
                  HLLOperation::get_similarity(hll_bin, *hmhs)])
      result_list = record.bins[hll_bin]
      intersect_count = result_list[0]

      expect(intersect_count < 1.8*entries.length).to be true

      hmhs << hmhs[0]

      expect_errors(key, Aerospike::ResultCode::OP_NOT_APPLICABLE,
        [HLLOperation::get_intersect_count(hll_bin, *hmhs)])
      expect_errors(key, Aerospike::ResultCode::OP_NOT_APPLICABLE,
        [HLLOperation::get_similarity(hll_bin, *hmhs)])
    end
  end

  ################################################################################
  #
  # Utility Methods
  #
  ################################################################################

  def hll_post_op
    client.get(key).bins[hll_bin]
  end

  def expect_errors(key, err_code, ops)
    expect {
      client.operate(key, ops, operate_policy)
    }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
      error.result_code == err_code
    }
  end

  def expect_success(key, ops)
      client.operate(key, ops, operate_policy)
  end

  def expect_init(index_bits, minhash_bits, should_pass)
      hll_policy = HLLPolicy::DEFAULT
      ops = [
        HLLOperation::init(hll_bin, index_bits, minhash_bits, hll_policy),
        HLLOperation::get_count(hll_bin),
        HLLOperation::refresh_count(hll_bin),
        HLLOperation::describe(hll_bin),
      ]

      if !should_pass
        expect_errors(key, Aerospike::ResultCode::PARAMETER_ERROR, ops)
        return
      end

      record = expect_success(key, ops)
      result_list = record.bins[hll_bin]

      count = result_list[1]
      count1 = result_list[2]
      description = result_list[3]

      # expect_description(description, index_bits, minhash_bits)
      expect(count).to eq 0
      expect(count1).to eq 0
    end


  def expect_description(description, index_bits, minhash_bits)
    expect(index_bits).to eq description[0]
    expect(minhash_bits).to eq description[1]
  end

  def check_bits(index_bits, minhash_bits)
    return !(index_bits < min_index_bits || index_bits > max_index_bits ||
      (minhash_bits != 0 && minhash_bits < min_minhash_bits) ||
      minhash_bits > max_minhash_bits || index_bits+minhash_bits > 64)
  end

  def relative_count_error(n_index_bits)
    1.04 / Math.sqrt(2 ** n_index_bits)
  end

  def expect_description(description, index_bits, minhash_bits)
    expect(index_bits).to eq description[0]
    expect(minhash_bits).to eq description[1]
  end


  def is_within_relative_error(expected, estimate, relative_error)
    return expected*(1-relative_error) <= estimate || estimate <= expected*(1+relative_error)
  end

  def expect_hll_count(index_bits, hll_count, expected)
    count_err_6sigma = relative_count_error(index_bits) * 6

    expect(is_within_relative_error(expected, hll_count, count_err_6sigma)).to be true
  end

  def expect_add_init(index_bits, minhash_bits)
    client.delete(key)

    ops = [
      HLLOperation::add(hll_bin, *entries, index_bit_count: index_bits, minhash_bit_count: minhash_bits),
      HLLOperation::get_count(hll_bin),
      HLLOperation::refresh_count(hll_bin),
      HLLOperation::describe(hll_bin),
      HLLOperation::add(hll_bin, *entries),
    ]

    if !check_bits(index_bits, minhash_bits)
      expect_errors(key, Aerospike::ResultCode::PARAMETER_ERROR, ops)
      return
    end

    record = expect_success(key, ops)
    result_list = record.bins[hll_bin]
    count = result_list[1]
    count1 = result_list[2]
    description = result_list[3]
    n_added = result_list[4]

    expect_description(description, index_bits, minhash_bits)
    expect_hll_count(index_bits, count, entries.length)
    expect(count).to eq count1
    expect(n_added).to eq 0
  end

  def expect_fold(vals0, vals1, index_bits)

    (min_index_bits..index_bits).each do |ix|
      if !check_bits(index_bits, 0) || !check_bits(ix, 0)
        # Expected valid inputs
        expect(true).to(BeFalse())
      end

      recorda = expect_success(key,
        [Aerospike::Operation::delete,
        HLLOperation::add(hll_bin, *vals0, index_bit_count: index_bits),
        HLLOperation::get_count(hll_bin),
        HLLOperation::refresh_count(hll_bin),
        HLLOperation::describe(hll_bin)])

      resulta_list = recorda.bins[hll_bin]
      counta = resulta_list[1]
      counta1 = resulta_list[2]
      descriptiona = resulta_list[3]

      expect_description(descriptiona, index_bits, 0)
      expect_hll_count(index_bits, counta, vals0.length)
      expect(counta).to eq counta1

      recordb = expect_success(key,
        [HLLOperation::fold(hll_bin, ix),
        HLLOperation::get_count(hll_bin),
        HLLOperation::add(hll_bin, *vals0),
        HLLOperation::add(hll_bin, *vals1),
        HLLOperation::get_count(hll_bin),
        HLLOperation::describe(hll_bin)])

      resultb_list = recordb.bins[hll_bin]
      countb = resultb_list[1]
      n_added0 = resultb_list[2]
      countb1 = resultb_list[4]
      descriptionb = resultb_list[5]

      expect(0).to eq n_added0
      expect_description(descriptionb, ix, 0)
      expect_hll_count(ix, countb, vals0.length)
      expect_hll_count(ix, countb1, vals0.length+vals1.length)
    end
  end

  def expect_set_union(vals, index_bits, folding, allow_folding)
    p = HLLPolicy::DEFAULT
    u = HLLPolicy::DEFAULT

    if allow_folding
      u = HLLPolicy.new(write_flags: HLLWriteFlags::ALLOW_FOLD)
    end

    union_expected = 0
    folded = false

    (0...keys.length).each do |i|
      ix = index_bits

      if folding
        ix -= i

        if ix < min_index_bits
          ix = min_index_bits
        end

        if ix < index_bits
          folded = true
        end
      end

      sub_vals = vals[i]

      union_expected += sub_vals.length

      record = expect_success(keys[i],
        [Aerospike::Operation::delete,
        HLLOperation::add(hll_bin, *sub_vals, index_bit_count: ix),
        HLLOperation::get_count(hll_bin)
      ])
      result_list = record.bins[hll_bin]
      count = result_list[1]

      expect_hll_count(ix, count, sub_vals.length)
    end

    hlls = []

    (0...keys.length).each do |i|
      record = expect_success(keys[i], [Aerospike::Operation::get(hll_bin), HLLOperation::get_count(hll_bin)])
      result_list = record.bins[hll_bin]
      hll = result_list[0]

      expect(hll).not_to be nil
      hlls << hll
    end

    ops = [
      Aerospike::Operation::delete,
      HLLOperation::init(hll_bin, index_bits, -1),
      HLLOperation::set_union(hll_bin, *hlls, policy: u),
      HLLOperation::get_count(hll_bin),
      Aerospike::Operation::delete, # And recreate it to test creating empty.
      HLLOperation::set_union(hll_bin, *hlls),
      HLLOperation::get_count(hll_bin),
    ]

    if folded && !allow_folding
      expect_errors(key, Aerospike::ResultCode::OP_NOT_APPLICABLE, ops)
      return
    end

    record_union = expect_success(key, ops)
    union_result_list = record_union.bins[hll_bin]
    union_count = union_result_list[2]
    union_count2 = union_result_list[4]

    expect_hll_count(index_bits, union_count, union_expected)
    expect(union_count).to eq union_count2

    (0...keys.length).each do |i|
      sub_vals = vals[i]
      record = expect_success(key,
        [HLLOperation::add(hll_bin, *sub_vals, index_bit_count: index_bits),
        HLLOperation::get_count(hll_bin)])
      result_list = record.bins[hll_bin]
      n_added = result_list[0]
      count = result_list[1]

      expect(0).to eq n_added
      expect(union_count).to eq count
      expect_hll_count(index_bits, count, union_expected)
    end
  end

  def absolute_similarity_error(index_bits, minhash_bits, expected_similarity)
    min_err_index = 1 / Math.sqrt(1<<index_bits)
    min_err_minhash = 6 * ((Math::E ** minhash_bits)*-1) / expected_similarity

    [min_err_index, min_err_minhash].max
  end

  def expect_hmh_similarity(index_bits, minhash_bits, similarity,
      expected_similarity, intersect_count, expected_intersect_count)
    sim_err_6sigma = 0.0

    if minhash_bits != 0
      sim_err_6sigma = 6 * absolute_similarity_error(index_bits, minhash_bits, expected_similarity)
    end

    if minhash_bits == 0
      return
    end

    expect(sim_err_6sigma > (expected_similarity-similarity).abs).to be true
    expect(is_within_relative_error(expected_intersect_count, intersect_count, sim_err_6sigma)).to be true
  end

  def expect_similarity(overlap, common, vals, index_bits, minhash_bits)
    hlls = []

    (0...keys.length).each do |i|
      record = expect_success(keys[i],
        [Aerospike::Operation::delete,
        HLLOperation::add(hll_bin, *vals[i], index_bit_count: index_bits, minhash_bit_count: minhash_bits),
        HLLOperation::add(hll_bin, *common, index_bit_count: index_bits, minhash_bit_count: minhash_bits),
        Aerospike::Operation::get(hll_bin)])

      result_list = record.bins[hll_bin]
      hlls << result_list[2]
    end

    # Keep record around win hll_bin is removed.
    record = expect_success(key,
      [Aerospike::Operation::delete,
            HLLOperation::init("#{hll_bin}other", index_bits, minhash_bits),
            HLLOperation::set_union(hll_bin, *hlls),
            HLLOperation::describe(hll_bin)])
    result_list = record.bins[hll_bin]
    description = result_list[1]

    expect_description(description, index_bits, minhash_bits)

    record = expect_success(key,
      [HLLOperation::get_similarity(hll_bin, *hlls),
            HLLOperation::get_intersect_count(hll_bin, *hlls)])
    result_list = record.bins[hll_bin]
    sim = result_list[0]
    intersect_count = result_list[1]
    expected_similarity = overlap
    expected_intersect_count = common.length

    expect_hmh_similarity(index_bits, minhash_bits, sim, expected_similarity, intersect_count,
      expected_intersect_count)
  end

end
