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

describe "client.operate() - CDT Bitwise Operations", skip: !Support.feature?(Aerospike::Features::BLOB_BITS) do

  let(:client) { Support.client }
  let(:key) { Support.gen_random_key(5, ns: "test", set: "test", key_val: "key_val") }
  let(:bin_name) { Support.rand_string(10) }
  let(:bit_policy) { BitPolicy::DEFAULT }
  let(:operate_policy) { Aerospike::OperatePolicy.new(record_bin_multiplicity: Aerospike::RecordBinMultiplicity::ARRAY) }

  before(:each) do
    bin_name = Support.rand_string(10)
  end

  def binary_string(len, val = 0)
    initial = ''.force_encoding('binary')
    len.times do
      initial << val
    end
    initial
  end

  def assert_equals(e, expected, actual)
    expect(actual).to eq(expected), e
  end

  def assert_bit_modify_region(bin_sz, offset, set_sz, expected, is_insert, *ops)
    client.delete(key)

    initial = binary_string(bin_sz, 0xff)

    # puts initial.bytes
    client.put(key, Aerospike::Bin.new(bin_name, Aerospike::BytesValue.new(initial)))

    int_sz = 64
    int_sz = set_sz if set_sz < int_sz

    bin_bit_sz = bin_sz * 8
    bin_bit_sz += set_sz if is_insert 

    full_ops = ops.dup
    full_ops << BitOperation.lscan(bin_name, offset, set_sz, true)
    full_ops << BitOperation.rscan(bin_name, offset, set_sz, true)
    full_ops << BitOperation.get_int(bin_name, offset, int_sz, false)
    full_ops << BitOperation.count(bin_name, offset, set_sz)
    full_ops << BitOperation.lscan(bin_name, 0, bin_bit_sz, false)
    full_ops << BitOperation.rscan(bin_name, 0, bin_bit_sz, false)
    full_ops << BitOperation.get(bin_name, offset, set_sz)

    record = client.operate(key, full_ops, operate_policy)

    # puts "RECORD IS: #{record}"
    result_list = record.bins[bin_name]
    lscan1_result = result_list[-7]
    rscan1_result = result_list[-6]
    getint_result = result_list[-5]
    count_result = result_list[-4]
    lscan_result = result_list[-3]
    rscan_result = result_list[-2]
    actual = result_list[-1]

    err_output = "bin_sz #{bin_sz} offset #{offset} set_sz #{set_sz}"
    assert_equals("lscan1 - #{err_output}", -1, lscan1_result)
    assert_equals("rscan1 - #{err_output}", -1, rscan1_result)
    assert_equals("getint - #{err_output}", 0, getint_result)
    assert_equals("count - #{err_output}", 0, count_result)
    assert_equals("lscan - #{err_output}", offset, lscan_result)
    assert_equals("rscan - #{err_output}", offset+set_sz-1, rscan_result)
    assert_equals("op - #{err_output}", expected, actual)
  end

  def assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, expected, *ops)
    assert_bit_modify_region(bin_sz, offset, set_sz, expected, false, *ops)
  end

  def assert_bit_modify_insert(bin_sz, offset, set_sz, expected, *ops)
    assert_bit_modify_region(bin_sz, offset, set_sz, expected, true, *ops)
  end

  def assert_bit_read_operation(initial, expected, *ops)
    client.delete(key)

    err = client.put(key, Aerospike::Bin.new(bin_name, Aerospike::BytesValue.new(initial.pack('C*'))))

    rec = client.operate(key, ops, operate_policy)
    bin_results = rec.bins[bin_name]
    results = []
    bin_results.each do |br|
      results << br
    end

    expect(results).to eq expected
  end

  def assert_bit_modify_operations(initial, expected, *ops)
    client.delete(key)

    client.put(key, Aerospike::Bin.new(bin_name, Aerospike::BytesValue.new(initial.pack('C*')))) if initial

    client.operate(key, ops)

    rec = client.get(key)
    expect(rec.bins[bin_name].bytes).to eq expected
  end

  def assert_throws(code, *ops)
    expect {
      client.operate(key, ops)
    }.to raise_error (Aerospike::Exceptions::Aerospike){ |error|
      error.result_code == code
    }
  end


  it "should Set a Bin" do

    bit0 = [0x80]
    put_mode = BitPolicy::DEFAULT
    update_mode = BitPolicy.new(BitWriteFlags::UPDATE_ONLY)

    assert_bit_modify_operations(
      [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
      [0x51, 0x02, 0x03, 0x04, 0x05, 0x06],
      BitOperation.set(bin_name, 1, 1, bit0),
      BitOperation.set(bin_name, 3, 1, bit0),
      BitOperation.remove(bin_name, 6, 2, policy: update_mode)
    )

    add_mode = BitPolicy.new(BitWriteFlags::CREATE_ONLY)
    bytes1 = [0x0A]

    assert_bit_modify_operations(
      nil, [0x00, 0x0A],
      BitOperation.insert(bin_name, 1, bytes1, policy: add_mode)
    )

    assert_throws(17,
      BitOperation.set("b", 1, 1, bit0, policy: put_mode))

    assert_throws(4,
      BitOperation.set(bin_name, 1, 1, bit0, policy: add_mode))
  end

  it "should Set a Bin's bits" do

    put_mode = BitPolicy::DEFAULT
    bit0 = [0x80]
    bits1 = [0x11, 0x22, 0x33]

    assert_bit_modify_operations(
      [0x01, 0x12, 0x02, 0x03, 0x04, 0x05, 0x06,
        0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
        0x0E, 0x0F, 0x10, 0x11, 0x41],
      [0x41,
        0x13,
        0x11, 0x22, 0x33,
        0x11, 0x22, 0x33,
        0x08,
        0x08, 0x91, 0x1B,
        0X01, 0x12, 0x23,
        0x11, 0x22, 0x11,
        0xc1],
      BitOperation.set(bin_name, 1, 1, bit0, policy: put_mode),
      BitOperation.set(bin_name, 15, 1, bit0, policy: put_mode),
      # SUM Offest Size
      BitOperation.set(bin_name, 16, 24, bits1, policy: put_mode),  #  Y    Y      Y
      BitOperation.set(bin_name, 40, 22, bits1, policy: put_mode),  #  N    Y      N
      BitOperation.set(bin_name, 73, 21, bits1, policy: put_mode),  #  N    N      N
      BitOperation.set(bin_name, 100, 20, bits1, policy: put_mode), #  Y    N      N
      BitOperation.set(bin_name, 120, 17, bits1, policy: put_mode), #  N    Y      N

      BitOperation.set(bin_name, 144, 1, bit0, policy: put_mode),
    )
  end

  it "should LSHIFT bits" do

    assert_bit_modify_operations(
      [0x01, 0x01, 0x00, 0x80,
        0xFF, 0x01, 0x01,
        0x18, 0x01],
      [0x02, 0x40, 0x01, 0x00,
        0xF8, 0x08, 0x01,
        0x28, 0x01],
      BitOperation.lshift(bin_name, 0, 8, 1),
      BitOperation.lshift(bin_name, 9, 7, 6),
      BitOperation.lshift(bin_name, 23, 2, 1),

      BitOperation.lshift(bin_name, 37, 18, 3),

      BitOperation.lshift(bin_name, 58, 2, 1),
      BitOperation.lshift(bin_name, 64, 4, 7),
    )

    assert_bit_modify_operations(
      [0xFF, 0xFF, 0xFF],
      [0xF8, 0x00, 0x0F],
      BitOperation.lshift(bin_name, 0, 20, 15),
    )
  end

  it "should RSHIFT bits" do

    assert_bit_modify_operations(
      [0x80, 0x40, 0x01, 0x00,
        0xFF, 0x01, 0x01,
        0x18, 0x80],
      [0x40, 0x01, 0x00, 0x80,
        0xF8, 0xE0, 0x21,
        0x14, 0x80],
      BitOperation.rshift(bin_name, 0, 8, 1),
      BitOperation.rshift(bin_name, 9, 7, 6),
      BitOperation.rshift(bin_name, 23, 2, 1),

      BitOperation.rshift(bin_name, 37, 18, 3),

      BitOperation.rshift(bin_name, 60, 2, 1),
      BitOperation.rshift(bin_name, 68, 4, 7),
    )
  end

  it "should OR bits" do

    bits1 = [0x11, 0x22, 0x33]
    put_mode = BitPolicy::DEFAULT

    assert_bit_modify_operations(
      [0x80, 0x40, 0x01, 0x00, 0x00,
        0x01, 0x02, 0x03],
      [0x90, 0x48, 0x01, 0x20, 0x11,
        0x11, 0x22, 0x33],
      BitOperation.or(bin_name, 0, 5, bits1, policy: put_mode),
      BitOperation.or(bin_name, 9, 7, bits1, policy: put_mode),
      BitOperation.or(bin_name, 23, 6, bits1, policy: put_mode),
      BitOperation.or(bin_name, 32, 8, bits1, policy: put_mode),

      BitOperation.or(bin_name, 40, 24, bits1, policy: put_mode),
    )
  end

  it "should XOR bits" do

    bits1 = [0x11, 0x22, 0x33]
    put_mode = BitPolicy::DEFAULT

    assert_bit_modify_operations(
      [0x80, 0x40, 0x01, 0x00, 0x00,
        0x01, 0x02, 0x03],
      [0x90, 0x48, 0x01, 0x20, 0x11, 0x10, 0x20,
        0x30],
      BitOperation.xor(bin_name, 0, 5, bits1, policy: put_mode),
      BitOperation.xor(bin_name, 9, 7, bits1, policy: put_mode),
      BitOperation.xor(bin_name, 23, 6, bits1, policy: put_mode),
      BitOperation.xor(bin_name, 32, 8, bits1, policy: put_mode),

      BitOperation.xor(bin_name, 40, 24, bits1, policy: put_mode),
    )
  end

  it "should AND bits" do

    bits1 = [0x11, 0x22, 0x33]
    put_mode = BitPolicy::DEFAULT

    assert_bit_modify_operations(
      [0x80, 0x40, 0x01, 0x00, 0x00,
        0x01, 0x02, 0x03],
      [0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03],
      BitOperation.and(bin_name, 0, 5, bits1, policy: put_mode),
      BitOperation.and(bin_name, 9, 7, bits1, policy: put_mode),
      BitOperation.and(bin_name, 23, 6, bits1, policy: put_mode),
      BitOperation.and(bin_name, 32, 8, bits1, policy: put_mode),

      BitOperation.and(bin_name, 40, 24, bits1, policy: put_mode),
    )
  end

  it "should NOT bits" do

    put_mode = BitPolicy::DEFAULT

    assert_bit_modify_operations(
      [0x80, 0x40, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03],
      [0x78, 0x3F, 0x00, 0xF8, 0xFF, 0xFE, 0xFD, 0xFC],
      BitOperation.not(bin_name, 0, 5, policy: put_mode),
      BitOperation.not(bin_name, 9, 7, policy: put_mode),
      BitOperation.not(bin_name, 23, 6, policy: put_mode),
      BitOperation.not(bin_name, 32, 8, policy: put_mode),

      BitOperation.not(bin_name, 40, 24, policy: put_mode),
    )
  end

  it "should ADD bits" do

    put_mode = BitPolicy::DEFAULT

    assert_bit_modify_operations(
      [0x38, 0x1F, 0x00, 0xE8, 0x7F,
        0x00, 0x00, 0x00,
        0x01, 0x01, 0x01,
        0x01, 0x01, 0x01,
        0x02, 0x02, 0x02,
        0x03, 0x03, 0x03],
      [0x40, 0x20, 0x01, 0xF0, 0x80,
        0x7F, 0x7F, 0x7F,
        0x02, 0x02, 0x01,
        0x02, 0x02, 0x02,
        0x03, 0x03, 0x06,
        0x07, 0x07, 0x07],
      BitOperation.add(bin_name, 0, 5, 1, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 9, 7, 1, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 23, 6, 0x21, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 32, 8, 1, false, BitOverflowAction::FAIL, policy: put_mode),

      BitOperation.add(bin_name, 40, 24, 0x7F7F7F, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 64, 20, 0x01010, false, BitOverflowAction::FAIL, policy: put_mode),

      BitOperation.add(bin_name, 92, 20, 0x10101, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 113, 22, 0x8082, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 136, 23, 0x20202, false, BitOverflowAction::FAIL, policy: put_mode),
    )

    initial = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    i = 0

    assert_bit_modify_operations(
      initial,
      [0xFE, 0xFE, 0x7F, 0xFF, 0x7F, 0x80],
      BitOperation.add(bin_name, 8*i, 8, 0xFF, false, BitOverflowAction::WRAP, policy: put_mode),
      BitOperation.add(bin_name, 8*i, 8, 0xFF, false, BitOverflowAction::WRAP, policy: put_mode),

      BitOperation.add(bin_name, 8*(i+1), 8, 0x7F, true, BitOverflowAction::WRAP, policy: put_mode),
      BitOperation.add(bin_name, 8*(i+1), 8, 0x7F, true, BitOverflowAction::WRAP, policy: put_mode),

      BitOperation.add(bin_name, 8*(i+2), 8, 0x80, true, BitOverflowAction::WRAP, policy: put_mode),
      BitOperation.add(bin_name, 8*(i+2), 8, 0xFF, true, BitOverflowAction::WRAP, policy: put_mode),

      BitOperation.add(bin_name, 8*(i+3), 8, 0x80, false, BitOverflowAction::SATURATE, policy: put_mode),
      BitOperation.add(bin_name, 8*(i+3), 8, 0x80, false, BitOverflowAction::SATURATE, policy: put_mode),

      BitOperation.add(bin_name, 8*(i+4), 8, 0x77, true, BitOverflowAction::SATURATE, policy: put_mode),
      BitOperation.add(bin_name, 8*(i+4), 8, 0x77, true, BitOverflowAction::SATURATE, policy: put_mode),

      BitOperation.add(bin_name, 8*(i+5), 8, 0x8F, true, BitOverflowAction::SATURATE, policy: put_mode),
      BitOperation.add(bin_name, 8*(i+5), 8, 0x8F, true, BitOverflowAction::SATURATE, policy: put_mode),
    )

    client.put(key, Aerospike::Bin.new(bin_name, initial))

    assert_throws(26,
      BitOperation.add(bin_name, 0, 8, 0xFF, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 0, 8, 0xFF, false, BitOverflowAction::FAIL, policy: put_mode),
    )

    assert_throws(26,
      BitOperation.add(bin_name, 0, 8, 0x7F, true, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 0, 8, 0x02, true, BitOverflowAction::FAIL, policy: put_mode),
    )

    assert_throws(26,
      BitOperation.add(bin_name, 0, 8, 0x81, true, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.add(bin_name, 0, 8, 0xFE, true, BitOverflowAction::FAIL, policy: put_mode),
    )
  end

  it "should SUB bits" do

    put_mode = BitPolicy::DEFAULT

    assert_bit_modify_operations(
      [0x38, 0x1F, 0x00, 0xE8, 0x7F,

        0x80, 0x80, 0x80,
        0x01, 0x01, 0x01,

        0x01, 0x01, 0x01,
        0x02, 0x02, 0x02,
        0x03, 0x03, 0x03],
      [0x30, 0x1E, 0x00, 0xD0, 0x7E,

        0x7F, 0x7F, 0x7F,
        0x00, 0xF0, 0xF1,

        0x00, 0x00, 0x00,
        0x01, 0xFD, 0xFE,
        0x00, 0xE0, 0xE1],
      BitOperation.subtract(bin_name, 0, 5, 0x01, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 9, 7, 0x01, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 23, 6, 0x03, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 32, 8, 0x01, false, BitOverflowAction::FAIL, policy: put_mode),

      BitOperation.subtract(bin_name, 40, 24, 0x10101, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 64, 20, 0x101, false, BitOverflowAction::FAIL, policy: put_mode),

      BitOperation.subtract(bin_name, 92, 20, 0x10101, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 113, 21, 0x101, false, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 136, 23, 0x11111, false, BitOverflowAction::FAIL, policy: put_mode),
    )

    initial = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    i = 0

    assert_bit_modify_operations(
      initial,
      [0xFF, 0xF6, 0x7F, 0x00, 0x80, 0x7F],
      BitOperation.subtract(bin_name, 8*i, 8, 0x01, false, BitOverflowAction::WRAP, policy: put_mode),

      BitOperation.subtract(bin_name, 8*(i+1), 8, 0x80, true, BitOverflowAction::WRAP, policy: put_mode),
      BitOperation.subtract(bin_name, 8*(i+1), 8, 0x8A, true, BitOverflowAction::WRAP, policy: put_mode),

      BitOperation.subtract(bin_name, 8*(i+2), 8, 0x7F, true, BitOverflowAction::WRAP, policy: put_mode),
      BitOperation.subtract(bin_name, 8*(i+2), 8, 0x02, true, BitOverflowAction::WRAP, policy: put_mode),

      BitOperation.subtract(bin_name, 8*(i+3), 8, 0xAA, false, BitOverflowAction::SATURATE, policy: put_mode),

      BitOperation.subtract(bin_name, 8*(i+4), 8, 0x77, true, BitOverflowAction::SATURATE, policy: put_mode),
      BitOperation.subtract(bin_name, 8*(i+4), 8, 0x77, true, BitOverflowAction::SATURATE, policy: put_mode),

      BitOperation.subtract(bin_name, 8*(i+5), 8, 0x81, true, BitOverflowAction::SATURATE, policy: put_mode),
      BitOperation.subtract(bin_name, 8*(i+5), 8, 0x8F, true, BitOverflowAction::SATURATE, policy: put_mode),
    )

    client.put(key, Aerospike::Bin.new(bin_name, initial))

    assert_throws(26,
      BitOperation.subtract(bin_name, 0, 8, 1, false, BitOverflowAction::FAIL, policy: put_mode),
    )

    assert_throws(26,
      BitOperation.subtract(bin_name, 0, 8, 0x7F, true, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 0, 8, 0x02, true, BitOverflowAction::FAIL, policy: put_mode),
    )

    assert_throws(26,
      BitOperation.subtract(bin_name, 0, 8, 0x81, true, BitOverflowAction::FAIL, policy: put_mode),
      BitOperation.subtract(bin_name, 0, 8, 0xFE, true, BitOverflowAction::FAIL, policy: put_mode),
    )
  end

  it "should SetInt bits" do

    put_mode = BitPolicy::DEFAULT

    assert_bit_modify_operations(
      [0x38, 0x1F, 0x00, 0xE8, 0x7F,

        0x80, 0x80, 0x80,
        0x01, 0x01, 0x01,

        0x01, 0x01, 0x01,
        0x02, 0x02, 0x02,
        0x03, 0x03, 0x03],
      [0x08, 0x01, 0x00, 0x18, 0x01,

        0x01, 0x01, 0x01,
        0x00, 0x10, 0x11,

        0x01, 0x01, 0x01,
        0x00, 0x04, 0x06,
        0x02, 0x22, 0x23],
      BitOperation.set_int(bin_name, 0, 5, 0x01, policy: put_mode),
      BitOperation.set_int(bin_name, 9, 7, 0x01, policy: put_mode),
      BitOperation.set_int(bin_name, 23, 6, 0x03, policy: put_mode),
      BitOperation.set_int(bin_name, 32, 8, 0x01, policy: put_mode),

      BitOperation.set_int(bin_name, 40, 24, 0x10101, policy: put_mode),
      BitOperation.set_int(bin_name, 64, 20, 0x101, policy: put_mode),

      BitOperation.set_int(bin_name, 92, 20, 0x10101, policy: put_mode),
      BitOperation.set_int(bin_name, 113, 21, 0x101, policy: put_mode),
      BitOperation.set_int(bin_name, 136, 23, 0x11111, policy: put_mode),
    )
  end

  it "should Get bits" do

    client.delete(key)

    bytes = Aerospike::BytesValue.new([0xC1, 0xAA, 0xAA].pack('C*'))
    client.put(key, Aerospike::Bin.new(bin_name, bytes))

    record = client.operate(key,
      [BitOperation.get(bin_name, 0, 1),
      BitOperation.get(bin_name, 1, 1),
      BitOperation.get(bin_name, 7, 1),
      BitOperation.get(bin_name, 0, 8),

      BitOperation.get(bin_name, 8, 16),
      BitOperation.get(bin_name, 9, 15),
      BitOperation.get(bin_name, 9, 14)],
      operate_policy
    )
    expect(record).not_to be nil

    expected = [
      [0x80],
      [0x80],
      [0x80],
      [0xC1],

      [0xAA, 0xAA],
      [0x55, 0x54],
      [0x55, 0x54],
    ]

    results = record.bins[bin_name].map{ |elem| elem.bytes}
    expect(results).to eq expected
  end

  it "should Count bits" do

    assert_bit_read_operation(
      [0xC1, 0xAA, 0xAB],
      [1, 1, 1, 3, 9, 8, 7],
      BitOperation.count(bin_name, 0, 1),
      BitOperation.count(bin_name, 1, 1),
      BitOperation.count(bin_name, 7, 1),
      BitOperation.count(bin_name, 0, 8),

      BitOperation.count(bin_name, 8, 16),
      BitOperation.count(bin_name, 9, 15),
      BitOperation.count(bin_name, 9, 14),
    )
  end

  it "should LSCAN bits" do

    assert_bit_read_operation(
      [0xFF, 0xFF, 0xFF,
        0xFF, 0x00, 0x00, 0x00, 0x00, 0x01],
      [0, 0, 0,
        0, -1, -1,
        39, -1, 0, 0,
        0, 32,
        0, -1],
      BitOperation.lscan(bin_name, 0, 1, true),
      BitOperation.lscan(bin_name, 0, 8, true),
      BitOperation.lscan(bin_name, 0, 9, true),

      BitOperation.lscan(bin_name, 0, 32, true),
      BitOperation.lscan(bin_name, 0, 32, false),
      BitOperation.lscan(bin_name, 1, 30, false),

      BitOperation.lscan(bin_name, 32, 40, true),
      BitOperation.lscan(bin_name, 33, 38, true),
      BitOperation.lscan(bin_name, 32, 40, false),
      BitOperation.lscan(bin_name, 33, 38, false),

      BitOperation.lscan(bin_name, 0, 72, true),
      BitOperation.lscan(bin_name, 0, 72, false),

      BitOperation.lscan(bin_name, -1, 1, true),
      BitOperation.lscan(bin_name, -1, 1, false),
    )
  end

  it "should RSCAN bits" do

    assert_bit_read_operation(
      [0xFF, 0xFF, 0xFF, 0xFF,
        0x00, 0x00, 0x00, 0x00, 0x01],
      [0, 7, 8,
        31, -1, -1,
        39, -1, 38, 37,
        71, 70,
        0, -1],
      BitOperation.rscan(bin_name, 0, 1, true),
      BitOperation.rscan(bin_name, 0, 8, true),
      BitOperation.rscan(bin_name, 0, 9, true),

      BitOperation.rscan(bin_name, 0, 32, true),
      BitOperation.rscan(bin_name, 0, 32, false),
      BitOperation.rscan(bin_name, 1, 30, false),

      BitOperation.rscan(bin_name, 32, 40, true),
      BitOperation.rscan(bin_name, 33, 38, true),
      BitOperation.rscan(bin_name, 32, 40, false),
      BitOperation.rscan(bin_name, 33, 38, false),

      BitOperation.rscan(bin_name, 0, 72, true),
      BitOperation.rscan(bin_name, 0, 72, false),

      BitOperation.rscan(bin_name, -1, 1, true),
      BitOperation.rscan(bin_name, -1, 1, false),
    )
  end

  it "should GetInt bits" do

    assert_bit_read_operation(
      [0x0F, 0x0F, 0x00],
      [15, -1,
        15, 15,
        8, -8,
        3840, 3840,
        3840, 3840,
        1920, 1920,
        115648, -15424,
        15, -1],
      BitOperation.get_int(bin_name, 4, 4, false),
      BitOperation.get_int(bin_name, 4, 4, true),

      BitOperation.get_int(bin_name, 0, 8, false),
      BitOperation.get_int(bin_name, 0, 8, true),

      BitOperation.get_int(bin_name, 7, 4, false),
      BitOperation.get_int(bin_name, 7, 4, true),

      BitOperation.get_int(bin_name, 8, 16, false),
      BitOperation.get_int(bin_name, 8, 16, true),

      BitOperation.get_int(bin_name, 9, 15, false),
      BitOperation.get_int(bin_name, 9, 15, true),

      BitOperation.get_int(bin_name, 9, 14, false),
      BitOperation.get_int(bin_name, 9, 14, true),

      BitOperation.get_int(bin_name, 5, 17, false),
      BitOperation.get_int(bin_name, 5, 17, true),

      BitOperation.get_int(bin_name, -12, 4, false),
      BitOperation.get_int(bin_name, -12, 4, true),
    )
  end

  it "should BitSetEx bits" do

    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    (1..80).each do |set_sz|
      set_data = binary_string((set_sz+7)/8)

      (0..(bin_bit_sz - set_sz)).each do |offset|
        assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data, BitOperation.set(bin_name, offset, set_sz, set_data))
      end
    end
  end

  it "should LSHIFT Ex bits" do

    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    (1..80).each do |set_sz|
      set_data = binary_string((set_sz+7)/8)

      (0..(bin_bit_sz - set_sz)).each do |offset|
        limit = 16
        limit = set_sz + 1 if set_sz < 16 

        (0..limit).each do |n_bits|
          assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data,
            BitOperation.set(bin_name, offset, set_sz, set_data),
            BitOperation.lshift(bin_name, offset, set_sz, n_bits))
        end

        (63..set_sz).each do |n_bits|
          assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data,
            BitOperation.set(bin_name, offset, set_sz,
              set_data),
            BitOperation.lshift(bin_name, offset, set_sz,
              n_bits))
        end
      end
    end
  end

  it "should RSHIFT Ex bits" do
    partial_policy = BitPolicy.new(BitWriteFlags::PARTIAL)
    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    (1..80).each do |set_sz|
      set_data = binary_string((set_sz+7)/8)

      (0..(bin_bit_sz - set_sz)).each do |offset|
        limit = 16
        limit = set_sz + 1 if set_sz < 16
          
        (0..limit).each do |n_bits|
          assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data,
            BitOperation.set(bin_name, offset, set_sz,
              set_data),
            BitOperation.rshift(bin_name, offset, set_sz,
              n_bits))
        end

        (63..set_sz).each do |n_bits|
          assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data,
            BitOperation.set(bin_name, offset, set_sz,
              set_data),
            BitOperation.rshift(bin_name, offset, set_sz,
              n_bits))
        end
      end

      # Test Partial
      n_bits = 1

      ((bin_bit_sz - set_sz + 1)...bin_bit_sz).each do |offset|
        actual_set_sz = bin_bit_sz - offset
        actual_set_data = binary_string((actual_set_sz+7)/8)

        assert_bit_modify_region_not_insert(bin_sz, offset, actual_set_sz,
          actual_set_data,
          BitOperation.set(bin_name, offset, set_sz,
            set_data, policy: partial_policy),
          BitOperation.rshift(bin_name, offset, set_sz,
            n_bits, policy: partial_policy))
      end
    end
  end

  it "should AND Ex bits" do

    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    (1..80).each do |set_sz|
      set_data = binary_string((set_sz+7)/8)

      (0..(bin_bit_sz - set_sz)).each do |offset|
        assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data,
          BitOperation.and(bin_name, offset, set_sz,
            set_data))
      end
    end
  end

  it "should NOT Ex bits" do

    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    (1..80).each do |set_sz|
      set_data = binary_string((set_sz+7)/8)

      (0..(bin_bit_sz - set_sz)).each do |offset|
        assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data,
          BitOperation.not(bin_name, offset, set_sz))
      end
    end
  end

  it "should INSERT Ex bits" do

    bin_sz = 15

    (1..10).each do |set_sz|
      set_data = binary_string(set_sz)

      (0..set_sz).each do |offset|
        assert_bit_modify_insert(bin_sz, offset*8, set_sz*8, set_data,
          BitOperation.insert(bin_name, offset, set_data))
      end
    end
  end

  it "should ADD Ex bits" do

    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    (1..64).each do |set_sz|
      set_data = binary_string((set_sz+7)/8)

      (0..(bin_bit_sz - set_sz)).each do |offset|
        assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, set_data,
          BitOperation.add(bin_name, offset, set_sz, 1,
            false, BitOverflowAction::WRAP))
      end
    end
  end

  it "should SUB Ex bits" do

    bin_sz = 15
    bin_bit_sz = bin_sz * 8

    (1..64).each do |set_sz|
      expected = binary_string((set_sz+7)/8)
      value = Integer(0xFFFFffffFFFFffff >> (64-set_sz))

      (0..(bin_bit_sz - set_sz)).each do |offset|
        assert_bit_modify_region_not_insert(bin_sz, offset, set_sz, expected,
          BitOperation.subtract(bin_name, offset, set_sz,
            value, false, BitOverflowAction::WRAP))
      end
    end
  end

  it "should LSHIFT bits" do

    initial = []
    buf = [0x80]

    client.delete(key)
    client.put(key, Aerospike::Bin.new(bin_name, Aerospike::BytesValue.new(initial.pack('C*'))))

    assert_throws(26,
      BitOperation.set(bin_name, 0, 1, buf))
    assert_throws(26,
      BitOperation.or(bin_name, 0, 1, buf))
    assert_throws(26,
      BitOperation.xor(bin_name, 0, 1, buf))
    assert_throws(26,
      BitOperation.and(bin_name, 0, 1, buf))
    assert_throws(26,
      BitOperation.not(bin_name, 0, 1))
    assert_throws(26,
      BitOperation.lshift(bin_name, 0, 1, 1))
    assert_throws(26,
      BitOperation.rshift(bin_name, 0, 1, 1))
    # OK for insert.
    assert_throws(4,
      BitOperation.remove(bin_name, 0, 1))
    assert_throws(26,
      BitOperation.add(bin_name, 0, 1, 1, false, BitOverflowAction::FAIL))
    assert_throws(26,
      BitOperation.subtract(bin_name, 0, 1, 1, false, BitOverflowAction::FAIL))
    assert_throws(26,
      BitOperation.set_int(bin_name, 0, 1, 1))

    assert_throws(26,
      BitOperation.get(bin_name, 0, 1))
    assert_throws(26,
      BitOperation.count(bin_name, 0, 1))
    assert_throws(26,
      BitOperation.lscan(bin_name, 0, 1, true))
    assert_throws(26,
      BitOperation.rscan(bin_name, 0, 1, true))
    assert_throws(26,
      BitOperation.get_int(bin_name, 0, 1, false))
  end

  it "should Resize bits" do

    client.delete(key)

    no_fail = BitPolicy.new(BitWriteFlags::NO_FAIL)
    record = client.operate(key,
      [BitOperation.resize(bin_name, 20, BitResizeFlags::DEFAULT),
      BitOperation.get(bin_name, 19*8, 8),
      BitOperation.resize(bin_name, 10, BitResizeFlags::GROW_ONLY, policy: no_fail),
      BitOperation.get(bin_name, 19*8, 8),
      BitOperation.resize(bin_name, 10, BitResizeFlags::SHRINK_ONLY),
      BitOperation.get(bin_name, 9*8, 8),
      BitOperation.resize(bin_name, 30, BitResizeFlags::SHRINK_ONLY, policy: no_fail),
      BitOperation.get(bin_name, 9*8, 8),
      BitOperation.resize(bin_name, 19, BitResizeFlags::GROW_ONLY),
      BitOperation.get(bin_name, 18*8, 8),
      BitOperation.resize(bin_name, 0, BitResizeFlags::GROW_ONLY, policy: no_fail),
      BitOperation.resize(bin_name, 0, BitResizeFlags::SHRINK_ONLY)],
      operate_policy
    )

    result_list = record.bins[bin_name]
    get0 = result_list[1].bytes
    get1 = result_list[3].bytes
    get2 = result_list[5].bytes
    get3 = result_list[7].bytes
    get4 = result_list[9].bytes

    expect([0x00]).to eq get0
    expect([0x00]).to eq get1
    expect([0x00]).to eq get2
    expect([0x00]).to eq get3
    expect([0x00]).to eq get4
  end

end
