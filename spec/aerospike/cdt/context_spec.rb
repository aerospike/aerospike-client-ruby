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

describe Aerospike::CDT::Context do
  let (:ctx_list) { [Context.map_key("key2"), Context.list_rank(0)] }

  it "should encode to base64 and return the same values" do
    bytes = Context.base64(ctx_list)
    expect(bytes).to eq "lCKlA2tleTIRAA=="

    list = Context.from_base64(bytes)
    expect(list).to eq ctx_list
  end

  it "should handle nil values" do
    expect(Context.bytes(nil)).to eq nil
    expect(Context.bytes([])).to eq nil

    expect(Context.base64(nil)).to eq ""
    expect(Context.base64([])).to eq ""
  end
end
