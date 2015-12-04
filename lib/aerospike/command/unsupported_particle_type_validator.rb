# encoding: utf-8
# Copyright 2015 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Aerospike

  class UnsupportedParticleTypeValidator

    def initialize(*particle_types)
      @unsupported_types = particle_types.to_set
    end

    def call(*commands)
      used = commands.flat_map(&:write_bins).map(&:type)
      unsupported = @unsupported_types.intersection(used)
      unless unsupported.empty?
        fail Aerospike::Exceptions::Aerospike.new(Aerospike::ResultCode::TYPE_NOT_SUPPORTED, "Particle type(s) not supported by cluster: #{@unsupported_types.to_a}")
      end
    end

  end # class

end # module
