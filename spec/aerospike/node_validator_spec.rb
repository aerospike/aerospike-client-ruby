# frozen_string_literal: true

describe Aerospike::NodeValidator do
  subject(:validator) do
    described_class.new(
      cluster, cluster.seeds[0], cluster.connection_timeout, cluster_name, tls_options
    )
  end

  let(:cluster) { ::Aerospike::Cluster.new(policy, hosts) }
  let(:policy) { ::Aerospike::ClientPolicy.new }
  let(:timeout) { }
  let(:cluster_name) { 'test' }
  let(:tls_options) { {} }

  let(:socket) { instance_double(::Aerospike::Socket::TCP, close: true) }

  context 'with loopback node' do
    let(:hosts) { [::Aerospike::Host.new('127.0.0.1', '3000')] }

    before do
      allow(socket).to receive(:write).and_return(nil)
      allow(socket).to receive(:read).and_return(nil)
      expect(::Aerospike::Cluster::CreateConnection).to receive(:call).and_return(socket)
    end

    it { expect(validator.aliases).to eq(hosts) }
  end

  context 'with default nodes' do
    let(:hosts) { ::Aerospike::Host::Parse.(ENV['AEROSPIKE_HOSTS']) }

    # map to string for non-object comparison
    it { expect(validator.aliases.map { |a| a.to_s }).to match_array(hosts.map { |a| a.to_s }) }
  end

  context 'with non-SSL LB discovery node' do
    let(:hosts) { [::Aerospike::Host.new('my.lb.com', '3000')] }

    before do
      allow(::Aerospike::Cluster::CreateConnection).to receive(:call).and_return(socket)
      expect(Resolv).to receive(:getaddresses).and_return(['101.1.1.1', '102.1.1.1'])

      expect(::Aerospike::Info).to receive(:request).and_return(
        {
          'node' => 'test-node',
          'service-clear-std' => '101.1.1.2:3002,101.1.1.3:3003'
        }
      )
      expect(::Aerospike::Info).to receive(:request).and_return(
        {
          'node' => 'test-node',
          'service-clear-std' => '102.1.1.2:3002,102.1.1.3:3003'
        }
      )
    end

    # map to string for non-object comparison
    it do
      expect(validator.aliases.map { |a| a.to_s }).to match_array(
        %w[101.1.1.2:3002 101.1.1.3:3003 102.1.1.2:3002 102.1.1.3:3003]
      )
    end
  end

  context 'with SSL LB discovery node' do
    let(:hosts) { [::Aerospike::Host.new('my.lb.com', '3000')] }
    let(:policy) { ::Aerospike::ClientPolicy.new(tls: { enable: true }) }

    before do
      allow(::Aerospike::Cluster::CreateConnection).to receive(:call).and_return(socket)
      expect(Resolv).to receive(:getaddresses).and_return(['101.1.1.1', '102.1.1.1'])

      expect(::Aerospike::Info).to receive(:request).and_return(
        {
          'node' => 'test-node',
          'service-tls-std' => '101.1.1.2:3002,101.1.1.3:3003'
        }
      )
      expect(::Aerospike::Info).to receive(:request).and_return(
        {
          'node' => 'test-node',
          'service-tls-std' => '102.1.1.2:3002,102.1.1.3:3003'
        }
      )
    end

    # map to string for non-object comparison
    it do
      expect(validator.aliases.map { |a| a.to_s }).to match_array(
        %w[101.1.1.2:3002 101.1.1.3:3003 102.1.1.2:3002 102.1.1.3:3003]
      )
    end
  end
end
