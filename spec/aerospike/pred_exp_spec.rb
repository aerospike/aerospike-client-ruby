# frozen_string_literal: true

describe Aerospike::PredExp do
  let(:client) { Support.client }

  before :all do
    @namespace = "test"
    @set = "predexp"
    @record_count = 5
    @record_count.times do |i|
      key = Aerospike::Key.new(@namespace, @set, i)
      bin_map = {
        'bin1' => "value#{i}",
        'bin2' => i,
        'bin3' => [ i, i + 1_000, i + 1_000_000 ],
        'bin4' => { "key#{i}" => i },
        'bin5' => Aerospike::GeoJSON.new(type: "Point", coordinates: [103.9114, 1.3083])
      }
      Support.client.put(key, bin_map)
    end

    tasks = []
    tasks << Support.client.create_index(@namespace, @set, "index_str_bin1", "bin1", :string)
    tasks << Support.client.create_index(@namespace, @set, "index_int_bin2", "bin2", :numeric)
    tasks << Support.client.create_index(@namespace, @set, "index_lst_bin3", "bin3", :numeric, :list)
    tasks << Support.client.create_index(@namespace, @set, "index_mapkey_bin4", "bin4", :string, :mapkeys)
    tasks << Support.client.create_index(@namespace, @set, "index_mapval_bin4", "bin4", :numeric, :mapvalues)
    tasks.each(&:wait_till_completed)
    expect(tasks.all?(&:completed?)).to be true
  end

  let(:statement) { Aerospike::Statement.new(@namespace, @set) }

  describe 'expressions for integer bins' do
    let(:value) { 3 }
    let(:integer_bin) { 'bin2' }
    let(:predexp) do
      [
        Aerospike::PredExp.integer_bin(integer_bin),
        Aerospike::PredExp.integer_value(value)
      ]
    end

    it 'returns records with bin equal to value' do
      predexp << Aerospike::PredExp.integer_equal
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[integer_bin]).to eq(value)
      end
    end

    it 'returns records not equal to value' do
      predexp << Aerospike::PredExp.integer_unequal
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[integer_bin]).not_to eq(value)
      end
    end

    it 'returns records with bin less than value' do
      predexp << Aerospike::PredExp.integer_less
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[integer_bin]).to be < value
      end
    end

    it 'returns records with bin less or equal than value' do
      predexp << Aerospike::PredExp.integer_less_eq
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[integer_bin]).to be <= value
      end
    end

    it 'returns records with bin greater than value' do
      predexp << Aerospike::PredExp.integer_greater
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[integer_bin]).to be > value
      end
    end

    it 'returns records with bin greater or equal to value' do
      predexp << Aerospike::PredExp.integer_greater_eq
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[integer_bin]).to be >= value
      end
    end
  end

  describe 'expressions for string bins' do
    let(:value) { 'value3' }
    let(:string_bin) { 'bin1' }
    let(:predexp) do
      [
        Aerospike::PredExp.string_bin(string_bin),
        Aerospike::PredExp.string_value(value)
      ]
    end

    it 'returns records equal to value' do
      predexp << Aerospike::PredExp.string_equal
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[string_bin]).to eq(value)
      end
    end

    it 'returns records not equal to value' do
      predexp << Aerospike::PredExp.string_unequal
      statement.predexp = predexp
      rs = client.query(statement)
      rs.each do |r|
        expect(r.bins[string_bin]).not_to eq(value)
      end
    end
  end
end
