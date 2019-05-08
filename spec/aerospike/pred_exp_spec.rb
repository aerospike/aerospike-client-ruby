# frozen_string_literal: true

describe Aerospike::PredExp do
  let(:client) { Support.client }

  before :all do
    @lat = 1.3083
    @lng = 103.9114
    @namespace = "test"
    @set = "predexp"
    @record_count = 5
    point = Aerospike::GeoJSON.new(type: "Point", coordinates: [@lng, @lat])
    @record_count.times do |i|
      key = Aerospike::Key.new(@namespace, @set, i)
      bin_map = {
        'bin1' => "value#{i}",
        'bin2' => i,
        'bin3' => [ i, i + 1_000, i + 1_000_000 ],
        'bin4' => { "key#{i}" => i },
        # 'bin5' => Support::Geo.destination_point(@lng, @lat, i * 200, rand(0..359))
      }
      Support.client.put(key, bin_map)
    end

    tasks = []
    tasks << Support.client.create_index(@namespace, @set, "index_str_bin1", "bin1", :string)
    tasks << Support.client.create_index(@namespace, @set, "index_int_bin2", "bin2", :numeric)
    tasks << Support.client.create_index(@namespace, @set, "index_lst_bin3", "bin3", :numeric, :list)
    tasks << Support.client.create_index(@namespace, @set, "index_mapkey_bin4", "bin4", :string, :mapkeys)
    tasks << Support.client.create_index(@namespace, @set, "index_mapval_bin4", "bin4", :numeric, :mapvalues)
    tasks << Support.client.create_index(@namespace, @set, "index_geo_bin5", "bin5", :geo2dsphere)
    tasks.each(&:wait_till_completed)
    expect(tasks.all?(&:completed?)).to be true
  end

  let(:statement) { Aerospike::Statement.new(@namespace, @set) }
  let(:string_bin) { 'bin1' }
  let(:integer_bin) { 'bin2' }
  let(:list_bin) { 'bin3' }
  let(:map_bin) { 'bin4' }

  describe 'expressions for integer bins' do
    let(:value) { 3 }
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

    context 'regex' do
      let(:value) { 'lue3' }
      context 'default flag' do
        it 'returns records matching regex' do
          predexp << Aerospike::PredExp.string_regex(Aerospike::PredExp::Regex::Flags::NONE)
          statement.predexp = predexp
          rs = client.query(statement)
          count = 0
          rs.each do |r|
            count += 1
          end

          expect(count).to eq(1)
        end
      end
    end
  end

  # context 'expressions for GeoJSON bins' do
  #   let(:geo_json_bin) { 'bin5' }
  #
  #   context 'within' do
  #     let(:geo_json_area_circle) { Aerospike::GeoJSON.new(type: 'AeroCircle', coordinates: [[@lng, @lat], 10000]) }
  #     let(:geo_json_polygon) do
  #       Aerospike::GeoJSON.new(
  #         type: "Polygon",
  #         coordinates: [
  #           [
  #             [@lng - 1, @lat - 1],
  #             [@lng + 1, @lat - 1],
  #             [@lng + 1, @lat + 1],
  #             [@lng - 1, @lat + 1],
  #             [@lng - 1, @lat - 1]
  #           ]
  #         ]
  #       )
  #     end
  #
  #     let(:predexp) do
  #       [
  #         Aerospike::PredExp.geojson_bin(geo_json_bin),
  #         Aerospike::PredExp.geojson_contains,
  #         Aerospike::PredExp.geojson_value(geo_json_area_circle.to_json)
  #       ]
  #     end
  #
  #     it 'returns records within a circle' do
  #       # predexp <<
  #       statement.predexp = predexp
  #       rs = client.query(statement)
  #       count = 0
  #       rs.each do |r|
  #         puts r.bins
  #         count += 1
  #       end
  #       expect(count).to eq(3)
  #     end
  #   end
  # end

  context 'expressions for bins with lists' do
    let(:value) { 3 }

    context 'list_or' do
      let(:predexp) do
        [
          Aerospike::PredExp.integer_value(value),
          Aerospike::PredExp.integer_var('x'),
          Aerospike::PredExp.integer_equal,
          Aerospike::PredExp.list_bin(list_bin),
          Aerospike::PredExp.list_iterate_or('x')
        ]
      end

      it 'returns items which has any item equal to value' do
        statement.predexp = predexp
        rs = client.query(statement)
        count = 0
        rs.each do |r|
          expect(r.bins[list_bin]).to include(value)
          count += 1
        end

        expect(count).to eq(1)
      end
    end

    context 'list_and' do
      let(:predexp) do
        [
          Aerospike::PredExp.integer_value(value),
          Aerospike::PredExp.integer_var('x'),
          Aerospike::PredExp.integer_unequal,
          Aerospike::PredExp.list_bin(list_bin),
          Aerospike::PredExp.list_iterate_and('x')
        ]
      end

      it 'returns items which do NOT contain an item equal to value' do
        statement.predexp = predexp
        rs = client.query(statement)
        count = 0
        rs.each do |r|
          expect(r.bins[list_bin]).not_to include(value)
          count += 1
        end

        expect(count).to eq(4)
      end
    end
  end

  context 'expressions for bins with mapkeys' do
    let(:bin) { 'bin4' }
    let(:value) { 3 }
    let(:key) { 'key3' }

    context 'keys' do
      context 'iterate_or' do
        let(:predexp) do
          [
            Aerospike::PredExp.string_value(key),
            Aerospike::PredExp.string_var('k'),
            Aerospike::PredExp.string_equal,
            Aerospike::PredExp.map_bin(map_bin),
            Aerospike::PredExp.mapkey_iterate_or('k')
          ]
        end

        it 'returns records with map bins containing chosen key' do
          statement.predexp = predexp
          rs = client.query(statement)

          count = 0
          rs.each do |r|
            expect(r.bins[map_bin].keys).to include(key)
            count += 1
          end

          expect(count).to eq(1)
        end
      end

      context 'iterate_and' do
        let(:predexp) do
          [
            Aerospike::PredExp.string_value(key),
            Aerospike::PredExp.string_var('k'),
            Aerospike::PredExp.string_unequal,
            Aerospike::PredExp.map_bin(map_bin),
            Aerospike::PredExp.mapkey_iterate_and('k')
          ]
        end

        it 'returns records with map bins NOT containing chosen key' do
          statement.predexp = predexp
          rs = client.query(statement)

          count = 0
          rs.each do |r|
            expect(r.bins[map_bin].keys).not_to include(key)
            count += 1
          end

          expect(count).to eq(4)
        end
      end
    end

    context 'values' do
      context 'iterate_or' do
        let(:predexp) do
          [
            Aerospike::PredExp.integer_value(value),
            Aerospike::PredExp.integer_var('v'),
            Aerospike::PredExp.integer_equal,
            Aerospike::PredExp.map_bin(map_bin),
            Aerospike::PredExp.mapval_iterate_or('v')
          ]
        end

        it 'returns records with map bins containing chosen value' do
          statement.predexp = predexp
          rs = client.query(statement)

          count = 0
          rs.each do |r|
            expect(r.bins[map_bin].values).to include(value)
            count += 1
          end

          expect(count).to eq(1)
        end
      end

      context 'iterate_and' do
        let(:predexp) do
          [
            Aerospike::PredExp.integer_value(value),
            Aerospike::PredExp.integer_var('v'),
            Aerospike::PredExp.integer_unequal,
            Aerospike::PredExp.map_bin(map_bin),
            Aerospike::PredExp.mapval_iterate_and('v')
          ]
        end

        it 'returns records with map bins NOT containing chosen value' do
          statement.predexp = predexp
          rs = client.query(statement)

          count = 0
          rs.each do |r|
            expect(r.bins[map_bin].values).not_to include(value)
            count += 1
          end

          expect(count).to eq(4)
        end
      end
    end
  end

  context '.and' do
    let(:min_value) { 2 }

    # return records with bin2 > 2 AND bin1 equal to 'value4'
    let(:predexp) do
      [
        Aerospike::PredExp.integer_bin(integer_bin),
        Aerospike::PredExp.integer_value(min_value),
        Aerospike::PredExp.integer_greater,
        Aerospike::PredExp.string_bin(string_bin),
        Aerospike::PredExp.string_value('value4'),
        Aerospike::PredExp.string_equal,
        Aerospike::PredExp.and(2)
      ]
    end

    it 'returns records fulfilling multiple predicates' do
      statement.predexp = predexp
      rs = client.query(statement)

      count = 0
      rs.each do |r|
        bins = r.bins
        expect(bins[integer_bin]).to be > min_value
        expect(bins[string_bin]).to eq('value4')
        count += 1
      end

      expect(count).to eq(1)
    end
  end

  context '.or' do
    let(:max_value) { 2 }

    # return all records with bin2 <=2 OR bin1 equal to 'value4'
    let(:predexp) do
      [
        Aerospike::PredExp.integer_bin(integer_bin),
        Aerospike::PredExp.integer_value(max_value),
        Aerospike::PredExp.integer_less_eq,
        Aerospike::PredExp.string_bin(string_bin),
        Aerospike::PredExp.string_value('value4'),
        Aerospike::PredExp.string_equal,
        Aerospike::PredExp.or(2)
      ]
    end

    it 'returns records fulfilling one of the predicates' do
      statement.predexp = predexp
      rs = client.query(statement)

      count = 0
      rs.each do |r|
        bins = r.bins
        if bins[integer_bin] > max_value
          expect(bins[string_bin]).to eq('value4')
        else
          expect(bins[integer_bin]).to be <= max_value
        end

        count += 1
      end

      expect(count).to eq(4)
    end
  end

  context '.not' do
    let(:value) { 3 }
    # return all records with bin2 not equal 3
    let(:predexp) do
      [
        Aerospike::PredExp.integer_bin(integer_bin),
        Aerospike::PredExp.integer_value(value),
        Aerospike::PredExp.integer_equal,
        Aerospike::PredExp.not
      ]
    end

    it 'returns records NOT fulfilling the predicate' do
      statement.predexp = predexp
      rs = client.query(statement)

      count = 0
      rs.each do |r|
        expect(r.bins[integer_bin]).not_to eq(value)
        count += 1
      end

      expect(count).to eq(4)
    end
  end
end
