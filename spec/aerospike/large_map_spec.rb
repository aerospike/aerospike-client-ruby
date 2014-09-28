require "spec_helper"

describe Aerospike::Client do

  describe "LDT operations" do

    describe "LargeMap operations" do

      let(:client) do
        described_class.new(nil, "127.0.0.1", 3000)
      end

      after do
        client.close
      end

      let(:lmap) do
        client.get_large_map(nil, Support.gen_random_key, 'bbb')
      end

      context "a large map object" do

        it "should #put, #get and #remove an element" do

          for i in 1..100
            j = i + 10000
            lmap.put(i, j)

            expect(lmap.size).to eq 1

            expect(lmap.get(i)).to eq ({ i => j })
            lmap.remove(i)

            # expect(lmap.get(i)).to eq nil
          end

        end # it

        it "should #put_map and #scan all elements" do

          map = {}
          for i in 1..100
            map[i] = i
          end

          lmap.put_map(map)
          expect(lmap.size).to eq 100

          expect(lmap.scan).to eq map

        end # it

        it "should get and map capacity" do

          for i in 1..10
            lmap.put(i, i)

            expect(lmap.size).to eq i
          end

          lmap.capacity = 99
          expect(lmap.capacity).to eq 99

        end # it

      end # describe

    end # describe

  end # describe

end # describe
