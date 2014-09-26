require "spec_helper"

describe Apik::Client do

  describe "LDT operations" do

    describe "LargeSet operations" do

      let(:client) do
        described_class.new(nil, "127.0.0.1", 3000)
      end

      after do
        client.close
      end

      let(:lset) do
        client.get_large_set(nil, Support.gen_random_key, 'bbb')
      end

      context "a large set object" do

        it "should #add, #find and #remove an element" do

          for i in 1..100
            lset.add(i)

            expect(lset.size).to eq 1

            expect(lset.get(i)).to eq i
            expect(lset.exists(i)).to eq true
            lset.remove(i)

            expect(lset.exists(i)).to eq false
          end

        end # it

        it "should #scan all elements" do

          for i in 1..100
            lset.add(i)

            expect(lset.size).to eq i
          end

          expect((lset.scan & (1..100).to_a).length).to eq 100

        end # it

        it "should get and set capacity" do

          for i in 1..10
            lset.add(i)

            expect(lset.size).to eq i
          end

          lset.capacity = 99
          expect(lset.capacity).to eq 99

        end # it

      end # describe

    end # describe

  end # describe

end # describe
