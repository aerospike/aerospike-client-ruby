require "spec_helper"

describe Aerospike::Client do

  describe "LDT operations" do

    describe "LargeList operations" do

      let(:client) do
        described_class.new(nil, "127.0.0.1", 3000)
      end

      after do
        client.close
      end

      let(:llist) do
        client.get_large_list(nil, Support.gen_random_key, 'bbb')
      end

      context "a large list object" do

        it "should #add, #find and #remove an element" do

          for i in 1..100
            llist.add(i)

            expect(llist.size).to eq 1

            expect(llist.find(i)).to eq [i]
            llist.remove(i)

            expect(llist.find(i)).to eq nil
          end

        end # it

        it "should #scan all elements" do

          for i in 1..100
            llist.add(i)

            expect(llist.size).to eq i
          end

          expect(llist.scan).to eq (1..100).to_a

        end # it

        it "should get and set capacity" do

          for i in 1..10
            llist.add(i)

            expect(llist.size).to eq i
          end

          llist.capacity = 99
          expect(llist.capacity).to eq 99

        end # it

      end # describe

    end # describe

  end # describe

end # describe
