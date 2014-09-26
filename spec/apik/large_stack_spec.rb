require "spec_helper"

describe Apik::Client do

  describe "LDT operations" do

    describe "LargeStack operations" do

      let(:client) do
        described_class.new(nil, "127.0.0.1", 3000)
      end

      after do
        client.close
      end

      let(:lstack) do
        client.get_large_stack(nil, Support.gen_random_key, 'bbb')
      end

      context "a large stack object" do

        it "should #push and #peek an element" do

          for i in 1..100
            lstack.push(i)

            expect(lstack.size).to eq i

            expect(lstack.peek(1)).to eq [i]
          end

        end # it

        it "should #scan all elements" do

          for i in 1..100
            lstack.push(i)

            expect(lstack.size).to eq i
          end

          expect(lstack.scan).to eq (1..100).to_a.reverse!

        end # it

        it "should get and set capacity" do

          for i in 1..10
            lstack.push(i)

            expect(lstack.size).to eq i
          end

          lstack.capacity = 99
          expect(lstack.capacity).to eq 99

        end # it

      end # describe

    end # describe

  end # describe

end # describe
