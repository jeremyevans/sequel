require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

shared_examples "parse then to_hash" do
  subject{ @c.parse(input).to_hash }
  it { should be_a_kind_of Hash }
  it { should == expected }
end

describe "pg_hstore extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.extend(Module.new{def bound_variable_arg(arg, conn) arg end})
    @m = Sequel::Postgres
    @c = @m::HStore
    @db.extension :pg_hstore
  end

  describe "When parsing via `HStore.parse`" do
    context "Given an empty string" do
      subject{ @c.parse('').to_hash }
      it { should be_a_kind_of Hash }
      it { should be_empty }
    end
  
    context "Given a simple key/value pair" do
      context "With no spaces in the hash expression" do
        it_behaves_like "parse then to_hash" do
          let(:input) { '"a"=>"b"' }
          let(:expected) { {'a'=>'b'} }
        end
      end
      context "With spaces in the hash expression" do
        it_behaves_like "parse then to_hash" do
          let(:input) { '"a" => "b"' }
          let(:expected) { {'a'=>'b'} }
        end
        context "But the key and value are not wrapped in quotes" do
          it_behaves_like "parse then to_hash" do
            let(:input) { 'k => v' }
            let(:expected) { {'k'=>'v'} }
          end
          it_behaves_like "parse then to_hash" do
            let(:input) { 'foo => bar, baz => whatever' }
            let(:expected) { {'foo'=>'bar', 'baz' => 'whatever'} }
          end
        end
        context "And non word characters in the key" do
          it_behaves_like "parse then to_hash" do
            let(:input) { '"1-a" => "anything at all"' }
            let(:expected) { {"1-a"=>"anything at all"} }
          end
          context "And also escaped characters in the value" do
            it_behaves_like "parse then to_hash" do
              let(:input) { %q(c=>"}", "\"a\""=>"b \"a b") }
              let(:expected) { {"c"=>"}", "\"a\""=>"b \"a b"} }
            end          
          end
        end
      end
    end
    context "Given a hash" do
      context "With multiple keys and values" do
        context "Of ordinary strings" do
          context "With no spaces in the hash expression" do
            it_behaves_like "parse then to_hash" do
              let(:input) { '"a"=>"b","c"=>"d"' }
              let(:expected) { {'a'=>'b', 'c'=>'d'} }
            end
          end
          context "With spaces in the hash expression" do
            it_behaves_like "parse then to_hash" do
              let(:input) { '"a"=>"b", "c"=>"d"' }
              let(:expected) { {'a'=>'b', 'c'=>'d'} }
            end
            it_behaves_like "parse then to_hash" do
              let(:input) { '"a" => "b", "c"=>"d"' }
              let(:expected) { {'a'=>'b', 'c'=>'d'} }
            end
            it_behaves_like "parse then to_hash" do
              let(:input) { '"a"=>"b","c" => "d"' }
              let(:expected) { {'a'=>'b', 'c'=>'d'} }
            end
          end
        end
        context "Containing a SQL null value" do
          it_behaves_like "parse then to_hash" do
            let(:input) { '"a"=>"b", "c"=>NULL' }
            let(:expected) { {'a'=>'b', 'c'=>nil} }
          end
        end
        context "Containing a value of a string with NULL in it" do
          it_behaves_like "parse then to_hash" do
            let(:input) { '"a"=>"b", "c"=>"NULL"' }
            let(:expected) { {'a'=>'b', 'c'=>'NULL'} }
          end
        end
        context "Containing a value of a string with escapes in it" do
          it_behaves_like "parse then to_hash" do
            let(:input) { '"a"=>"b", "c"=>"\\\\ \\"\'=>"' }
            let(:expected) { {'a'=>'b', 'c'=>'\ "\'=>'} }
          end
        end
      end
    end
  end

  it "should cache parse results" do
    s = ''
    o = s.from_hstore
    o.should == {}
    s.from_hstore.should equal(o)
  end

  it "should literalize HStores to strings correctly" do
    @db.literal({}.hstore).should == '\'\'::hstore'
    @db.literal({"a"=>"b"}.hstore).should == '\'"a"=>"b"\'::hstore'
    @db.literal({"c"=>nil}.hstore).should == '\'"c"=>NULL\'::hstore'
    @db.literal({"c"=>'NULL'}.hstore).should == '\'"c"=>"NULL"\'::hstore'
    @db.literal({'c'=>'\ "\'=>'}.hstore).should == '\'"c"=>"\\\\ \\"\'\'=>"\'::hstore'
    ['\'"a"=>"b","c"=>"d"\'::hstore', '\'"c"=>"d","a"=>"b"\'::hstore'].should include(@db.literal({"a"=>"b","c"=>"d"}.hstore))
  end

  it "should have Hash#hstore method for creating HStore instances" do
    {}.hstore.should be_a_kind_of(@c)
  end

  it "should HStore#to_hash method for getting underlying hash" do
    {}.hstore.to_hash.should be_a_kind_of(Hash)
  end

  it "should convert keys and values to strings on creation" do
    {1=>2}.hstore.to_hash.should == {"1"=>"2"}
  end

  it "should convert keys and values to strings on assignment" do
    v = {}.hstore
    v[1] = 2
    v.to_hash.should == {"1"=>"2"}
    v.store(:'1', 3)
    v.to_hash.should == {"1"=>"3"}
  end

  it "should not convert nil values to strings on creation" do
    {:foo=>nil}.hstore.to_hash.should == {"foo"=>nil}
  end

  it "should not convert nil values to strings on assignment" do
    v = {}.hstore
    v[:foo] = nil
    v.to_hash.should == {"foo"=>nil}
  end

  it "should convert lookups by key to string" do
    {'foo'=>'bar'}.hstore[:foo].should == 'bar'
    {'1'=>'bar'}.hstore[1].should == 'bar'

    {'foo'=>'bar'}.hstore.fetch(:foo).should == 'bar'
    {'foo'=>'bar'}.hstore.fetch(:foo2, 2).should == 2
    k = nil
    {'foo2'=>'bar'}.hstore.fetch(:foo){|key| k = key }.should == 'foo'
    k.should == 'foo'
    
    {'foo'=>'bar'}.hstore.has_key?(:foo).should be_true
    {'foo'=>'bar'}.hstore.has_key?(:bar).should be_false
    {'foo'=>'bar'}.hstore.key?(:foo).should be_true
    {'foo'=>'bar'}.hstore.key?(:bar).should be_false
    {'foo'=>'bar'}.hstore.member?(:foo).should be_true
    {'foo'=>'bar'}.hstore.member?(:bar).should be_false
    {'foo'=>'bar'}.hstore.include?(:foo).should be_true
    {'foo'=>'bar'}.hstore.include?(:bar).should be_false

    {'foo'=>'bar', '1'=>'2'}.hstore.values_at(:foo3, :foo, :foo2, 1).should == [nil, 'bar', nil, '2']

    if RUBY_VERSION >= '1.9.0'
      {'foo'=>'bar'}.hstore.assoc(:foo).should == ['foo', 'bar']
      {'foo'=>'bar'}.hstore.assoc(:foo2).should == nil
    end
  end

  it "should convert has_value?/value? lookups to string" do
    {'foo'=>'bar'}.hstore.has_value?(:bar).should be_true
    {'foo'=>'bar'}.hstore.has_value?(:foo).should be_false
    {'foo'=>'bar'}.hstore.value?(:bar).should be_true
    {'foo'=>'bar'}.hstore.value?(:foo).should be_false
  end

  it "should handle nil values in has_value?/value? lookups" do
    {'foo'=>''}.hstore.has_value?('').should be_true
    {'foo'=>''}.hstore.has_value?(nil).should be_false
    {'foo'=>nil}.hstore.has_value?(nil).should be_true
  end

  it "should have underlying hash convert lookups by key to string" do
    {'foo'=>'bar'}.hstore.to_hash[:foo].should == 'bar'
    {'1'=>'bar'}.hstore.to_hash[1].should == 'bar'
  end

  if RUBY_VERSION >= '1.9.0'
    it "should convert key lookups to string" do
      {'foo'=>'bar'}.hstore.key(:bar).should == 'foo'
      {'foo'=>'bar'}.hstore.key(:bar2).should be_nil
    end

    it "should handle nil values in key lookups" do
      {'foo'=>''}.hstore.key('').should == 'foo'
      {'foo'=>''}.hstore.key(nil).should == nil
      {'foo'=>nil}.hstore.key(nil).should == 'foo'
    end

    it "should convert rassoc lookups to string" do
      {'foo'=>'bar'}.hstore.rassoc(:bar).should == ['foo', 'bar']
      {'foo'=>'bar'}.hstore.rassoc(:bar2).should be_nil
    end

    it "should handle nil values in rassoc lookups" do
      {'foo'=>''}.hstore.rassoc('').should == ['foo', '']
      {'foo'=>''}.hstore.rassoc(nil).should == nil
      {'foo'=>nil}.hstore.rassoc(nil).should == ['foo', nil]
    end
  end

  it "should have delete convert key to string" do
    v = {'foo'=>'bar'}.hstore
    v.delete(:foo).should == 'bar'
    v.to_hash.should == {}
  end

  it "should handle #replace with hashes that do not use strings" do
    v = {'foo'=>'bar'}.hstore
    v.replace(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'bar'=>'1'}
    v.to_hash[:bar].should == '1'
  end

  it "should handle #merge with hashes that do not use strings" do
    v = {'foo'=>'bar'}.hstore.merge(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'foo'=>'bar', 'bar'=>'1'}
  end

  it "should handle #merge/#update with hashes that do not use strings" do
    v = {'foo'=>'bar'}.hstore
    v.merge!(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'foo'=>'bar', 'bar'=>'1'}

    v = {'foo'=>'bar'}.hstore
    v.update(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'foo'=>'bar', 'bar'=>'1'}
  end

  it "should support using hstores as bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg({'1'=>'2'}, nil).should == '"1"=>"2"'
    @db.bound_variable_arg({'1'=>'2'}.hstore, nil).should == '"1"=>"2"'
    @db.bound_variable_arg({'1'=>nil}.hstore, nil).should == '"1"=>NULL'
    @db.bound_variable_arg({'1'=>"NULL"}.hstore, nil).should == '"1"=>"NULL"'
    @db.bound_variable_arg({'1'=>"'\\ \"=>"}.hstore, nil).should == '"1"=>"\'\\\\ \\"=>"'
    ['"a"=>"b","c"=>"d"', '"c"=>"d","a"=>"b"'].should include(@db.bound_variable_arg({"a"=>"b","c"=>"d"}.hstore, nil))
  end

  it "should parse hstore type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'hstore'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :hstore]
  end

  it "should support typecasting for the hstore type" do
    h = {1=>2}.hstore
    @db.typecast_value(:hstore, h).should equal(h)
    @db.typecast_value(:hstore, '').should be_a_kind_of(@c)
    @db.typecast_value(:hstore, '').should == {}.hstore
    @db.typecast_value(:hstore, '"a"=>"b"').should == {"a"=>"b"}.hstore
    @db.typecast_value(:hstore, {}).should be_a_kind_of(@c)
    @db.typecast_value(:hstore, {}).should == {}.hstore
    @db.typecast_value(:hstore, {'a'=>'b'}).should == {"a"=>"b"}.hstore
    proc{@db.typecast_value(:hstore, [])}.should raise_error(Sequel::InvalidValue)
  end
end
