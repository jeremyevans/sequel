require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_hstore extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @m = Sequel::Postgres
    @c = @m::HStore
    @db.extension :pg_hstore
  end

  it "should parse hstore strings correctly" do
    @c.parse('').to_hash.should == {}
    @c.parse('"a"=>"b"').to_hash.should == {'a'=>'b'}
    @c.parse('"a"=>"b", "c"=>NULL').to_hash.should == {'a'=>'b', 'c'=>nil}
    @c.parse('"a"=>"b", "c"=>"NULL"').to_hash.should == {'a'=>'b', 'c'=>'NULL'}
    @c.parse('"a"=>"b", "c"=>"\\\\ \\"\'=>"').to_hash.should == {'a'=>'b', 'c'=>'\ "\'=>'}
  end

  it "should cache parse results" do
    r = @c::Parser.new('')
    o = r.parse
    o.should == {}
    r.parse.should equal(o)
  end

  it "should literalize HStores to strings correctly" do
    @db.literal(Sequel.hstore({})).should == '\'\'::hstore'
    @db.literal(Sequel.hstore("a"=>"b")).should == '\'"a"=>"b"\'::hstore'
    @db.literal(Sequel.hstore("c"=>nil)).should == '\'"c"=>NULL\'::hstore'
    @db.literal(Sequel.hstore("c"=>'NULL')).should == '\'"c"=>"NULL"\'::hstore'
    @db.literal(Sequel.hstore('c'=>'\ "\'=>')).should == '\'"c"=>"\\\\ \\"\'\'=>"\'::hstore'
    ['\'"a"=>"b","c"=>"d"\'::hstore', '\'"c"=>"d","a"=>"b"\'::hstore'].should include(@db.literal(Sequel.hstore("a"=>"b","c"=>"d")))
  end

  it "should have Sequel.hstore method for creating HStore instances" do
    Sequel.hstore({}).should be_a_kind_of(@c)
  end

  it "should have Sequel.hstore return HStores as-is" do
    a = Sequel.hstore({})
    Sequel.hstore(a).should equal(a)
  end

  it "should HStore#to_hash method for getting underlying hash" do
    Sequel.hstore({}).to_hash.should be_a_kind_of(Hash)
  end

  it "should convert keys and values to strings on creation" do
    Sequel.hstore(1=>2).to_hash.should == {"1"=>"2"}
  end

  it "should convert keys and values to strings on assignment" do
    v = Sequel.hstore({})
    v[1] = 2
    v.to_hash.should == {"1"=>"2"}
    v.store(:'1', 3)
    v.to_hash.should == {"1"=>"3"}
  end

  it "should not convert nil values to strings on creation" do
    Sequel.hstore(:foo=>nil).to_hash.should == {"foo"=>nil}
  end

  it "should not convert nil values to strings on assignment" do
    v = Sequel.hstore({})
    v[:foo] = nil
    v.to_hash.should == {"foo"=>nil}
  end

  it "should convert lookups by key to string" do
    Sequel.hstore('foo'=>'bar')[:foo].should == 'bar'
    Sequel.hstore('1'=>'bar')[1].should == 'bar'

    Sequel.hstore('foo'=>'bar').fetch(:foo).should == 'bar'
    Sequel.hstore('foo'=>'bar').fetch(:foo2, 2).should == 2
    k = nil
    Sequel.hstore('foo2'=>'bar').fetch(:foo){|key| k = key }.should == 'foo'
    k.should == 'foo'
    
    Sequel.hstore('foo'=>'bar').has_key?(:foo).should be_true
    Sequel.hstore('foo'=>'bar').has_key?(:bar).should be_false
    Sequel.hstore('foo'=>'bar').key?(:foo).should be_true
    Sequel.hstore('foo'=>'bar').key?(:bar).should be_false
    Sequel.hstore('foo'=>'bar').member?(:foo).should be_true
    Sequel.hstore('foo'=>'bar').member?(:bar).should be_false
    Sequel.hstore('foo'=>'bar').include?(:foo).should be_true
    Sequel.hstore('foo'=>'bar').include?(:bar).should be_false

    Sequel.hstore('foo'=>'bar', '1'=>'2').values_at(:foo3, :foo, :foo2, 1).should == [nil, 'bar', nil, '2']

    if RUBY_VERSION >= '1.9.0'
      Sequel.hstore('foo'=>'bar').assoc(:foo).should == ['foo', 'bar']
      Sequel.hstore('foo'=>'bar').assoc(:foo2).should == nil
    end
  end

  it "should convert has_value?/value? lookups to string" do
    Sequel.hstore('foo'=>'bar').has_value?(:bar).should be_true
    Sequel.hstore('foo'=>'bar').has_value?(:foo).should be_false
    Sequel.hstore('foo'=>'bar').value?(:bar).should be_true
    Sequel.hstore('foo'=>'bar').value?(:foo).should be_false
  end

  it "should handle nil values in has_value?/value? lookups" do
    Sequel.hstore('foo'=>'').has_value?('').should be_true
    Sequel.hstore('foo'=>'').has_value?(nil).should be_false
    Sequel.hstore('foo'=>nil).has_value?(nil).should be_true
  end

  it "should have underlying hash convert lookups by key to string" do
    Sequel.hstore('foo'=>'bar').to_hash[:foo].should == 'bar'
    Sequel.hstore('1'=>'bar').to_hash[1].should == 'bar'
  end

  if RUBY_VERSION >= '1.9.0'
    it "should convert key lookups to string" do
      Sequel.hstore('foo'=>'bar').key(:bar).should == 'foo'
      Sequel.hstore('foo'=>'bar').key(:bar2).should be_nil
    end

    it "should handle nil values in key lookups" do
      Sequel.hstore('foo'=>'').key('').should == 'foo'
      Sequel.hstore('foo'=>'').key(nil).should == nil
      Sequel.hstore('foo'=>nil).key(nil).should == 'foo'
    end

    it "should convert rassoc lookups to string" do
      Sequel.hstore('foo'=>'bar').rassoc(:bar).should == ['foo', 'bar']
      Sequel.hstore('foo'=>'bar').rassoc(:bar2).should be_nil
    end

    it "should handle nil values in rassoc lookups" do
      Sequel.hstore('foo'=>'').rassoc('').should == ['foo', '']
      Sequel.hstore('foo'=>'').rassoc(nil).should == nil
      Sequel.hstore('foo'=>nil).rassoc(nil).should == ['foo', nil]
    end
  end

  it "should have delete convert key to string" do
    v = Sequel.hstore('foo'=>'bar')
    v.delete(:foo).should == 'bar'
    v.to_hash.should == {}
  end

  it "should handle #replace with hashes that do not use strings" do
    v = Sequel.hstore('foo'=>'bar')
    v.replace(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'bar'=>'1'}
    v.to_hash[:bar].should == '1'
  end

  it "should handle #merge with hashes that do not use strings" do
    v = Sequel.hstore('foo'=>'bar').merge(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'foo'=>'bar', 'bar'=>'1'}
  end

  it "should handle #merge/#update with hashes that do not use strings" do
    v = Sequel.hstore('foo'=>'bar')
    v.merge!(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'foo'=>'bar', 'bar'=>'1'}

    v = Sequel.hstore('foo'=>'bar')
    v.update(:bar=>1)
    v.should be_a_kind_of(@c)
    v.should == {'foo'=>'bar', 'bar'=>'1'}
  end

  it "should support using hstores as bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg({'1'=>'2'}, nil).should == '"1"=>"2"'
    @db.bound_variable_arg(Sequel.hstore('1'=>'2'), nil).should == '"1"=>"2"'
    @db.bound_variable_arg(Sequel.hstore('1'=>nil), nil).should == '"1"=>NULL'
    @db.bound_variable_arg(Sequel.hstore('1'=>"NULL"), nil).should == '"1"=>"NULL"'
    @db.bound_variable_arg(Sequel.hstore('1'=>"'\\ \"=>"), nil).should == '"1"=>"\'\\\\ \\"=>"'
    ['"a"=>"b","c"=>"d"', '"c"=>"d","a"=>"b"'].should include(@db.bound_variable_arg(Sequel.hstore("a"=>"b","c"=>"d"), nil))
  end

  it "should parse hstore type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'hstore'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :hstore]
  end

  it "should support typecasting for the hstore type" do
    h = Sequel.hstore(1=>2)
    @db.typecast_value(:hstore, h).should equal(h)
    @db.typecast_value(:hstore, {}).should be_a_kind_of(@c)
    @db.typecast_value(:hstore, {}).should == Sequel.hstore({})
    @db.typecast_value(:hstore, {'a'=>'b'}).should == Sequel.hstore("a"=>"b")
    proc{@db.typecast_value(:hstore, [])}.should raise_error(Sequel::InvalidValue)
  end

  it "should be serializable" do 
    v = Sequel.hstore('foo'=>'bar')
    dump = Marshal.dump(v) 
    Marshal.load(dump).should == v    
  end 
end
