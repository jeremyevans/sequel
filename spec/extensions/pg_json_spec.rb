require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

Sequel.extension :pg_array, :pg_json

describe "pg_json extension" do
  before(:all) do
    m = Sequel::Postgres
    @m = m::JSONDatabaseMethods
    @hc = m::JSONHash
    @ac = m::JSONArray
    @bhc = m::JSONBHash
    @bac = m::JSONBArray
  end
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.extension(:pg_array, :pg_json)
  end

  it "should parse json strings correctly" do
    @m.parse_json('[]').should be_a_kind_of(@ac)
    @m.parse_json('[]').to_a.should == []
    @m.parse_json('[1]').to_a.should == [1]
    @m.parse_json('[1, 2]').to_a.should == [1, 2]
    @m.parse_json('[1, [2], {"a": "b"}]').to_a.should == [1, [2], {'a'=>'b'}]
    @m.parse_json('{}').should be_a_kind_of(@hc)
    @m.parse_json('{}').to_hash.should == {}
    @m.parse_json('{"a": "b"}').to_hash.should == {'a'=>'b'}
    @m.parse_json('{"a": "b", "c": [1, 2, 3]}').to_hash.should == {'a'=>'b', 'c'=>[1, 2, 3]}
    @m.parse_json('{"a": "b", "c": {"d": "e"}}').to_hash.should == {'a'=>'b', 'c'=>{'d'=>'e'}}
  end

  it "should parse json and non-json plain strings, integers, and floats correctly in db_parse_json" do
    @m.db_parse_json('{"a": "b", "c": {"d": "e"}}').to_hash.should == {'a'=>'b', 'c'=>{'d'=>'e'}}
    @m.db_parse_json('[1, [2], {"a": "b"}]').to_a.should == [1, [2], {'a'=>'b'}]
    @m.db_parse_json('1').should == 1
    @m.db_parse_json('"b"').should == 'b'
    @m.db_parse_json('1.1').should == 1.1
  end

  it "should parse json and non-json plain strings, integers, and floats correctly in db_parse_jsonb" do
    @m.db_parse_jsonb('{"a": "b", "c": {"d": "e"}}').to_hash.should == {'a'=>'b', 'c'=>{'d'=>'e'}}
    @m.db_parse_jsonb('[1, [2], {"a": "b"}]').to_a.should == [1, [2], {'a'=>'b'}]
    @m.db_parse_jsonb('1').should == 1
    @m.db_parse_jsonb('"b"').should == 'b'
    @m.db_parse_jsonb('1.1').should == 1.1
  end

  it "should raise an error when attempting to parse invalid json" do
    proc{@m.parse_json('')}.should raise_error(Sequel::InvalidValue)
    proc{@m.parse_json('1')}.should raise_error(Sequel::InvalidValue)

    begin
      Sequel.instance_eval do
        alias pj parse_json
        def parse_json(v)
          {'1'=>1, "'a'"=>'a', 'true'=>true, 'false'=>false, 'null'=>nil, 'o'=>Object.new}.fetch(v){pj(v)}
        end
      end
      @m.parse_json('1').should == 1
      @m.parse_json("'a'").should == 'a'
      @m.parse_json('true').should == true
      @m.parse_json('false').should == false
      @m.parse_json('null').should == nil
      proc{@m.parse_json('o')}.should raise_error(Sequel::InvalidValue)
    ensure
      Sequel.instance_eval do
        alias parse_json pj
      end
    end
  end

  it "should literalize JSONHash and JSONArray to strings correctly" do
    @db.literal(Sequel.pg_json([])).should == "'[]'::json"
    @db.literal(Sequel.pg_json([1, [2], {'a'=>'b'}])).should == "'[1,[2],{\"a\":\"b\"}]'::json"
    @db.literal(Sequel.pg_json({})).should == "'{}'::json"
    @db.literal(Sequel.pg_json('a'=>'b')).should == "'{\"a\":\"b\"}'::json"
  end

  it "should literalize JSONHash and JSONArray to strings correctly" do
    @db.literal(Sequel.pg_jsonb([])).should == "'[]'::jsonb"
    @db.literal(Sequel.pg_jsonb([1, [2], {'a'=>'b'}])).should == "'[1,[2],{\"a\":\"b\"}]'::jsonb"
    @db.literal(Sequel.pg_jsonb({})).should == "'{}'::jsonb"
    @db.literal(Sequel.pg_jsonb('a'=>'b')).should == "'{\"a\":\"b\"}'::jsonb"
  end

  it "should have Sequel.pg_json return JSONHash and JSONArray as is" do
    a = Sequel.pg_json({})
    Sequel.pg_json(a).should equal(a)
    a = Sequel.pg_json([])
    Sequel.pg_json(a).should equal(a)
  end

  it "should have Sequel.pg_json convert jsonb values" do
    a = {}
    v = Sequel.pg_json(Sequel.pg_jsonb(a))
    v.to_hash.should equal(a)
    v.should be_a_kind_of(@hc)

    a = []
    v = Sequel.pg_json(Sequel.pg_jsonb(a))
    v.to_a.should equal(a)
    v.should be_a_kind_of(@ac)
  end

  it "should have Sequel.pg_jsonb return JSONBHash and JSONBArray as is" do
    a = Sequel.pg_jsonb({})
    Sequel.pg_jsonb(a).should equal(a)
    a = Sequel.pg_jsonb([])
    Sequel.pg_jsonb(a).should equal(a)
  end

  it "should have Sequel.pg_jsonb convert json values" do
    a = {}
    v = Sequel.pg_jsonb(Sequel.pg_json(a))
    v.to_hash.should equal(a)
    v.should be_a_kind_of(@bhc)

    a = []
    v = Sequel.pg_jsonb(Sequel.pg_json(a))
    v.to_a.should equal(a)
    v.should be_a_kind_of(@bac)
  end

  it "should have JSONHashBase#to_hash method for getting underlying hash" do
    Sequel.pg_json({}).to_hash.should be_a_kind_of(Hash)
    Sequel.pg_jsonb({}).to_hash.should be_a_kind_of(Hash)
  end

  it "should allow aliasing json objects" do
    @db.literal(Sequel.pg_json({}).as(:a)).should == "'{}'::json AS a"
    @db.literal(Sequel.pg_json([]).as(:a)).should == "'[]'::json AS a"
    @db.literal(Sequel.pg_jsonb({}).as(:a)).should == "'{}'::jsonb AS a"
    @db.literal(Sequel.pg_jsonb([]).as(:a)).should == "'[]'::jsonb AS a"
  end

  it "should allow casting json objects" do
    @db.literal(Sequel.pg_json({}).cast(String)).should == "CAST('{}'::json AS text)"
    @db.literal(Sequel.pg_json([]).cast(String)).should == "CAST('[]'::json AS text)"
    @db.literal(Sequel.pg_jsonb({}).cast(String)).should == "CAST('{}'::jsonb AS text)"
    @db.literal(Sequel.pg_jsonb([]).cast(String)).should == "CAST('[]'::jsonb AS text)"
  end

  it "should have JSONArrayBase#to_a method for getting underlying array" do
    Sequel.pg_json([]).to_a.should be_a_kind_of(Array)
    Sequel.pg_jsonb([]).to_a.should be_a_kind_of(Array)
  end

  it "should support using JSONHashBase and JSONArrayBase as bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg(Sequel.pg_json([1]), nil).should == '[1]'
    @db.bound_variable_arg(Sequel.pg_json('a'=>'b'), nil).should == '{"a":"b"}'
    @db.bound_variable_arg(Sequel.pg_jsonb([1]), nil).should == '[1]'
    @db.bound_variable_arg(Sequel.pg_jsonb('a'=>'b'), nil).should == '{"a":"b"}'
  end

  it "should support using json[] and jsonb[] types in bound variables" do
    @db.bound_variable_arg(Sequel.pg_array([Sequel.pg_json([{"a"=>1}]), Sequel.pg_json("b"=>[1, 2])]), nil).should == '{"[{\\"a\\":1}]","{\\"b\\":[1,2]}"}'
    @db.bound_variable_arg(Sequel.pg_array([Sequel.pg_jsonb([{"a"=>1}]), Sequel.pg_jsonb("b"=>[1, 2])]), nil).should == '{"[{\\"a\\":1}]","{\\"b\\":[1,2]}"}'
  end

  it "should parse json type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'json'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :json]
  end

  it "should parse json type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'jsonb'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :jsonb]
  end

  it "should support typecasting for the json type" do
    h = Sequel.pg_json(1=>2)
    a = Sequel.pg_json([1])
    @db.typecast_value(:json, h).should equal(h)
    @db.typecast_value(:json, h.to_hash).should == h
    @db.typecast_value(:json, h.to_hash).should be_a_kind_of(@hc)
    @db.typecast_value(:json, Sequel.pg_jsonb(h)).should == h
    @db.typecast_value(:json, Sequel.pg_jsonb(h)).should be_a_kind_of(@hc)
    @db.typecast_value(:json, a).should equal(a)
    @db.typecast_value(:json, a.to_a).should == a
    @db.typecast_value(:json, a.to_a).should be_a_kind_of(@ac)
    @db.typecast_value(:json, Sequel.pg_jsonb(a)).should == a
    @db.typecast_value(:json, Sequel.pg_jsonb(a)).should be_a_kind_of(@ac)
    @db.typecast_value(:json, '[]').should == Sequel.pg_json([])
    @db.typecast_value(:json, '[]').should be_a_kind_of(@ac)
    @db.typecast_value(:json, '{"a": "b"}').should == Sequel.pg_json("a"=>"b")
    @db.typecast_value(:json, '{"a": "b"}').should be_a_kind_of(@hc)
    proc{@db.typecast_value(:json, '')}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:json, 1)}.should raise_error(Sequel::InvalidValue)
  end

  it "should support typecasting for the jsonb type" do
    h = Sequel.pg_jsonb(1=>2)
    a = Sequel.pg_jsonb([1])
    @db.typecast_value(:jsonb, h).should equal(h)
    @db.typecast_value(:jsonb, h.to_hash).should == h
    @db.typecast_value(:jsonb, h.to_hash).should be_a_kind_of(@bhc)
    @db.typecast_value(:jsonb, Sequel.pg_json(h)).should == h
    @db.typecast_value(:jsonb, Sequel.pg_json(h)).should be_a_kind_of(@bhc)
    @db.typecast_value(:jsonb, a).should equal(a)
    @db.typecast_value(:jsonb, a.to_a).should == a
    @db.typecast_value(:jsonb, a.to_a).should be_a_kind_of(@bac)
    @db.typecast_value(:jsonb, Sequel.pg_json(a)).should == a
    @db.typecast_value(:jsonb, Sequel.pg_json(a)).should be_a_kind_of(@bac)
    @db.typecast_value(:jsonb, '[]').should == Sequel.pg_jsonb([])
    @db.typecast_value(:jsonb, '[]').should be_a_kind_of(@bac)
    @db.typecast_value(:jsonb, '{"a": "b"}').should == Sequel.pg_jsonb("a"=>"b")
    @db.typecast_value(:jsonb, '{"a": "b"}').should be_a_kind_of(@bhc)
    proc{@db.typecast_value(:jsonb, '')}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:jsonb, 1)}.should raise_error(Sequel::InvalidValue)
  end

  it "should return correct results for Database#schema_type_class" do
    @db.schema_type_class(:json).should == [Sequel::Postgres::JSONHash, Sequel::Postgres::JSONArray]
    @db.schema_type_class(:jsonb).should == [Sequel::Postgres::JSONBHash, Sequel::Postgres::JSONBArray]
    @db.schema_type_class(:integer).should == Integer
  end
end
