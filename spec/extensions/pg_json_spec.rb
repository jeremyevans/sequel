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
    @m.parse_json('[]').class.must_equal(@ac)
    @m.parse_json('[]').to_a.must_equal []
    @m.parse_json('[1]').to_a.must_equal [1]
    @m.parse_json('[1, 2]').to_a.must_equal [1, 2]
    @m.parse_json('[1, [2], {"a": "b"}]').to_a.must_equal [1, [2], {'a'=>'b'}]
    @m.parse_json('{}').class.must_equal(@hc)
    @m.parse_json('{}').to_hash.must_equal({})
    @m.parse_json('{"a": "b"}').to_hash.must_equal('a'=>'b')
    @m.parse_json('{"a": "b", "c": [1, 2, 3]}').to_hash.must_equal('a'=>'b', 'c'=>[1, 2, 3])
    @m.parse_json('{"a": "b", "c": {"d": "e"}}').to_hash.must_equal('a'=>'b', 'c'=>{'d'=>'e'})
  end

  it "should parse json and non-json plain strings, integers, and floats correctly in db_parse_json" do
    @m.db_parse_json('{"a": "b", "c": {"d": "e"}}').to_hash.must_equal('a'=>'b', 'c'=>{'d'=>'e'})
    @m.db_parse_json('[1, [2], {"a": "b"}]').to_a.must_equal [1, [2], {'a'=>'b'}]
    @m.db_parse_json('1').must_equal 1
    @m.db_parse_json('"b"').must_equal 'b'
    @m.db_parse_json('1.1').must_equal 1.1
  end

  it "should parse json and non-json plain strings, integers, and floats correctly in db_parse_jsonb" do
    @m.db_parse_jsonb('{"a": "b", "c": {"d": "e"}}').to_hash.must_equal('a'=>'b', 'c'=>{'d'=>'e'})
    @m.db_parse_jsonb('[1, [2], {"a": "b"}]').to_a.must_equal [1, [2], {'a'=>'b'}]
    @m.db_parse_jsonb('1').must_equal 1
    @m.db_parse_jsonb('"b"').must_equal 'b'
    @m.db_parse_jsonb('1.1').must_equal 1.1
  end

  it "should raise an error when attempting to parse invalid json" do
    proc{@m.parse_json('')}.must_raise(Sequel::InvalidValue)
    proc{@m.parse_json('1')}.must_raise(Sequel::InvalidValue)

    begin
      Sequel.instance_eval do
        alias pj parse_json
        def parse_json(v)
          {'1'=>1, "'a'"=>'a', 'true'=>true, 'false'=>false, 'null'=>nil, 'o'=>Object.new}.fetch(v){pj(v)}
        end
      end
      @m.parse_json('1').must_equal 1
      @m.parse_json("'a'").must_equal 'a'
      @m.parse_json('true').must_equal true
      @m.parse_json('false').must_equal false
      @m.parse_json('null').must_equal nil
      proc{@m.parse_json('o')}.must_raise(Sequel::InvalidValue)
    ensure
      Sequel.instance_eval do
        alias parse_json pj
      end
    end
  end

  it "should literalize JSONHash and JSONArray to strings correctly" do
    @db.literal(Sequel.pg_json([])).must_equal "'[]'::json"
    @db.literal(Sequel.pg_json([1, [2], {'a'=>'b'}])).must_equal "'[1,[2],{\"a\":\"b\"}]'::json"
    @db.literal(Sequel.pg_json({})).must_equal "'{}'::json"
    @db.literal(Sequel.pg_json('a'=>'b')).must_equal "'{\"a\":\"b\"}'::json"
  end

  it "should literalize JSONHash and JSONArray to strings correctly" do
    @db.literal(Sequel.pg_jsonb([])).must_equal "'[]'::jsonb"
    @db.literal(Sequel.pg_jsonb([1, [2], {'a'=>'b'}])).must_equal "'[1,[2],{\"a\":\"b\"}]'::jsonb"
    @db.literal(Sequel.pg_jsonb({})).must_equal "'{}'::jsonb"
    @db.literal(Sequel.pg_jsonb('a'=>'b')).must_equal "'{\"a\":\"b\"}'::jsonb"
  end

  it "should have Sequel.pg_json return JSONHash and JSONArray as is" do
    a = Sequel.pg_json({})
    Sequel.pg_json(a).object_id.must_equal(a.object_id)
    a = Sequel.pg_json([])
    Sequel.pg_json(a).object_id.must_equal(a.object_id)
  end

  it "should have Sequel.pg_json convert jsonb values" do
    a = {}
    v = Sequel.pg_json(Sequel.pg_jsonb(a))
    v.to_hash.must_be_same_as(a)
    v.class.must_equal(@hc)

    a = []
    v = Sequel.pg_json(Sequel.pg_jsonb(a))
    v.to_a.must_be_same_as(a)
    v.class.must_equal(@ac)
  end

  it "should have Sequel.pg_jsonb return JSONBHash and JSONBArray as is" do
    a = Sequel.pg_jsonb({})
    Sequel.pg_jsonb(a).object_id.must_equal(a.object_id)
    a = Sequel.pg_jsonb([])
    Sequel.pg_jsonb(a).object_id.must_equal(a.object_id)
  end

  it "should have Sequel.pg_jsonb convert json values" do
    a = {}
    v = Sequel.pg_jsonb(Sequel.pg_json(a))
    v.to_hash.must_be_same_as(a)
    v.class.must_equal(@bhc)

    a = []
    v = Sequel.pg_jsonb(Sequel.pg_json(a))
    v.to_a.must_be_same_as(a)
    v.class.must_equal(@bac)
  end

  it "should have JSONHashBase#to_hash method for getting underlying hash" do
    Sequel.pg_json({}).to_hash.must_be_kind_of(Hash)
    Sequel.pg_jsonb({}).to_hash.must_be_kind_of(Hash)
  end

  it "should allow aliasing json objects" do
    @db.literal(Sequel.pg_json({}).as(:a)).must_equal "'{}'::json AS a"
    @db.literal(Sequel.pg_json([]).as(:a)).must_equal "'[]'::json AS a"
    @db.literal(Sequel.pg_jsonb({}).as(:a)).must_equal "'{}'::jsonb AS a"
    @db.literal(Sequel.pg_jsonb([]).as(:a)).must_equal "'[]'::jsonb AS a"
  end

  it "should allow casting json objects" do
    @db.literal(Sequel.pg_json({}).cast(String)).must_equal "CAST('{}'::json AS text)"
    @db.literal(Sequel.pg_json([]).cast(String)).must_equal "CAST('[]'::json AS text)"
    @db.literal(Sequel.pg_jsonb({}).cast(String)).must_equal "CAST('{}'::jsonb AS text)"
    @db.literal(Sequel.pg_jsonb([]).cast(String)).must_equal "CAST('[]'::jsonb AS text)"
  end

  it "should have JSONArrayBase#to_a method for getting underlying array" do
    Sequel.pg_json([]).to_a.must_be_kind_of(Array)
    Sequel.pg_jsonb([]).to_a.must_be_kind_of(Array)
  end

  it "should support using JSONHashBase and JSONArrayBase as bound variables" do
    @db.bound_variable_arg(1, nil).must_equal 1
    @db.bound_variable_arg(Sequel.pg_json([1]), nil).must_equal '[1]'
    @db.bound_variable_arg(Sequel.pg_json('a'=>'b'), nil).must_equal '{"a":"b"}'
    @db.bound_variable_arg(Sequel.pg_jsonb([1]), nil).must_equal '[1]'
    @db.bound_variable_arg(Sequel.pg_jsonb('a'=>'b'), nil).must_equal '{"a":"b"}'
  end

  it "should support using json[] and jsonb[] types in bound variables" do
    @db.bound_variable_arg(Sequel.pg_array([Sequel.pg_json([{"a"=>1}]), Sequel.pg_json("b"=>[1, 2])]), nil).must_equal '{"[{\\"a\\":1}]","{\\"b\\":[1,2]}"}'
    @db.bound_variable_arg(Sequel.pg_array([Sequel.pg_jsonb([{"a"=>1}]), Sequel.pg_jsonb("b"=>[1, 2])]), nil).must_equal '{"[{\\"a\\":1}]","{\\"b\\":[1,2]}"}'
  end

  it "should parse json type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'json'}]
    @db.schema(:items).map{|e| e[1][:type]}.must_equal [:integer, :json]
  end

  it "should parse json type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'jsonb'}]
    @db.schema(:items).map{|e| e[1][:type]}.must_equal [:integer, :jsonb]
  end

  it "should support typecasting for the json type" do
    h = Sequel.pg_json(1=>2)
    a = Sequel.pg_json([1])
    @db.typecast_value(:json, h).object_id.must_equal(h.object_id)
    @db.typecast_value(:json, h.to_hash).must_equal h
    @db.typecast_value(:json, h.to_hash).class.must_equal(@hc)
    @db.typecast_value(:json, Sequel.pg_jsonb(h)).must_equal h
    @db.typecast_value(:json, Sequel.pg_jsonb(h)).class.must_equal(@hc)
    @db.typecast_value(:json, a).object_id.must_equal(a.object_id)
    @db.typecast_value(:json, a.to_a).must_equal a
    @db.typecast_value(:json, a.to_a).class.must_equal(@ac)
    @db.typecast_value(:json, Sequel.pg_jsonb(a)).must_equal a
    @db.typecast_value(:json, Sequel.pg_jsonb(a)).class.must_equal(@ac)
    @db.typecast_value(:json, '[]').must_equal Sequel.pg_json([])
    @db.typecast_value(:json, '[]').class.must_equal(@ac)
    @db.typecast_value(:json, '{"a": "b"}').must_equal Sequel.pg_json("a"=>"b")
    @db.typecast_value(:json, '{"a": "b"}').class.must_equal(@hc)
    proc{@db.typecast_value(:json, '')}.must_raise(Sequel::InvalidValue)
    proc{@db.typecast_value(:json, 1)}.must_raise(Sequel::InvalidValue)
  end

  it "should support typecasting for the jsonb type" do
    h = Sequel.pg_jsonb(1=>2)
    a = Sequel.pg_jsonb([1])
    @db.typecast_value(:jsonb, h).object_id.must_equal(h.object_id)
    @db.typecast_value(:jsonb, h.to_hash).must_equal h
    @db.typecast_value(:jsonb, h.to_hash).class.must_equal(@bhc)
    @db.typecast_value(:jsonb, Sequel.pg_json(h)).must_equal h
    @db.typecast_value(:jsonb, Sequel.pg_json(h)).class.must_equal(@bhc)
    @db.typecast_value(:jsonb, a).object_id.must_equal(a.object_id)
    @db.typecast_value(:jsonb, a.to_a).must_equal a
    @db.typecast_value(:jsonb, a.to_a).class.must_equal(@bac)
    @db.typecast_value(:jsonb, Sequel.pg_json(a)).must_equal a
    @db.typecast_value(:jsonb, Sequel.pg_json(a)).class.must_equal(@bac)
    @db.typecast_value(:jsonb, '[]').must_equal Sequel.pg_jsonb([])
    @db.typecast_value(:jsonb, '[]').class.must_equal(@bac)
    @db.typecast_value(:jsonb, '{"a": "b"}').must_equal Sequel.pg_jsonb("a"=>"b")
    @db.typecast_value(:jsonb, '{"a": "b"}').class.must_equal(@bhc)
    proc{@db.typecast_value(:jsonb, '')}.must_raise(Sequel::InvalidValue)
    proc{@db.typecast_value(:jsonb, 1)}.must_raise(Sequel::InvalidValue)
  end

  it "should return correct results for Database#schema_type_class" do
    @db.schema_type_class(:json).must_equal [Sequel::Postgres::JSONHash, Sequel::Postgres::JSONArray]
    @db.schema_type_class(:jsonb).must_equal [Sequel::Postgres::JSONBHash, Sequel::Postgres::JSONBArray]
    @db.schema_type_class(:integer).must_equal Integer
  end
end
