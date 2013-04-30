require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_json extension" do
  before(:all) do
    m = Sequel::Postgres
    @m = m::JSONDatabaseMethods
    @hc = m::JSONHash
    @ac = m::JSONArray

    # Create subclass in correct namespace for easily overriding methods
    j = m::JSON = JSON.dup
    j.instance_eval do
      Parser = JSON::Parser
      alias old_parse parse
      def parse(s)
        return 1 if s == '1'
        old_parse(s) 
      end
    end
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

  it "should raise an error when attempting to parse invalid json" do
    proc{@m.parse_json('')}.should raise_error(Sequel::InvalidValue)
    proc{@m.parse_json('1')}.should raise_error(Sequel::InvalidValue)

    begin
      Sequel.instance_eval do
        alias pj parse_json
        def parse_json(v)
          v
        end
      end
      proc{@m.parse_json('1')}.should raise_error(Sequel::InvalidValue)
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

  it "should have Sequel.pg_json return JSONHash and JSONArray as is" do
    a = Sequel.pg_json({})
    Sequel.pg_json(a).should equal(a)
    a = Sequel.pg_json([])
    Sequel.pg_json(a).should equal(a)
  end

  it "should have Sequel.pg_json raise an Error if called with a non-hash or array" do
    proc{Sequel.pg_json(:a)}.should raise_error(Sequel::Error)
  end

  it "should have JSONHash#to_hash method for getting underlying hash" do
    Sequel.pg_json({}).to_hash.should be_a_kind_of(Hash)
  end

  it "should have JSONArray#to_a method for getting underlying array" do
    Sequel.pg_json([]).to_a.should be_a_kind_of(Array)
  end

  it "should support using JSONHash and JSONArray as bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg(Sequel.pg_json([1]), nil).should == '[1]'
    @db.bound_variable_arg(Sequel.pg_json('a'=>'b'), nil).should == '{"a":"b"}'
  end

  it "should support using json[] types in bound variables" do
    @db.bound_variable_arg(Sequel.pg_array([Sequel.pg_json([{"a"=>1}]), Sequel.pg_json("b"=>[1, 2])]), nil).should == '{"[{\\"a\\":1}]","{\\"b\\":[1,2]}"}'
  end

  it "should parse json type from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'json'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :json]
  end

  it "should support typecasting for the json type" do
    h = Sequel.pg_json(1=>2)
    a = Sequel.pg_json([1])
    @db.typecast_value(:json, h).should equal(h)
    @db.typecast_value(:json, h.to_hash).should == h
    @db.typecast_value(:json, h.to_hash).should be_a_kind_of(@hc)
    @db.typecast_value(:json, a).should equal(a)
    @db.typecast_value(:json, a.to_a).should == a
    @db.typecast_value(:json, a.to_a).should be_a_kind_of(@ac)
    @db.typecast_value(:json, '[]').should == Sequel.pg_json([])
    @db.typecast_value(:json, '{"a": "b"}').should == Sequel.pg_json("a"=>"b")
    proc{@db.typecast_value(:json, '')}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:json, 1)}.should raise_error(Sequel::InvalidValue)
  end

  it "should return correct results for Database#schema_type_class" do
    @db.schema_type_class(:json).should == [Sequel::Postgres::JSONHash, Sequel::Postgres::JSONArray]
    @db.schema_type_class(:integer).should == Integer
  end
end
