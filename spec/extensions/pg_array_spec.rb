require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

begin
  Sequel.extension :pg_array
rescue LoadError => e
  skip_warn "can't load pg_array extension (#{e.class}: #{e})"
else
describe "pg_array extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.extend(Module.new{def bound_variable_arg(arg, conn) arg end})
    @m = Sequel::Postgres
    @convertor = @m::PG_TYPES
  end

  it "should parse single dimensional text arrays" do
    c = @convertor[1009]
    c.call("{a}").to_a.first.should be_a_kind_of(String)
    c.call("{}").to_a.should == []
    c.call("{a}").to_a.should == ['a']
    c.call('{"a b"}').to_a.should == ['a b']
    c.call('{a,b}').to_a.should == ['a', 'b']
  end

  it "should parse multi-dimensional text arrays" do
    c = @convertor[1009]
    c.call("{{}}").to_a.should == [[]]
    c.call("{{a},{b}}").to_a.should == [['a'], ['b']]
    c.call('{{"a b"},{c}}').to_a.should == [['a b'], ['c']]
    c.call('{{{a},{b}},{{c},{d}}}').to_a.should == [[['a'], ['b']], [['c'], ['d']]]
    c.call('{{{a,e},{b,f}},{{c,g},{d,h}}}').to_a.should == [[['a', 'e'], ['b', 'f']], [['c', 'g'], ['d', 'h']]]
  end

  it "should parse text arrays with embedded deliminaters" do
    c = @convertor[1009]
    c.call('{{"{},","\\",\\,\\\\\\"\\""}}').to_a.should == [['{},', '",,\\""']]
  end

  it "should parse single dimensional integer arrays" do
    c = @convertor[1007]
    c.call("{1}").to_a.first.should be_a_kind_of(Integer)
    c.call("{}").to_a.should == []
    c.call("{1}").to_a.should == [1]
    c.call('{2,3}').to_a.should == [2, 3]
    c.call('{3,4,5}').to_a.should == [3, 4, 5]
  end

  it "should parse multiple dimensional integer arrays" do
    c = @convertor[1007]
    c.call("{{}}").to_a.should == [[]]
    c.call("{{1}}").to_a.should == [[1]]
    c.call('{{2},{3}}').to_a.should == [[2], [3]]
    c.call('{{{1,2},{3,4}},{{5,6},{7,8}}}').to_a.should == [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
  end

  it "should parse single dimensional float arrays" do
    c = @convertor[1022]
    c.call("{}").to_a.should == []
    c.call("{1.5}").to_a.should == [1.5]
    c.call('{2.5,3.5}').to_a.should == [2.5, 3.5]
    c.call('{3.5,4.5,5.5}').to_a.should == [3.5, 4.5, 5.5]
  end

  it "should parse multiple dimensional float arrays" do
    c = @convertor[1022]
    c.call("{{}}").to_a.should == [[]]
    c.call("{{1.5}}").to_a.should == [[1.5]]
    c.call('{{2.5},{3.5}}').to_a.should == [[2.5], [3.5]]
    c.call('{{{1.5,2.5},{3.5,4.5}},{{5.5,6.5},{7.5,8.5}}}').to_a.should == [[[1.5, 2.5], [3.5, 4.5]], [[5.5, 6.5], [7.5, 8.5]]]
  end

  it "should parse integers in float arrays as floats" do
    c = @convertor[1022]
    c.call("{1}").to_a.first.should be_a_kind_of(Float)
    c.call("{1}").to_a.should == [1.0]
    c.call('{{{1,2},{3,4}},{{5,6},{7,8}}}').to_a.should == [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]
  end

  it "should parse single dimensional decimal arrays" do
    c = @convertor[1231]
    c.call("{}").to_a.should == []
    c.call("{1.5}").to_a.should == [BigDecimal.new('1.5')]
    c.call('{2.5,3.5}').to_a.should == [BigDecimal.new('2.5'), BigDecimal.new('3.5')]
    c.call('{3.5,4.5,5.5}').to_a.should == [BigDecimal.new('3.5'), BigDecimal.new('4.5'), BigDecimal.new('5.5')]
  end

  it "should parse multiple dimensional decimal arrays" do
    c = @convertor[1231]
    c.call("{{}}").to_a.should == [[]]
    c.call("{{1.5}}").to_a.should == [[BigDecimal.new('1.5')]]
    c.call('{{2.5},{3.5}}').to_a.should == [[BigDecimal.new('2.5')], [BigDecimal.new('3.5')]]
    c.call('{{{1.5,2.5},{3.5,4.5}},{{5.5,6.5},{7.5,8.5}}}').to_a.should == [[[BigDecimal.new('1.5'), BigDecimal.new('2.5')], [BigDecimal.new('3.5'), BigDecimal.new('4.5')]], [[BigDecimal.new('5.5'), BigDecimal.new('6.5')], [BigDecimal.new('7.5'), BigDecimal.new('8.5')]]]
  end

  it "should parse decimal values with arbitrary precision" do
    c = @convertor[1231]
    c.call("{1.000000000000000000005}").to_a.should == [BigDecimal.new('1.000000000000000000005')]
    c.call("{{1.000000000000000000005,2.000000000000000000005},{3.000000000000000000005,4.000000000000000000005}}").to_a.should == [[BigDecimal.new('1.000000000000000000005'), BigDecimal.new('2.000000000000000000005')], [BigDecimal.new('3.000000000000000000005'), BigDecimal.new('4.000000000000000000005')]]
  end

  it "should parse integers in decimal arrays as BigDecimals" do
    c = @convertor[1231]
    c.call("{1}").to_a.first.should be_a_kind_of(BigDecimal)
    c.call("{1}").to_a.should == [BigDecimal.new('1')]
    c.call('{{{1,2},{3,4}},{{5,6},{7,8}}}').to_a.should == [[[BigDecimal.new('1'), BigDecimal.new('2')], [BigDecimal.new('3'), BigDecimal.new('4')]], [[BigDecimal.new('5'), BigDecimal.new('6')], [BigDecimal.new('7'), BigDecimal.new('8')]]]
  end

  it "should parse arrays with NULL values" do
    @convertor.values_at(1007, 1009, 1022, 1231).each do |c|
      c.call("{NULL}").should == [nil]
      c.call("{NULL,NULL}").should == [nil,nil]
      c.call("{{NULL,NULL},{NULL,NULL}}").should == [[nil,nil],[nil,nil]]
    end
  end

  it 'should parse arrays with "NULL" values' do
    c = @convertor[1009]
    c.call('{NULL,"NULL",NULL}').to_a.should == [nil, "NULL", nil]
    c.call('{NULLA,"NULL",NULL}').to_a.should == ["NULLA", "NULL", nil]
  end

  it "should literalize arrays without types correctly" do
    @db.literal(@m::PGArray.new([])).should == 'ARRAY[]'
    @db.literal(@m::PGArray.new([1])).should == 'ARRAY[1]'
    @db.literal(@m::PGArray.new([nil])).should == 'ARRAY[NULL]'
    @db.literal(@m::PGArray.new([nil, 1])).should == 'ARRAY[NULL,1]'
    @db.literal(@m::PGArray.new([1.0, 2.5])).should == 'ARRAY[1.0,2.5]'
    @db.literal(@m::PGArray.new([BigDecimal.new('1'), BigDecimal.new('2.000000000000000000005')])).should == 'ARRAY[1.0,2.000000000000000000005]'
    @db.literal(@m::PGArray.new([nil, "NULL"])).should == "ARRAY[NULL,'NULL']"
    @db.literal(@m::PGArray.new([nil, "{},[]'\""])).should == "ARRAY[NULL,'{},[]''\"']"
  end

  it "should literalize multidimensional arrays correctly" do
    @db.literal(@m::PGArray.new([[]])).should == 'ARRAY[[]]'
    @db.literal(@m::PGArray.new([[1, 2]])).should == 'ARRAY[[1,2]]'
    @db.literal(@m::PGArray.new([[3], [5]])).should == 'ARRAY[[3],[5]]'
    @db.literal(@m::PGArray.new([[[1.0]], [[2.5]]])).should == 'ARRAY[[[1.0]],[[2.5]]]'
    @db.literal(@m::PGArray.new([[[["NULL"]]]])).should == "ARRAY[[[['NULL']]]]"
    @db.literal(@m::PGArray.new([["a", "b"], ["{},[]'\"", nil]])).should == "ARRAY[['a','b'],['{},[]''\"',NULL]]"
  end

  it "should literalize with types correctly" do
    @db.literal(@m::PGArray.new([1], :int4)).should == 'ARRAY[1]::int4[]'
    @db.literal(@m::PGArray.new([nil], :text)).should == 'ARRAY[NULL]::text[]'
    @db.literal(@m::PGArray.new([nil, 1], :int8)).should == 'ARRAY[NULL,1]::int8[]'
    @db.literal(@m::PGArray.new([1.0, 2.5], :real)).should == 'ARRAY[1.0,2.5]::real[]'
    @db.literal(@m::PGArray.new([BigDecimal.new('1'), BigDecimal.new('2.000000000000000000005')], :decimal)).should == 'ARRAY[1.0,2.000000000000000000005]::decimal[]'
    @db.literal(@m::PGArray.new([nil, "NULL"], :varchar)).should == "ARRAY[NULL,'NULL']::varchar[]"
    @db.literal(@m::PGArray.new([nil, "{},[]'\""], :"varchar(255)")).should == "ARRAY[NULL,'{},[]''\"']::varchar(255)[]"
  end

  it "should have Array#pg_array method for easy PGArray creation" do
    @db.literal([1].pg_array).should == 'ARRAY[1]'
    @db.literal([1, 2].pg_array(:int4)).should == 'ARRAY[1,2]::int4[]'
    @db.literal([[[1], [2]], [[3], [4]]].pg_array(:real)).should == 'ARRAY[[[1],[2]],[[3],[4]]]::real[]'
  end

  it "should support using arrays as bound variables" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg([1,2].pg_array, nil).should == '{1,2}'
    @db.bound_variable_arg([1,2], nil).should == '{1,2}'
    @db.bound_variable_arg([[1,2]], nil).should == '{{1,2}}'
    @db.bound_variable_arg([1.0,2.0], nil).should == '{1.0,2.0}'
    @db.bound_variable_arg([Sequel.lit('a'), Sequel.blob("a\0'\"")], nil).should == '{a,"a\\\\000\\\\047\\""}'
    @db.bound_variable_arg(["\\ \"", 'NULL', nil], nil).should == '{"\\\\ \\"","NULL",NULL}'
  end

  it "should parse array types from the schema correctly" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'integer[]'}, {:name=>'f', :db_type=>'real[]'}, {:name=>'d', :db_type=>'numeric[]'}, {:name=>'t', :db_type=>'text[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :integer_array, :float_array, :decimal_array, :string_array]
  end

  it "should support typecasting of the various array types" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods
    a = [1, 2]
    o = a.pg_array
    {:integer=>:integer, :float=>'double precision', :decimal=>'numeric', :string=>:text}.each do |x, y|
      v = @db.typecast_value(:"#{x}_array", o)
      v.should == o
      v.array_type.should_not be_nil
      @db.typecast_value(:"#{x}_array", v).should equal(v)
      @db.typecast_value(:"#{x}_array", a).should == a
      @db.literal(@db.typecast_value(:"#{x}_array", a)).should == "ARRAY[1,2]::#{y}[]"
      @db.typecast_value(:"#{x}_array", '{}').should == []
      @db.literal(@db.typecast_value(:"#{x}_array", '{}')).should == "ARRAY[]::#{y}[]"
    end
    @db.typecast_value(:integer_array, '{1}').should == [1]
    @db.typecast_value(:float_array, '{1}').should == [1.0]
    @db.typecast_value(:decimal_array, '{1}').should == [BigDecimal.new('1')]
    @db.typecast_value(:string_array, '{1}').should == ['1']
    proc{@db.typecast_value(:integer_array, {})}.should raise_error(Sequel::InvalidValue)
  end

  it "should support registering custom array types" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods

    Sequel::Postgres::PGArray.register('foo')
    @db.typecast_value(:foo_array, []).should be_a_kind_of(Sequel::Postgres::PGArray)
    @db.fetch = [{:name=>'id', :db_type=>'foo[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:foo_array]
  end

  it "should support registering custom types with :type_symbol option" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods

    Sequel::Postgres::PGArray.register('foo', :type_symbol=>:bar)
    @db.typecast_value(:bar_array, []).should be_a_kind_of(Sequel::Postgres::PGArray)
    @db.fetch = [{:name=>'id', :db_type=>'foo[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:bar_array]
  end

  it "should support using a block as a custom conversion proc given as block" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods

    Sequel::Postgres::PGArray.register('foo'){|s| (s*2).to_i}
    @db.typecast_value(:foo_array, '{1}').should == [11]
  end

  it "should support using a block as a custom conversion proc given as :converter option" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods

    Sequel::Postgres::PGArray.register('foo', :converter=>proc{|s| (s*2).to_i})
    @db.typecast_value(:foo_array, '{1}').should == [11]
  end

  it "should support using an existing scaler conversion proc via the :scalar_oid option" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods

    Sequel::Postgres::PGArray.register('foo', :scalar_oid=>16)
    @db.typecast_value(:foo_array, '{"t"}').should == [true]
  end

  it "should raise an error if using :scalar_oid option with unexisting scalar conversion proc" do
    proc{Sequel::Postgres::PGArray.register('foo', :scalar_oid=>0)}.should raise_error(Sequel::Error)
  end

  it "should raise an error if using :converter option and a block argument" do
    proc{Sequel::Postgres::PGArray.register('foo', :converter=>proc{}){}}.should raise_error(Sequel::Error)
  end

  it "should raise an error if using :scalar_oid option and a block argument" do
    proc{Sequel::Postgres::PGArray.register('foo', :scalar_oid=>16){}}.should raise_error(Sequel::Error)
  end

  it "should support registering custom types with :oid option" do
    Sequel::Postgres::PGArray.register('foo', :oid=>1)
    Sequel::Postgres::PG_TYPES[1].call('{1}').should be_a_kind_of(Sequel::Postgres::PGArray)
  end

  it "should support registering custom types with :parser=>:json option" do
    Sequel::Postgres::PGArray.register('foo', :oid=>2, :parser=>:json)
    Sequel::Postgres::PG_TYPES[2].should be_a_kind_of(Sequel::Postgres::PGArray::JSONCreator)
  end

  it "should support registering convertors with :parser=>:json option" do
    Sequel::Postgres::PGArray.register('foo', :oid=>4, :parser=>:json){|s| s * 2}
    Sequel::Postgres::PG_TYPES[4].call('{{1, 2}, {3, 4}}').should == [[2, 4], [6, 8]]
  end

  it "should support registering custom types with :array_type option" do
    Sequel::Postgres::PGArray.register('foo', :oid=>3, :array_type=>:blah)
    @db.literal(Sequel::Postgres::PG_TYPES[3].call('{}')).should == 'ARRAY[]::blah[]'
  end

  it "should use and not override existing database typecast method if :typecast_method option is given" do
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods
    Sequel::Postgres::PGArray.register('foo', :typecast_method=>:float)
    @db.fetch = [{:name=>'id', :db_type=>'foo[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:float_array]
  end

  it "should set appropriate timestamp conversion procs when getting conversion procs" do
    @db.extend(Module.new{def get_conversion_procs(conn) {} end})
    @db.extend Sequel::Postgres::PGArray::DatabaseMethods
    procs = @db.send(:get_conversion_procs, nil)
    procs[1185].call('{"2011-10-20 11:12:13"}').should == [Time.local(2011, 10, 20, 11, 12, 13)]
    procs[1115].call('{"2011-10-20 11:12:13"}').should == [Time.local(2011, 10, 20, 11, 12, 13)]
  end
end
end
