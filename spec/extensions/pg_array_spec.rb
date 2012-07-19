require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_array extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.extend(Module.new{def bound_variable_arg(arg, conn) arg end; def get_conversion_procs(conn) {} end})
    @db.extend_datasets(Module.new{def supports_timestamp_timezones?; false; end; def supports_timestamp_usecs?; false; end})
    @db.extension(:pg_array)
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

  it "should have Sequel.pg_array method for easy PGArray creation" do
    @db.literal(Sequel.pg_array([1])).should == 'ARRAY[1]'
    @db.literal(Sequel.pg_array([1, 2], :int4)).should == 'ARRAY[1,2]::int4[]'
    @db.literal(Sequel.pg_array([[[1], [2]], [[3], [4]]], :real)).should == 'ARRAY[[[1],[2]],[[3],[4]]]::real[]'
  end

  it "should have Sequel.pg_array return existing PGArrays as-is" do
    a = Sequel.pg_array([1])
    Sequel.pg_array(a).should equal(a)
  end

  it "should have Sequel.pg_array create a new PGArrays if type of existing does not match" do
    a = Sequel.pg_array([1], :int4)
    b = Sequel.pg_array(a, :int8)
    a.should == b
    a.should_not equal(b)
    a.array_type.should == :int4
    b.array_type.should == :int8
  end

  it "should support using arrays as bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg(Sequel.pg_array([1,2]), nil).should == '{1,2}'
    @db.bound_variable_arg([1,2], nil).should == '{1,2}'
    @db.bound_variable_arg([[1,2]], nil).should == '{{1,2}}'
    @db.bound_variable_arg([1.0,2.0], nil).should == '{1.0,2.0}'
    @db.bound_variable_arg([Sequel.lit('a'), Sequel.blob("a\0'\"")], nil).should == '{a,"a\\\\000\\\\047\\""}'
    @db.bound_variable_arg(["\\ \"", 'NULL', nil], nil).should == '{"\\\\ \\"","NULL",NULL}'
  end

  it "should parse array types from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i', :db_type=>'integer[]'}, {:name=>'f', :db_type=>'real[]'}, {:name=>'d', :db_type=>'numeric[]'}, {:name=>'t', :db_type=>'text[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :integer_array, :float_array, :decimal_array, :string_array]
  end

  it "should support typecasting of the various array types" do
    {
      :integer=>{:class=>Integer, :convert=>['1', '1', 1, '1']},
      :float=>{:db_type=>'double precision',  :class=>Float, :convert=>['1.1', '1.1', 1.1, '1.1']},
      :decimal=>{:db_type=>'numeric', :class=>BigDecimal, :convert=>['1.00000000000000000000000001', '1.00000000000000000000000001', BigDecimal.new('1.00000000000000000000000001'), '1.00000000000000000000000001']},
      :string=>{:db_type=>'text', :class=>String, :convert=>['1', 1, '1', "'1'"]},
      :bigint=>{:class=>Integer, :convert=>['1', '1', 1, '1']},
      :boolean=>{:class=>TrueClass, :convert=>['t', 't', true, 'true']},
      :blob=>{:db_type=>'bytea', :class=>Sequel::SQL::Blob, :convert=>['1', '1', '1', "'1'"]},
      :date=>{:class=>Date, :convert=>['2011-10-12', '2011-10-12', Date.new(2011, 10, 12), "'2011-10-12'"]},
      :time=>{:db_type=>'time without time zone', :class=>Sequel::SQLTime, :convert=>['01:02:03', '01:02:03', Sequel::SQLTime.create(1, 2, 3), "'01:02:03'"]},
      :datetime=>{:db_type=>'timestamp without time zone', :class=>Time, :convert=>['2011-10-12 01:02:03', '2011-10-12 01:02:03', Time.local(2011, 10, 12, 1, 2, 3), "'2011-10-12 01:02:03'"]},
      :time_timezone=>{:db_type=>'time with time zone', :class=>Sequel::SQLTime, :convert=>['01:02:03', '01:02:03', Sequel::SQLTime.create(1, 2, 3), "'01:02:03'"]},
      :datetime_timezone=>{:db_type=>'timestamp with time zone', :class=>Time, :convert=>['2011-10-12 01:02:03', '2011-10-12 01:02:03', Time.local(2011, 10, 12, 1, 2, 3), "'2011-10-12 01:02:03'"]},
    }.each do |type, h|
      meth = :"#{type}_array"
      db_type = h[:db_type]||type
      klass = h[:class]
      text_in, array_in, value, output = h[:convert]

      ["{#{text_in}}", [array_in]].each do |input|
        v = @db.typecast_value(meth, input)
        v.should == [value]
        v.first.should be_a_kind_of(klass)
        v.array_type.should_not be_nil
        @db.typecast_value(meth, Sequel.pg_array([value])).should == v
        @db.typecast_value(meth, v).should equal(v)
      end

      ["{{#{text_in}}}", [[array_in]]].each do |input|
        v = @db.typecast_value(meth, input)
        v.should == [[value]]
        v.first.first.should be_a_kind_of(klass)
        v.array_type.should_not be_nil
        @db.typecast_value(meth, Sequel.pg_array([[value]])).should == v
        @db.typecast_value(meth, v).should equal(v)
      end

      @db.literal(@db.typecast_value(meth, [array_in])).should == "ARRAY[#{output}]::#{db_type}[]"
      @db.typecast_value(meth, '{}').should == []
      @db.literal(@db.typecast_value(meth, '{}')).should == "ARRAY[]::#{db_type}[]"
    end
    proc{@db.typecast_value(:integer_array, {})}.should raise_error(Sequel::InvalidValue)
  end

  it "should support registering custom array types" do
    Sequel::Postgres::PGArray.register('foo')
    @db.typecast_value(:foo_array, []).should be_a_kind_of(Sequel::Postgres::PGArray)
    @db.fetch = [{:name=>'id', :db_type=>'foo[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:foo_array]
  end

  it "should support registering custom types with :type_symbol option" do
    Sequel::Postgres::PGArray.register('foo', :type_symbol=>:bar)
    @db.typecast_value(:bar_array, []).should be_a_kind_of(Sequel::Postgres::PGArray)
    @db.fetch = [{:name=>'id', :db_type=>'foo[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:bar_array]
  end

  it "should support using a block as a custom conversion proc given as block" do
    Sequel::Postgres::PGArray.register('foo'){|s| (s*2).to_i}
    @db.typecast_value(:foo_array, '{1}').should == [11]
  end

  it "should support using a block as a custom conversion proc given as :converter option" do
    Sequel::Postgres::PGArray.register('foo', :converter=>proc{|s| (s*2).to_i})
    @db.typecast_value(:foo_array, '{1}').should == [11]
  end

  it "should support using an existing scaler conversion proc via the :scalar_oid option" do
    Sequel::Postgres::PGArray.register('foo', :scalar_oid=>16)
    @db.typecast_value(:foo_array, '{"t"}').should == [true]
  end

  it "should support using a given conversion procs hash via the :type_procs option" do
    Sequel::Postgres::PGArray.register('foo', :scalar_oid=>16, :type_procs=>{16=>proc{|s| "!#{s}"}})
    @db.typecast_value(:foo_array, '{"t"}').should == ["!t"]
  end

  it "should support adding methods to the given module via the :typecast_methods_module option" do
    m = Module.new
    Sequel::Postgres::PGArray.register('foo15', :scalar_oid=>16, :typecast_methods_module=>m)
    @db.typecast_value(:foo15_array, '{"t"}').should == '{"t"}'
    @db.extend(m)
    @db.typecast_value(:foo15_array, '{"t"}').should == [true]
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
    Sequel::Postgres::PGArray.register('foo', :typecast_method=>:float)
    @db.fetch = [{:name=>'id', :db_type=>'foo[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:float_array]
  end

  it "should set appropriate timestamp conversion procs when getting conversion procs" do
    procs = @db.send(:get_conversion_procs, nil)
    procs[1185].call('{"2011-10-20 11:12:13"}').should == [Time.local(2011, 10, 20, 11, 12, 13)]
    procs[1115].call('{"2011-10-20 11:12:13"}').should == [Time.local(2011, 10, 20, 11, 12, 13)]
  end
end
