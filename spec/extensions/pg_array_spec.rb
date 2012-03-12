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
  end

  it "should parse single dimensional text arrays" do
    @m::PGStringArray.parse("{a}").to_a.first.should be_a_kind_of(String)
    @m::PGStringArray.parse("{}").to_a.should == []
    @m::PGStringArray.parse("{a}").to_a.should == ['a']
    @m::PGStringArray.parse('{"a b"}').to_a.should == ['a b']
    @m::PGStringArray.parse('{a,b}').to_a.should == ['a', 'b']
  end

  it "should parse multi-dimensional text arrays" do
    @m::PGStringArray.parse("{{}}").to_a.should == [[]]
    @m::PGStringArray.parse("{{a},{b}}").to_a.should == [['a'], ['b']]
    @m::PGStringArray.parse('{{"a b"},{c}}').to_a.should == [['a b'], ['c']]
    @m::PGStringArray.parse('{{{a},{b}},{{c},{d}}}').to_a.should == [[['a'], ['b']], [['c'], ['d']]]
    @m::PGStringArray.parse('{{{a,e},{b,f}},{{c,g},{d,h}}}').to_a.should == [[['a', 'e'], ['b', 'f']], [['c', 'g'], ['d', 'h']]]
  end

  it "should parse text arrays with embedded deliminaters" do
    @m::PGStringArray.parse('{{"{},","\\",\\,\\\\\\"\\""}}').to_a.should == [['{},', '",,\\""']]
  end

  it "should parse single dimensional integer arrays" do
    @m::PGIntegerArray.parse("{1}").to_a.first.should be_a_kind_of(Integer)
    @m::PGIntegerArray.parse("{}").to_a.should == []
    @m::PGIntegerArray.parse("{1}").to_a.should == [1]
    @m::PGIntegerArray.parse('{2,3}').to_a.should == [2, 3]
    @m::PGIntegerArray.parse('{3,4,5}').to_a.should == [3, 4, 5]
  end

  it "should parse multiple dimensional integer arrays" do
    @m::PGIntegerArray.parse("{{}}").to_a.should == [[]]
    @m::PGIntegerArray.parse("{{1}}").to_a.should == [[1]]
    @m::PGIntegerArray.parse('{{2},{3}}').to_a.should == [[2], [3]]
    @m::PGIntegerArray.parse('{{{1,2},{3,4}},{{5,6},{7,8}}}').to_a.should == [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
  end

  it "should parse single dimensional float arrays" do
    @m::PGFloatArray.parse("{}").to_a.should == []
    @m::PGFloatArray.parse("{1.5}").to_a.should == [1.5]
    @m::PGFloatArray.parse('{2.5,3.5}').to_a.should == [2.5, 3.5]
    @m::PGFloatArray.parse('{3.5,4.5,5.5}').to_a.should == [3.5, 4.5, 5.5]
  end

  it "should parse multiple dimensional float arrays" do
    @m::PGFloatArray.parse("{{}}").to_a.should == [[]]
    @m::PGFloatArray.parse("{{1.5}}").to_a.should == [[1.5]]
    @m::PGFloatArray.parse('{{2.5},{3.5}}').to_a.should == [[2.5], [3.5]]
    @m::PGFloatArray.parse('{{{1.5,2.5},{3.5,4.5}},{{5.5,6.5},{7.5,8.5}}}').to_a.should == [[[1.5, 2.5], [3.5, 4.5]], [[5.5, 6.5], [7.5, 8.5]]]
  end

  it "should parse integers in float arrays as floats" do
    @m::PGFloatArray.parse("{1}").to_a.first.should be_a_kind_of(Float)
    @m::PGFloatArray.parse("{1}").to_a.should == [1.0]
    @m::PGFloatArray.parse('{{{1,2},{3,4}},{{5,6},{7,8}}}').to_a.should == [[[1.0, 2.0], [3.0, 4.0]], [[5.0, 6.0], [7.0, 8.0]]]
  end

  it "should parse single dimensional decimal arrays" do
    @m::PGDecimalArray.parse("{}").to_a.should == []
    @m::PGDecimalArray.parse("{1.5}").to_a.should == [BigDecimal.new('1.5')]
    @m::PGDecimalArray.parse('{2.5,3.5}').to_a.should == [BigDecimal.new('2.5'), BigDecimal.new('3.5')]
    @m::PGDecimalArray.parse('{3.5,4.5,5.5}').to_a.should == [BigDecimal.new('3.5'), BigDecimal.new('4.5'), BigDecimal.new('5.5')]
  end

  it "should parse multiple dimensional decimal arrays" do
    @m::PGDecimalArray.parse("{{}}").to_a.should == [[]]
    @m::PGDecimalArray.parse("{{1.5}}").to_a.should == [[BigDecimal.new('1.5')]]
    @m::PGDecimalArray.parse('{{2.5},{3.5}}').to_a.should == [[BigDecimal.new('2.5')], [BigDecimal.new('3.5')]]
    @m::PGDecimalArray.parse('{{{1.5,2.5},{3.5,4.5}},{{5.5,6.5},{7.5,8.5}}}').to_a.should == [[[BigDecimal.new('1.5'), BigDecimal.new('2.5')], [BigDecimal.new('3.5'), BigDecimal.new('4.5')]], [[BigDecimal.new('5.5'), BigDecimal.new('6.5')], [BigDecimal.new('7.5'), BigDecimal.new('8.5')]]]
  end

  it "should parse decimal values with arbitrary precision" do
    @m::PGDecimalArray.parse("{1.000000000000000000005}").to_a.should == [BigDecimal.new('1.000000000000000000005')]
    @m::PGDecimalArray.parse("{{1.000000000000000000005,2.000000000000000000005},{3.000000000000000000005,4.000000000000000000005}}").to_a.should == [[BigDecimal.new('1.000000000000000000005'), BigDecimal.new('2.000000000000000000005')], [BigDecimal.new('3.000000000000000000005'), BigDecimal.new('4.000000000000000000005')]]
  end

  it "should parse integers in decimal arrays as BigDecimals" do
    @m::PGDecimalArray.parse("{1}").to_a.first.should be_a_kind_of(BigDecimal)
    @m::PGDecimalArray.parse("{1}").to_a.should == [BigDecimal.new('1')]
    @m::PGDecimalArray.parse('{{{1,2},{3,4}},{{5,6},{7,8}}}').to_a.should == [[[BigDecimal.new('1'), BigDecimal.new('2')], [BigDecimal.new('3'), BigDecimal.new('4')]], [[BigDecimal.new('5'), BigDecimal.new('6')], [BigDecimal.new('7'), BigDecimal.new('8')]]]
  end

  it "should parse arrays with NULL values" do
    [@m::PGStringArray, @m::PGIntegerArray, @m::PGFloatArray, @m::PGDecimalArray].each do |c|
      c.parse("{NULL}").should == [nil]
      c.parse("{NULL,NULL}").should == [nil,nil]
      c.parse("{{NULL,NULL},{NULL,NULL}}").should == [[nil,nil],[nil,nil]]
    end
  end

  it 'should parse arrays with "NULL" values' do
    @m::PGStringArray.parse('{NULL,"NULL",NULL}').to_a.should == [nil, "NULL", nil]
    @m::PGStringArray.parse('{NULLA,"NULL",NULL}').to_a.should == ["NULLA", "NULL", nil]
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

  it "should have reasonable default types" do
    @db.literal(@m::PGArray.new([])).should == 'ARRAY[]'
    @db.literal(@m::PGIntegerArray.new([])).should == 'ARRAY[]::int4[]'
    @db.literal(@m::PGFloatArray.new([])).should == 'ARRAY[]::double precision[]'
    @db.literal(@m::PGStringArray.new([])).should == 'ARRAY[]::text[]'
    @db.literal(@m::PGDecimalArray.new([])).should == 'ARRAY[]::decimal[]'
  end

  it "should use varchar type for char arrays without length" do
    @db.literal(@m::PGStringArray.new([], :char)).should == 'ARRAY[]::varchar[]'
    @db.literal(@m::PGStringArray.new([], 'char')).should == 'ARRAY[]::varchar[]'
  end

  it "should use given type for char arrays with length" do
    @db.literal(@m::PGStringArray.new([], :'char(2)')).should == 'ARRAY[]::char(2)[]'
    @db.literal(@m::PGStringArray.new([], 'char(1)')).should == 'ARRAY[]::char(1)[]'
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
    %w[integer float decimal string].each do |x|
      @db.typecast_value(:"#{x}_array", o).should equal(o)
      @db.typecast_value(:"#{x}_array", a).should == a
      @db.typecast_value(:"#{x}_array", a).should be_a_kind_of(eval("Sequel::Postgres::PG#{x.capitalize}Array"))
      @db.typecast_value(:"#{x}_array", '{}').should == []
      @db.typecast_value(:"#{x}_array", '{}').should be_a_kind_of(eval("Sequel::Postgres::PG#{x.capitalize}Array"))
    end
    @db.typecast_value(:integer_array, '{1}').should == [1]
    @db.typecast_value(:float_array, '{1}').should == [1.0]
    @db.typecast_value(:decimal_array, '{1}').should == [BigDecimal.new('1')]
    @db.typecast_value(:string_array, '{1}').should == ['1']
    proc{@db.typecast_value(:integer_array, {})}.should raise_error(Sequel::InvalidValue)
  end
end
end
