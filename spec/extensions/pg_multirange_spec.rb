require_relative "spec_helper"

describe "pg_multirange extension" do
  before(:all) do
    Sequel.extension :pg_array, :pg_range, :pg_multirange
  end

  before do
    @db = Sequel.connect('mock://postgres')
    def @db.server_version(*) 140000 end
    @R = Sequel::Postgres::PGRange
    @MR = Sequel::Postgres::PGMultiRange
    @db.extend_datasets do
      def supports_timestamp_timezones?; false end
      def supports_timestamp_usecs?; false end
      def quote_identifiers?; false end
    end
    @db.extension(:pg_array, :pg_multirange)
  end

  it "should raise if loaded into a database that doesn't support multiranges" do
    @db = Sequel.connect('mock://postgres')
    def @db.server_version(*) 130000 end
    proc{@db.extension(:pg_multirange)}.must_raise Sequel::Error
  end

  it "should set up conversion procs correctly" do
    cp = @db.conversion_procs
    cp[4451].call("{[1,2],(3,4)}").must_equal @MR.new([
      @R.new(1,2, :exclude_begin=>false, :exclude_end=>false, :db_type=>'int4range'),
      @R.new(3,4, :exclude_begin=>true, :exclude_end=>true, :db_type=>'int4range'),
    ], 'int4multirange')
    cp[4532].call("{[1,2]}").must_equal @MR.new([@R.new(1,2, :exclude_begin=>false, :exclude_end=>false, :db_type=>'numrange')], 'nummultirange')
    cp[4533].call("{[2011-01-02 10:20:30,2011-02-03 10:20:30)}").must_equal @MR.new([@R.new(Time.local(2011, 1, 2, 10, 20, 30),Time.local(2011, 2, 3, 10, 20, 30), :exclude_begin=>false, :exclude_end=>true, :db_type=>'tsrange')], 'tsmultirange')
    cp[4534].call("{[2011-01-02 10:20:30,2011-02-03 10:20:30)}").must_equal @MR.new([@R.new(Time.local(2011, 1, 2, 10, 20, 30),Time.local(2011, 2, 3, 10, 20, 30), :exclude_begin=>false, :exclude_end=>true, :db_type=>'tstzrange')], 'tstzmultirange')
    cp[4535].call("{[2011-01-02,2011-02-03)}").must_equal  @MR.new([@R.new(Date.new(2011, 1, 2),Date.new(2011, 2, 3), :exclude_begin=>false, :exclude_end=>true, :db_type=>'daterange')], 'datemultirange')
    cp[4536].call("{[1,2]}").must_equal @MR.new([@R.new(1,2, :exclude_begin=>false, :exclude_end=>false, :db_type=>'int8range')], 'int8multirange')
  end

  it "should set up conversion procs for arrays correctly" do
    cp = @db.conversion_procs
    cp[6150].call("{\"{[1,2],(3,4)}\"}").must_equal [@MR.new([
      @R.new(1,2, :exclude_begin=>false, :exclude_end=>false, :db_type=>'int4range'),
      @R.new(3,4, :exclude_begin=>true, :exclude_end=>true, :db_type=>'int4range'),
    ], 'int4multirange')]
    cp[6151].call("{\"{[1,2]}\"}").must_equal [@MR.new([@R.new(1,2, :exclude_begin=>false, :exclude_end=>false, :db_type=>'numrange')], 'nummultirange')]
    cp[6152].call("{\"{[2011-01-02 10:20:30,2011-02-03 10:20:30)}\"}").must_equal [@MR.new([@R.new(Time.local(2011, 1, 2, 10, 20, 30),Time.local(2011, 2, 3, 10, 20, 30), :exclude_begin=>false, :exclude_end=>true, :db_type=>'tsrange')], 'tsmultirange')]
    cp[6153].call("{\"{[2011-01-02 10:20:30,2011-02-03 10:20:30)}\"}").must_equal [@MR.new([@R.new(Time.local(2011, 1, 2, 10, 20, 30),Time.local(2011, 2, 3, 10, 20, 30), :exclude_begin=>false, :exclude_end=>true, :db_type=>'tstzrange')], 'tstzmultirange')]
    cp[6155].call("{\"{[2011-01-02,2011-02-03)}\"}").must_equal [@MR.new([@R.new(Date.new(2011, 1, 2),Date.new(2011, 2, 3), :exclude_begin=>false, :exclude_end=>true, :db_type=>'daterange')], 'datemultirange')]
    cp[6157].call("{\"{[1,2]}\"}").must_equal [@MR.new([@R.new(1,2, :exclude_begin=>false, :exclude_end=>false, :db_type=>'int8range')], 'int8multirange')]
  end

  it "should parse empty multiranges" do
    @db.conversion_procs[4451].call("{}").must_equal @MR.new([], 'int4multirange')
  end

  it "should literalize PGMultiRange of Range instances to strings correctly" do
    @db.literal(@MR.new([], 'xmultirange')).must_equal "xmultirange()"
    @db.literal(@MR.new([Date.new(2011, 1, 2)...Date.new(2011, 3, 2), Date.new(2012, 1, 2)...Date.new(2012, 3, 2)], 'datemultirange')).must_equal "datemultirange(daterange('2011-01-02','2011-03-02','[)'), daterange('2012-01-02','2012-03-02','[)'))"
    @db.literal(@MR.new([Date.new(2011, 1, 2)...Date.new(2011, 3, 2)], 'datemultirange')).must_equal "datemultirange(daterange('2011-01-02','2011-03-02','[)'))"
    @db.literal(@MR.new([Time.local(2011, 1, 2, 10, 20, 30)...Time.local(2011, 2, 3, 10, 20, 30)], 'tsmultirange')).must_equal "tsmultirange(tsrange('2011-01-02 10:20:30','2011-02-03 10:20:30','[)'))"
    @db.literal(@MR.new([DateTime.new(2011, 1, 2, 10, 20, 30)...DateTime.new(2011, 2, 3, 10, 20, 30)], 'tsmultirange')).must_equal "tsmultirange(tsrange('2011-01-02 10:20:30','2011-02-03 10:20:30','[)'))"
    @db.literal(@MR.new([DateTime.new(2011, 1, 2, 10, 20, 30)...DateTime.new(2011, 2, 3, 10, 20, 30)], 'tstzmultirange')).must_equal "tstzmultirange(tstzrange('2011-01-02 10:20:30','2011-02-03 10:20:30','[)'))"
    @db.literal(@MR.new([1..2], 'int8multirange')).must_equal "int8multirange(int8range(1,2,'[]'))"
    @db.literal(@MR.new([1.0..2.0], 'nummultirange')).must_equal "nummultirange(numrange(1.0,2.0,'[]'))"
    @db.literal(@MR.new([BigDecimal('1.0')..BigDecimal('2.0')], 'nummultirange')).must_equal "nummultirange(numrange(1.0,2.0,'[]'))"
  end

  it "should literalize PGMultiRange of PGRange instances to strings correctly" do
    @db.literal(@MR.new([@R.new(1, 2, :db_type=>'int8range')], 'int8multirange')).must_equal "int8multirange(int8range(1,2,'[]'))"
    @db.literal(@MR.new([@R.new(1, 2, :exclude_begin=>true, :db_type=>'int8range')], 'int8multirange')).must_equal "int8multirange(int8range(1,2,'(]'))"
    @db.literal(@MR.new([@R.new(1, 2, :exclude_end=>true, :db_type=>'int8range')], 'int8multirange')).must_equal "int8multirange(int8range(1,2,'[)'))"
    @db.literal(@MR.new([@R.new(nil, nil, :empty=>true)], 'nummultirange')).must_equal "nummultirange('empty')"
    @db.literal(@MR.new([@R.new(nil, nil, :empty=>true, :db_type=>'int8range')], 'int8multirange')).must_equal "int8multirange('empty'::int8range)"
    @db.literal(@MR.new([@R.new("", 2)], 'nummultirange')).must_equal "nummultirange('[\"\",2]')"
  end

  it "should not affect literalization of custom objects" do
    o = Object.new
    def o.sql_literal(ds) 'v' end
    @db.literal(o).must_equal 'v'
  end

  it "should support using PGMultiRange of Range instances as bound variables" do
    @db.bound_variable_arg(@MR.new([], 'int4multirange'), nil).must_equal "{}"
    @db.bound_variable_arg(@MR.new([1..2], 'int4multirange'), nil).must_equal "{[1,2]}"
    @db.bound_variable_arg(@MR.new([1..2, 3...4], 'int4multirange'), nil).must_equal "{[1,2], [3,4)}"
  end

  it "should support using PGMultiRange of PGRange instances as bound variables" do
    @db.bound_variable_arg(@MR.new([@R.new(1, 2)], 'int8multirange'), nil).must_equal "{[1,2]}"
    @db.bound_variable_arg(@MR.new([@R.new(1, 2), @R.new(3, 4, :exclude_begin=>true, :exclude_end=>true)], 'int8multirange'), nil).must_equal "{[1,2], (3,4)}"
  end

  it "should support using arrays of PGMultiRanges as bound variables" do
    @db.bound_variable_arg([@MR.new([1..2], 'int4multirange'), @MR.new([@R.new(2, 3, :exclude_end=>true)], 'int4multirange')], nil).must_equal '{"{[1,2]}","{[2,3)}"}'
  end

  it "should parse multirange types from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i4', :db_type=>'int4multirange', :typtype=>'m'}, {:name=>'i8', :db_type=>'int8multirange', :typtype=>'m'}, {:name=>'n', :db_type=>'nummultirange', :typtype=>'m'}, {:name=>'d', :db_type=>'datemultirange', :typtype=>'m'}, {:name=>'ts', :db_type=>'tsmultirange', :typtype=>'m'}, {:name=>'tz', :db_type=>'tstzmultirange', :typtype=>'m'}]
    @db.schema(:items).map{|e| e[1][:type]}.must_equal [:integer, :int4multirange, :int8multirange, :nummultirange, :datemultirange, :tsmultirange, :tstzmultirange]
  end

  it "should parse arrays of range types from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i4', :db_type=>'int4multirange[]', :is_array=>true}, {:name=>'i8', :db_type=>'int8multirange[]', :is_array=>true}, {:name=>'n', :db_type=>'nummultirange[]', :is_array=>true}, {:name=>'d', :db_type=>'datemultirange[]', :is_array=>true}, {:name=>'ts', :db_type=>'tsmultirange[]', :is_array=>true}, {:name=>'tz', :db_type=>'tstzmultirange[]', :is_array=>true}]
    @db.schema(:items).map{|e| e[1][:type]}.must_equal [:integer, :int4multirange_array, :int8multirange_array, :nummultirange_array, :datemultirange_array, :tsmultirange_array, :tstzmultirange_array]
  end

  it "should set :ruby_default schema entries if default value is recognized using a database query" do
    v = @MR.new([@R.new(1, 3, :exclude_end=>true, :db_type=>'int8range')], 'int8multirange')
    @db.fetch = [[{:name=>'id', :db_type=>'integer', :default=>'1'},
        {:name=>'t', :db_type=>'int8multirange', :default=>"int8multirange(int8range(1,3,'[)'))"}],
      [{:v=>v}]
    ]
    s = @db.schema(:items)
    s[1][1][:ruby_default].must_equal v
    @db.sqls.last.must_equal "SELECT int8multirange(int8range(1,3,'[)')) LIMIT 1"
  end

  it "should work correctly in hashes" do
    h = Hash.new(1)
    h[@MR.new([@R.new(1, 2)], 'int8multirange')] = 2
    h[@MR.new([@R.new(nil, nil, :empty => true)], 'int8multirange')] = 3
    h[@MR.new([@R.new(1, 2)], 'int8multirange')].must_equal 2
    h[@MR.new([@R.new(1, 3)], 'int8multirange')].must_equal 1
    h[@MR.new([@R.new(2, 2)], 'int8multirange')].must_equal 1
    h[@MR.new([@R.new(1, 2, :exclude_begin => true)], 'int8multirange')].must_equal 1
    h[@MR.new([@R.new(1, 2, :exclude_end => true)], 'int8multirange')].must_equal 1
    h[@MR.new([@R.new(1, 2, :db_type => :int)], 'int8multirange')].must_equal 1
    h[@MR.new([@R.new(nil, nil, :empty => true)], 'int8multirange')].must_equal 3
    h[@MR.new([@R.new(nil, nil, :empty => true, :db_type => :int)], 'int8multirange')].must_equal 1
  end

  describe "database typecasting" do
    before do
      @o = @MR.new([@R.new(1, 2, :db_type=>'int4range')], 'int4multirange')
      @o2 = @MR.new([@R.new(1, 2, :db_type=>'int8range')], 'int8multirange')
      @eo = @MR.new([@R.new(nil, nil, :empty=>true, :db_type=>'int4range')], 'int4multirange')
      @eo2 = @MR.new([@R.new(nil, nil, :empty=>true, :db_type=>'int8range')], 'int8multirange')
    end
    
    it "should handle multiple multirange types" do
      %w'int4 int8 num date ts tstz'.each do |i|
        @db.typecast_value(:"#{i}multirange", @MR.new([@R.new(1, 2, :db_type=>"#{i}range")], "#{i}multirange")).must_equal @MR.new([@R.new(1, 2, :db_type=>"#{i}range")], "#{i}multirange")
      end
    end

    it "should handle arrays of multiple multirange types" do
      %w'int4 int8 num date ts tstz'.each do |i|
        @db.typecast_value(:"#{i}multirange_array", [@MR.new([@R.new(1, 2, :db_type=>"#{i}range")], "#{i}multirange")]).class.must_equal(Sequel::Postgres::PGArray)
        @db.typecast_value(:"#{i}multirange_array", [@MR.new([@R.new(1, 2, :db_type=>"#{i}range")], "#{i}multirange")]).must_equal [@MR.new([@R.new(1, 2, :db_type=>"#{i}range")], "#{i}multirange")]
      end
    end

    it "should return PGMultiRange value as is if they have the same db_type" do
      @db.typecast_value(:int4multirange, @o).must_equal @o
    end

    it "should return new PGMultiRange value if they have a different db_type" do
      @db.typecast_value(:int8multirange, @o).must_equal @o2
    end

    it "should return new PGMultiRange value if they have a different dbtype and value is empty" do
      @db.typecast_value(:int8multirange, @eo).must_equal @eo2
    end

    it "should return new PGMultiRange value if given an Array" do
      @db.typecast_value(:int4multirange, [1..2]).must_equal @o
      @db.typecast_value(:int4multirange, [1..2]).wont_equal @o2
      @db.typecast_value(:int8multirange, [1..2]).must_equal @o2
    end

    it "should parse a string argument as the PostgreSQL output format" do
      @db.typecast_value(:int4multirange, ['[1,2]']).must_equal @o
    end

    it "should raise errors for unparsable formats" do
      proc{@db.typecast_value(:int8multirange, ['foo'])}.must_raise(Sequel::InvalidValue)
    end

    it "should raise errors for unhandled values" do
      proc{@db.typecast_value(:int4multirange, 1)}.must_raise(Sequel::InvalidValue)
    end
  end

  it "should support registering custom range types" do
    @db.register_multirange_type('foomultirange', :range_oid=>3904)
    @db.typecast_value(:foomultirange, [1..2]).class.must_equal @MR
    @db.fetch = [{:name=>'id', :db_type=>'foomultirange', :typtype=>'m'}]
    @db.schema(:items).map{|e| e[1][:type]}.must_equal [:foomultirange]
  end

  it "should support using a block as a custom conversion proc given as block" do
    @db.register_multirange_type('foo2multirange', :oid=>1234) do |s|
      beg, en = s[1...-1].split(',').map{|x| (x*2).to_i}
      beg..en
    end
    @db.conversion_procs[1234].call('{[1,2]}').must_be :==, [11..22]
  end

  it "should support using a block as a custom conversion proc given as :converter option" do
    @db.register_multirange_type('foo2multirange', :oid=>1234, :converter=>proc do |s|
      beg, en = s[1...-1].split(',').map{|x| (x*2).to_i}
      beg..en
    end)
    @db.conversion_procs[1234].call('{[1,2]}').must_be :==, [11..22]
  end

  it "should support using an existing scaler conversion proc via the :range_oid option" do
    @db.register_multirange_type('foo4multirange', :oid=>1234, :range_oid=>3904)
    v = @db.conversion_procs[1234].call('{[1,2]}')
    v.must_equal @MR.new([@R.new(1, 2, :db_type=>'int4range')], 'foo4multirange')
    v.db_type.must_equal 'foo4multirange'
  end

  it "should raise an error if using :range_oid option with unexisting scalar conversion proc" do
    proc{@db.register_multirange_type('fooimultirange', :range_oid=>0)}.must_raise(Sequel::Error)
  end

  it "should raise an error if using :converter option and a block argument" do
    proc{@db.register_multirange_type('fooimultirange', :converter=>proc{}){}}.must_raise(Sequel::Error)
  end

  it "should raise an error if using :range_oid option and a block argument" do
    proc{@db.register_multirange_type('fooimultirange', :range_oid=>16){}}.must_raise(Sequel::Error)
  end

  it "should raise an error if using :converter option and a :range_oid option" do
    proc{@db.register_multirange_type('fooimultirange', :range_oid=>16, :converter=>proc{})}.must_raise(Sequel::Error)
  end

  it "should raise an error if using :oid option without a converter" do
    proc{@db.register_multirange_type('fooimultirange', :oid=>16)}.must_raise(Sequel::Error)
  end

  it "should not support registering custom multirange types on a per-Database basis for frozen databases" do
    @db.freeze
    proc{@db.register_multirange_type('banana', :oid=>7865){|s| s}}.must_raise RuntimeError, TypeError
  end

  it "should support registering custom multirange types on a per-Database basis" do
    @db.register_multirange_type('banana', :oid=>7865){|s| s}
    @db.conversion_procs[7865].call('{}').must_equal @MR.new([], 'banana')
    @db.fetch = [{:name=>'id', :db_type=>'banana', :typtype=>'m'}]
    @db.schema(:items).map{|e| e[1][:type]}.must_equal [:banana]
    @db.conversion_procs.must_include(7865)
    @db.respond_to?(:typecast_value_banana, true).must_equal true

    db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    def db.server_version(*) 140000 end
    db.extend_datasets(Module.new{def supports_timestamp_timezones?; false; end; def supports_timestamp_usecs?; false; end})
    db.extension(:pg_multirange)
    db.fetch = [{:name=>'id', :db_type=>'banana', :typtype=>'m'}]
    db.schema(:items).map{|e| e[1][:type]}.must_equal [:multirange]
    db.conversion_procs.wont_include(7865)
    db.respond_to?(:typecast_value_banana, true).must_equal false
  end

  it "should automatically look up the multirange and subtype oids when registering per-Database types" do
    @db.fetch = [[{:rngtypid=>3904, :rngmultitypid=>7866}], [{:name=>'id', :db_type=>'banana', :typtype=>'m'}]]
    @db.register_multirange_type('banana')
    @db.sqls.must_equal ["SELECT rngmultitypid, rngtypid FROM pg_range INNER JOIN pg_type ON (pg_type.oid = pg_range.rngmultitypid) WHERE (typname = 'banana') LIMIT 1"]
    @db.schema(:items).map{|e| e[1][:type]}.must_equal [:banana]
    @db.conversion_procs[7866].call("{[1,3)}").must_be :==, [1...3]
  end

  it "should not automatically look up oids if given both :oid and :range_oid" do
    @db.register_multirange_type('banana', :oid=>7866, :range_oid=>3904)
    @db.sqls.must_equal []
    @db.conversion_procs[7866].call("{[1,3]}").must_equal @MR.new([@R.new(1, 3, :db_type=>'int4range')], 'banana')
  end

  it "should not automatically look up oids if given multirange oid and block" do
    @db.register_multirange_type('banana', :oid=>7866){|s| Range.new(*s[1...-1].split(',').map(&:to_i))}
    @db.sqls.must_equal []
    @db.conversion_procs[7866].call("{[1,3]}").must_be :==, [1..3]
  end

  it "should return correct results for Database#schema_type_class" do
    @db.schema_type_class(:int4multirange).must_equal Sequel::Postgres::PGMultiRange
    @db.schema_type_class(:integer).must_equal Integer
  end

  describe "parser" do
    before do
      @converter = :to_s.to_proc
    end

    it "should raise if input doesn't start with {" do
      proc{@MR::Parser.new('', @converter).parse}.must_raise Sequel::Error
      proc{@MR::Parser.new('a', @converter).parse}.must_raise Sequel::Error
    end

    it "should raise if there is data after parsing has finished" do
      proc{@MR::Parser.new('{}}', @converter).parse}.must_raise Sequel::Error
      proc{@MR::Parser.new('{[1,2]}a', @converter).parse}.must_raise Sequel::Error
      proc{@MR::Parser.new('{[1,2],(3,4)})', @converter).parse}.must_raise Sequel::Error
    end

    it "should raise if invalid separator is used" do
      proc{@MR::Parser.new('{[1,2]a}', @converter).parse}.must_raise Sequel::Error
    end

    it "should raise if incomplete multirange is parsed" do
      proc{@MR::Parser.new('{[1', @converter).parse}.must_raise Sequel::Error
    end
  end

  describe "a PGMultiRange instance" do
    before do
      @r1 = @MR.new([], 'int4multirange')
      @r2 = @MR.new([@R.new(3, nil, :exclude_begin=>true, :db_type=>'int4range')], 'int4multirange')
      @r3 = @MR.new([@R.new(nil, 4, :exclude_end=>true, :db_type=>'int8range'), @R.new(14, nil, :db_type=>'int8range')], 'int8multirange')
    end

    it "should have #db_type return the multirange's database type" do
      @r1.db_type.must_equal 'int4multirange'
      @r2.db_type.must_equal 'int4multirange'
      @r3.db_type.must_equal 'int8multirange'
    end

    it "should be able to be created by Sequel.pg_multirange" do
      Sequel.pg_multirange([], 'int4multirange').must_equal @r1
    end

    it "should have Sequel.pg_range return a PGRange as is" do
      Sequel.pg_multirange(@r1, 'int4multirange').to_a.must_be_same_as @r1.to_a
    end

    it "should have Sequel.pg_multirange return a new PGMultiRange if the database type differs" do
      v = Sequel.pg_multirange(@r2, 'int8multirange')
      v.must_equal @MR.new([@R.new(3, nil, :exclude_begin=>true, :db_type=>'int4range')], 'int8multirange')
      v.db_type.must_equal 'int8multirange'
    end

    it "should have cover? and === match if any member in the multiranges matches" do
      @r1.cover?(1).must_equal false
      @r2.cover?(1).must_equal false
      @r3.cover?(1).must_equal true

      @r1.cover?(3).must_equal false
      @r2.cover?(3).must_equal false
      @r3.cover?(3).must_equal true

      @r1.cover?(4).must_equal false
      @r2.cover?(4).must_equal true
      @r3.cover?(4).must_equal false

      @r1.cover?(5).must_equal false
      @r2.cover?(5).must_equal true
      @r3.cover?(5).must_equal false

      @r1.cover?(14).must_equal false
      @r2.cover?(14).must_equal true
      @r3.cover?(14).must_equal true

      @r1.===(14).must_equal false
      @r2.===(14).must_equal true
      @r3.===(14).must_equal true
    end

    it "should only consider PGMultiRanges equal if they have the same db_type" do
      (@MR.new([], 'int4range') == @MR.new([], 'int4range')).must_equal true
      (@MR.new([], 'int4range') == @MR.new([], 'int8range')).must_equal false
      (@MR.new([], 'int4range') == []).must_equal true
      (@MR.new([], 'int4range').eql? @MR.new([], 'int4range')).must_equal true
      (@MR.new([], 'int4range').eql? @MR.new([], 'int8range')).must_equal false
      (@MR.new([], 'int4range').eql? []).must_equal true
    end
  end
end
