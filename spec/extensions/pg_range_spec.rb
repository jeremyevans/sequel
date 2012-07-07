require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_range extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @R = Sequel::Postgres::PGRange
    @db.extend(Module.new{def get_conversion_procs(conn) {} end; def bound_variable_arg(arg, conn) arg end})
    @db.extend_datasets(Module.new{def supports_timestamp_timezones?; false; end; def supports_timestamp_usecs?; false; end})
    @db.extension(:pg_array)
    @db.extension(:pg_range)
  end

  it "should literalize Range instances to strings correctly" do
    @db.literal(Date.new(2011, 1, 2)...Date.new(2011, 3, 2)).should == "'[2011-01-02,2011-03-02)'"
    @db.literal(Time.local(2011, 1, 2, 10, 20, 30)...Time.local(2011, 2, 3, 10, 20, 30)).should == "'[2011-01-02 10:20:30,2011-02-03 10:20:30)'"
    @db.literal(DateTime.new(2011, 1, 2, 10, 20, 30)...DateTime.new(2011, 2, 3, 10, 20, 30)).should == "'[2011-01-02 10:20:30,2011-02-03 10:20:30)'"
    @db.literal(DateTime.new(2011, 1, 2, 10, 20, 30)...DateTime.new(2011, 2, 3, 10, 20, 30)).should == "'[2011-01-02 10:20:30,2011-02-03 10:20:30)'"
    @db.literal(1..2).should == "'[1,2]'"
    @db.literal(1.0..2.0).should == "'[1.0,2.0]'"
    @db.literal(BigDecimal.new('1.0')..BigDecimal.new('2.0')).should == "'[1.0,2.0]'"
    @db.literal(Sequel.lit('a')..Sequel.lit('z')).should == "'[a,z]'"
    @db.literal(''..'()[]",\\2').should == "'[\"\",\\(\\)\\[\\]\\\"\\,\\\\2]'"
  end

  it "should literalize PGRange instances to strings correctly" do
    @db.literal(@R.new(1, 2)).should == "'[1,2]'"
    @db.literal(@R.new(true, false)).should == "'[true,false]'"
    @db.literal(@R.new(1, 2, :exclude_begin=>true)).should == "'(1,2]'"
    @db.literal(@R.new(1, 2, :exclude_end=>true)).should == "'[1,2)'"
    @db.literal(@R.new(nil, 2)).should == "'[,2]'"
    @db.literal(@R.new(1, nil)).should == "'[1,]'"
    @db.literal(@R.new(1, 2, :db_type=>'int8range')).should == "'[1,2]'::int8range"
    @db.literal(@R.new(nil, nil, :empty=>true)).should == "'empty'"
    @db.literal(@R.new("", 2)).should == "'[\"\",2]'"
  end

  it "should not affect literalization of custom objects" do
    o = Object.new
    def o.sql_literal(ds) 'v' end
    @db.literal(o).should == 'v'
  end

  it "should support using Range instances as bound variables" do
    @db.bound_variable_arg(1..2, nil).should == "[1,2]"
  end

  it "should support using PGRange instances as bound variables" do
    @db.bound_variable_arg(@R.new(1, 2), nil).should == "[1,2]"
  end

  it "should support using arrays of Range instances as bound variables" do
    @db.bound_variable_arg([1..2,2...3], nil).should == '{"[1,2]","[2,3)"}'
  end

  it "should support using PGRange instances as bound variables" do
    @db.bound_variable_arg([@R.new(1, 2),@R.new(2, 3)], nil).should == '{"[1,2]","[2,3]"}'
  end

  it "should parse range types from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i4', :db_type=>'int4range'}, {:name=>'i8', :db_type=>'int8range'}, {:name=>'n', :db_type=>'numrange'}, {:name=>'d', :db_type=>'daterange'}, {:name=>'ts', :db_type=>'tsrange'}, {:name=>'tz', :db_type=>'tstzrange'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :int4range, :int8range, :numrange, :daterange, :tsrange, :tstzrange]
  end

  it "should parse arrays of range types from the schema correctly" do
    @db.fetch = [{:name=>'id', :db_type=>'integer'}, {:name=>'i4', :db_type=>'int4range[]'}, {:name=>'i8', :db_type=>'int8range[]'}, {:name=>'n', :db_type=>'numrange[]'}, {:name=>'d', :db_type=>'daterange[]'}, {:name=>'ts', :db_type=>'tsrange[]'}, {:name=>'tz', :db_type=>'tstzrange[]'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:integer, :int4range_array, :int8range_array, :numrange_array, :daterange_array, :tsrange_array, :tstzrange_array]
  end

  describe "database typecasting" do
    before do
      @o = @R.new(1, 2, :db_type=>'int4range')
      @o2 = @R.new(1, 2, :db_type=>'int8range')
      @eo = @R.new(nil, nil, :empty=>true, :db_type=>'int4range')
      @eo2 = @R.new(nil, nil, :empty=>true, :db_type=>'int8range')
    end

    it "should handle multiple range types" do
      %w'int4 int8 num date ts tstz'.each do |i|
        @db.typecast_value(:"#{i}range", @R.new(1, 2, :db_type=>"#{i}range")).should == @R.new(1, 2, :db_type=>"#{i}range")
      end
    end

    it "should handle multiple array range types" do
      %w'int4 int8 num date ts tstz'.each do |i|
        @db.typecast_value(:"#{i}range_array", [@R.new(1, 2, :db_type=>"#{i}range")]).should be_a_kind_of(Sequel::Postgres::PGArray)
        @db.typecast_value(:"#{i}range_array", [@R.new(1, 2, :db_type=>"#{i}range")]).should == [@R.new(1, 2, :db_type=>"#{i}range")]
      end
    end

    it "should return PGRange value as is if they have the same subtype" do
      @db.typecast_value(:int4range, @o).should equal(@o)
    end

    it "should return new PGRange value as is if they have a different subtype" do
      @db.typecast_value(:int8range, @o).should_not equal(@o)
      @db.typecast_value(:int8range, @o).should == @o2
    end

    it "should return new PGRange value as is if they have a different subtype and value is empty" do
      @db.typecast_value(:int8range, @eo).should == @eo2
    end

    it "should return new PGRange value if given a Range" do
      @db.typecast_value(:int4range, 1..2).should == @o
      @db.typecast_value(:int4range, 1..2).should_not == @o2
      @db.typecast_value(:int8range, 1..2).should == @o2
    end

    it "should parse a string argument as the PostgreSQL output format" do
      @db.typecast_value(:int4range, '[1,2]').should == @o
    end

    it "should raise errors for unparsable formats" do
      proc{@db.typecast_value(:int8range, 'foo')}.should raise_error(Sequel::InvalidValue)
    end

    it "should raise errors for unhandled values" do
      proc{@db.typecast_value(:int4range, 1)}.should raise_error(Sequel::InvalidValue)
    end
  end

  it "should support registering custom range types" do
    @R.register('foorange')
    @db.typecast_value(:foorange, 1..2).should be_a_kind_of(@R)
    @db.fetch = [{:name=>'id', :db_type=>'foorange'}]
    @db.schema(:items).map{|e| e[1][:type]}.should == [:foorange]
  end

  it "should support using a block as a custom conversion proc given as block" do
    @R.register('foo2range'){|s| (s*2).to_i}
    @db.typecast_value(:foo2range, '[1,2]').should == (11..22)
  end

  it "should support using a block as a custom conversion proc given as :converter option" do
    @R.register('foo3range', :converter=>proc{|s| (s*2).to_i})
    @db.typecast_value(:foo3range, '[1,2]').should == (11..22)
  end

  it "should support using an existing scaler conversion proc via the :subtype_oid option" do
    @R.register('foo4range', :subtype_oid=>16)
    @db.typecast_value(:foo4range, '[t,f]').should == @R.new(true, false, :db_type=>'foo4range')
  end

  it "should raise an error if using :subtype_oid option with unexisting scalar conversion proc" do
    proc{@R.register('fooirange', :subtype_oid=>0)}.should raise_error(Sequel::Error)
  end

  it "should raise an error if using :converter option and a block argument" do
    proc{@R.register('fooirange', :converter=>proc{}){}}.should raise_error(Sequel::Error)
  end

  it "should raise an error if using :subtype_oid option and a block argument" do
    proc{@R.register('fooirange', :subtype_oid=>16){}}.should raise_error(Sequel::Error)
  end

  it "should support registering custom types with :oid option" do
    @R.register('foo5range', :oid=>331)
    Sequel::Postgres::PG_TYPES[331].call('[1,3)').should be_a_kind_of(@R)
  end

  describe "parser" do
    before do
      @p = Sequel::Postgres::PG_TYPES[3904]
      @sp = @R::Parser.new(nil)
    end

    it "should have db_type method to return the database type string" do
      @p.db_type.should == 'int4range'
    end

    it "should have converter method which returns a callable used for conversion" do
      @p.converter.call('1').should == 1
    end

    it "should have call parse input string argument into PGRange instance" do
      @p.call('[1,2]').should == @R.new(1, 2, :db_type=>'int4range')
    end

    it "should handle empty ranges" do
      @p.call('empty').should == @R.new(nil, nil, :empty=>true, :db_type=>'int4range')
    end

    it "should handle exclusive beginnings and endings" do
      @p.call('(1,3]').should == @R.new(1, 3, :exclude_begin=>true, :db_type=>'int4range')
      @p.call('[1,3)').should == @R.new(1, 3, :exclude_end=>true, :db_type=>'int4range')
      @p.call('(1,3)').should == @R.new(1, 3, :exclude_begin=>true, :exclude_end=>true, :db_type=>'int4range')
    end

    it "should handle unbounded beginnings and endings" do
      @p.call('[,2]').should == @R.new(nil, 2, :db_type=>'int4range')
      @p.call('[1,]').should == @R.new(1, nil, :db_type=>'int4range')
      @p.call('[,]').should == @R.new(nil, nil, :db_type=>'int4range')
    end

    it "should unescape quoted beginnings and endings" do
      @sp.call('["\\\\ \\"","\\" \\\\"]').should == @R.new("\\ \"", "\" \\")
    end

    it "should treat empty quoted string not as unbounded" do
      @sp.call('["","z"]').should == @R.new("", "z")
      @sp.call('["a",""]').should == @R.new("a", "")
      @sp.call('["",""]').should == @R.new("", "")
    end
  end

  it "should set appropriate timestamp range conversion procs when getting conversion procs" do
    procs = @db.send(:get_conversion_procs, nil)
    procs[3908].call('[2011-10-20 11:12:13,2011-10-20 11:12:14]').should == (Time.local(2011, 10, 20, 11, 12, 13)..(Time.local(2011, 10, 20, 11, 12, 14)))
    procs[3910].call('[2011-10-20 11:12:13,2011-10-20 11:12:14]').should == (Time.local(2011, 10, 20, 11, 12, 13)..(Time.local(2011, 10, 20, 11, 12, 14)))
  end

  it "should set appropriate timestamp range array conversion procs when getting conversion procs" do
    procs = @db.send(:get_conversion_procs, nil)
    procs[3909].call('{"[2011-10-20 11:12:13,2011-10-20 11:12:14]"}').should == [Time.local(2011, 10, 20, 11, 12, 13)..Time.local(2011, 10, 20, 11, 12, 14)]
    procs[3911].call('{"[2011-10-20 11:12:13,2011-10-20 11:12:14]"}').should == [Time.local(2011, 10, 20, 11, 12, 13)..Time.local(2011, 10, 20, 11, 12, 14)]
  end

  describe "a PGRange instance" do
    before do
      @r1 = @R.new(1, 2)
      @r2 = @R.new(3, nil, :exclude_begin=>true, :db_type=>'int4range')
      @r3 = @R.new(nil, 4, :exclude_end=>true, :db_type=>'int8range')
    end

    it "should have #begin return the beginning of the range" do
      @r1.begin.should == 1
      @r2.begin.should == 3
      @r3.begin.should == nil
    end

    it "should have #end return the end of the range" do
      @r1.end.should == 2
      @r2.end.should == nil
      @r3.end.should == 4
    end

    it "should have #db_type return the range's database type" do
      @r1.db_type.should == nil
      @r2.db_type.should == 'int4range'
      @r3.db_type.should == 'int8range'
    end

    it "should be able to be created by Range#pg_range" do
      (1..2).pg_range.should == @r1
    end

    it "should have #initialize raise if requesting an empty range with beginning or ending" do
      proc{@R.new(1, nil, :empty=>true)}.should raise_error(Sequel::Error)
      proc{@R.new(nil, 2, :empty=>true)}.should raise_error(Sequel::Error)
      proc{@R.new(nil, nil, :empty=>true, :exclude_begin=>true)}.should raise_error(Sequel::Error)
      proc{@R.new(nil, nil, :empty=>true, :exclude_end=>true)}.should raise_error(Sequel::Error)
    end

    it "should quack like a range" do
      if RUBY_VERSION >= '1.9'
        @r1.cover?(1.5).should be_true
        @r1.cover?(2.5).should be_false
        @r1.first(1).should == [1]
        @r1.last(1).should == [2]
      end
      @r1.to_a.should == [1, 2]
      @r1.first.should == 1
      @r1.last.should == 2
      a = []
      @r1.step{|x| a << x}
      a.should == [1, 2]
    end

    it "should only consider PGRanges equal if they have the same db_type" do
      @R.new(1, 2, :db_type=>'int4range').should == @R.new(1, 2, :db_type=>'int4range')
      @R.new(1, 2, :db_type=>'int8range').should_not == @R.new(1, 2, :db_type=>'int4range')
    end

    it "should only consider empty PGRanges equal with other empty PGRanges" do
      @R.new(nil, nil, :empty=>true).should == @R.new(nil, nil, :empty=>true)
      @R.new(nil, nil, :empty=>true).should_not == @R.new(nil, nil)
      @R.new(nil, nil).should_not == @R.new(nil, nil, :empty=>true)
    end

    it "should only consider empty PGRanges equal if they have the same bounds" do
      @R.new(1, 2).should == @R.new(1, 2)
      @R.new(1, 2).should_not == @R.new(1, 3)
    end

    it "should only consider empty PGRanges equal if they have the same bound exclusions" do
      @R.new(1, 2, :exclude_begin=>true).should == @R.new(1, 2, :exclude_begin=>true)
      @R.new(1, 2, :exclude_end=>true).should == @R.new(1, 2, :exclude_end=>true)
      @R.new(1, 2, :exclude_begin=>true).should_not == @R.new(1, 2, :exclude_end=>true)
      @R.new(1, 2, :exclude_end=>true).should_not == @R.new(1, 2, :exclude_begin=>true)
    end

    it "should consider PGRanges equal with a Range they represent" do
      @R.new(1, 2).should == (1..2)
      @R.new(1, 2, :exclude_end=>true).should == (1...2)
      @R.new(1, 3).should_not == (1..2)
      @R.new(1, 2, :exclude_end=>true).should_not == (1..2)
    end

    it "should not consider a PGRange equal with a Range if it can't be expressed as a range" do
      @R.new(nil, nil).should_not == (1..2)
    end

    it "should not consider a PGRange equal to other objects" do
      @R.new(nil, nil).should_not == 1
    end

    it "should have #=== be true if given an equal PGRange" do
      @R.new(1, 2).should === @R.new(1, 2)
      @R.new(1, 2).should_not === @R.new(1, 3)

    end

    it "should have #=== be true if it would be true for the Range represented by the PGRange" do
      @R.new(1, 2).should === 1.5
      @R.new(1, 2).should_not === 2.5
    end

    it "should have #=== be false if the PGRange cannot be represented by a Range" do
      @R.new(nil, nil).should_not === 1.5
    end

    it "should have #empty? indicate whether the range is empty" do
      @R.empty.should be_empty
      @R.new(1, 2).should_not be_empty
    end

    it "should have #exclude_begin? and #exclude_end indicate whether the beginning or ending of the range is excluded" do
      @r1.exclude_begin?.should be_false
      @r1.exclude_end?.should be_false
      @r2.exclude_begin?.should be_true
      @r2.exclude_end?.should be_false
      @r3.exclude_begin?.should be_false
      @r3.exclude_end?.should be_true
    end

    it "should have #to_range raise an exception if the PGRange cannot be represented by a Range" do
      proc{@R.new(nil, 1).to_range}.should raise_error(Sequel::Error)
      proc{@R.new(1, nil).to_range}.should raise_error(Sequel::Error)
      proc{@R.new(0, 1, :exclude_begin=>true).to_range}.should raise_error(Sequel::Error)
      proc{@R.empty.to_range}.should raise_error(Sequel::Error)
    end

    it "should have #to_range return the represented range" do
      @r1.to_range.should == (1..2)
    end

    it "should have #to_range cache the returned value" do
      @r1.to_range.should equal(@r1.to_range)
    end

    it "should have #unbounded_begin? and #unbounded_end indicate whether the beginning or ending of the range is unbounded" do
      @r1.unbounded_begin?.should be_false
      @r1.unbounded_end?.should be_false
      @r2.unbounded_begin?.should be_false
      @r2.unbounded_end?.should be_true
      @r3.unbounded_begin?.should be_true
      @r3.unbounded_end?.should be_false
    end

    it "should have #valid_ruby_range? return true if the PGRange can be represented as a Range" do
      @r1.valid_ruby_range?.should be_true
      @R.new(1, 2, :exclude_end=>true).valid_ruby_range?.should be_true
    end

    it "should have #valid_ruby_range? return false if the PGRange cannot be represented as a Range" do
      @R.new(nil, 1).valid_ruby_range?.should be_false
      @R.new(1, nil).valid_ruby_range?.should be_false
      @R.new(0, 1, :exclude_begin=>true).valid_ruby_range?.should be_false
      @R.empty.valid_ruby_range?.should be_false
    end
  end
end
