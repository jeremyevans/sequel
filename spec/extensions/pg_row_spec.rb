require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_row extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.extension(:pg_array, :pg_row)
    @m = Sequel::Postgres::PGRow
    @db.sqls
  end

  it "should parse record objects as arrays" do
    a = Sequel::Postgres::PG_TYPES[2249].call("(a,b,c)")
    a.should be_a_kind_of(@m::ArrayRow)
    a.to_a.should be_a_kind_of(Array)
    a[0].should == 'a'
    a.should == %w'a b c'
    a.db_type.should == nil
    @db.literal(a).should == "ROW('a', 'b', 'c')"
  end

  it "should parse arrays of record objects as arrays of arrays" do
    as = Sequel::Postgres::PG_TYPES[2287].call('{"(a,b,c)","(d,e,f)"}')
    as.should == [%w'a b c', %w'd e f']
    as.each do |a|
      a.should be_a_kind_of(@m::ArrayRow)
      a.to_a.should be_a_kind_of(Array)
      a.db_type.should == nil
    end
    @db.literal(as).should == "ARRAY[ROW('a', 'b', 'c'),ROW('d', 'e', 'f')]::record[]"
  end

  it "should be able to register custom parsing of row types as array-like objects" do
    klass = @m::ArrayRow.subclass(:foo)
    parser = @m::Parser.new(:converter=>klass)
    a = parser.call("(a,b,c)")
    a.should be_a_kind_of(klass)
    a.to_a.should be_a_kind_of(Array)
    a[0].should == 'a'
    a.should == %w'a b c'
    a.db_type.should == :foo
    @db.literal(a).should == "ROW('a', 'b', 'c')::foo"
  end

  it "should be able to register custom parsing of row types as hash-like objects" do
    klass = @m::HashRow.subclass(:foo, [:a, :b, :c])
    parser = @m::Parser.new(:converter=>klass, :columns=>[:a, :b, :c])
    a = parser.call("(a,b,c)")
    a.should be_a_kind_of(klass)
    a.to_hash.should be_a_kind_of(Hash)
    a[:a].should == 'a'
    a.should == {:a=>'a', :b=>'b', :c=>'c'}
    a.db_type.should == :foo
    a.columns.should == [:a, :b, :c]
    @db.literal(a).should == "ROW('a', 'b', 'c')::foo"
  end

  it "should raise an error if attempting to literalize a HashRow without column information" do
    h = @m::HashRow.call(:a=>'a', :b=>'b', :c=>'c')
    proc{@db.literal(h)}.should raise_error(Sequel::Error)
  end

  it "should be able to manually override db_type per ArrayRow instance" do
    a = @m::ArrayRow.call(%w'a b c')
    a.db_type = :foo
    @db.literal(a).should == "ROW('a', 'b', 'c')::foo"
  end

  it "should be able to manually override db_type and columns per HashRow instance" do
    h = @m::HashRow.call(:a=>'a', :c=>'c', :b=>'b')
    h.db_type = :foo
    h.columns = [:a, :b, :c]
    @db.literal(h).should == "ROW('a', 'b', 'c')::foo"
  end

  it "should correctly split an empty row" do
    @m::Splitter.new("()").parse.should == [nil]
  end

  it "should correctly split a row with a single value" do
    @m::Splitter.new("(1)").parse.should == %w'1'
  end

  it "should correctly split a row with multiple values" do
    @m::Splitter.new("(1,2)").parse.should == %w'1 2'
  end

  it "should correctly NULL values when splitting" do
    @m::Splitter.new("(1,)").parse.should == ['1', nil]
  end

  it "should correctly empty string values when splitting" do
    @m::Splitter.new('(1,"")').parse.should == ['1', '']
  end

  it "should handle quoted values when splitting" do
    @m::Splitter.new('("1","2")').parse.should == %w'1 2'
  end

  it "should handle escaped backslashes in quoted values when splitting" do
    @m::Splitter.new('("\\\\1","2\\\\")').parse.should == ['\\1', '2\\']
  end

  it "should handle doubled quotes in quoted values when splitting" do
    @m::Splitter.new('("""1","2""")').parse.should == ['"1', '2"']
  end

  it "should correctly convert types when parsing into an array" do
    @m::Parser.new(:column_converters=>[proc{|s| s*2}, proc{|s| s*3}, proc{|s| s*4}]).call("(a,b,c)").should == %w'aa bbb cccc'
  end

  it "should correctly convert types into hashes if columns are known" do
    @m::Parser.new(:columns=>[:a, :b, :c]).call("(a,b,c)").should == {:a=>'a', :b=>'b', :c=>'c'}
  end

  it "should correctly handle type conversion when converting into hashes" do
    @m::Parser.new(:column_converters=>[proc{|s| s*2}, proc{|s| s*3}, proc{|s| s*4}], :columns=>[:a, :b, :c]).call("(a,b,c)").should == {:a=>'aa', :b=>'bbb', :c=>'cccc'}
  end

  it "should correctly wrap arrays when converting" do
    @m::Parser.new(:converter=>proc{|s| [:foo, s]}).call("(a,b,c)").should == [:foo, %w'a b c']
  end

  it "should correctly wrap hashes when converting" do
    @m::Parser.new(:converter=>proc{|s| [:foo, s]}, :columns=>[:a, :b, :c]).call("(a,b,c)").should == [:foo, {:a=>'a', :b=>'b', :c=>'c'}]
  end

  it "should have parser store reflection information" do
    p = @m::Parser.new(:oid=>1, :column_oids=>[2], :columns=>[:a], :converter=>Array, :typecaster=>Hash, :column_converters=>[Array])
    p.oid.should == 1
    p.column_oids.should == [2]
    p.columns.should == [:a]
    p.converter.should == Array
    p.typecaster.should == Hash
    p.column_converters.should == [Array]
  end

  it "should reload registered row types when reseting conversion procs" do
    db = Sequel.mock(:host=>'postgres')
    db.extension(:pg_row)
    db.conversion_procs[4] = proc{|s| s.to_i}
    db.conversion_procs[5] = proc{|s| s * 2}
    db.sqls
    db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    db.register_row_type(:foo)
    db.sqls.should == ["SELECT pg_type.oid, typrelid, typarray FROM pg_type WHERE ((typtype = 'c') AND (typname = 'foo')) LIMIT 1",
      "SELECT attname, atttypid FROM pg_attribute WHERE ((attrelid = 2) AND (attnum > 0) AND NOT attisdropped) ORDER BY attnum"]

    begin
      pgnt = Sequel::Postgres::PG_NAMED_TYPES.dup
      Sequel::Postgres::PG_NAMED_TYPES.clear
      db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
      db.reset_conversion_procs
      db.sqls.should == ["SELECT pg_type.oid, typrelid, typarray FROM pg_type WHERE ((typtype = 'c') AND (typname = 'foo')) LIMIT 1",
        "SELECT attname, atttypid FROM pg_attribute WHERE ((attrelid = 2) AND (attnum > 0) AND NOT attisdropped) ORDER BY attnum"]
    ensure
      Sequel::Postgres::PG_NAMED_TYPES.replace pgnt
    end
  end

  it "should handle ArrayRows and HashRows in bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg(@m::ArrayRow.call(["1", "abc\\'\","]), nil).should == '("1","abc\\\\\'\\",")'
    @db.bound_variable_arg(@m::HashRow.subclass(nil, [:a, :b]).call(:a=>"1", :b=>"abc\\'\","), nil).should == '("1","abc\\\\\'\\",")'
  end

  it "should handle ArrayRows and HashRows in arrays in bound variables" do
    @db.bound_variable_arg(1, nil).should == 1
    @db.bound_variable_arg([@m::ArrayRow.call(["1", "abc\\'\","])], nil).should == '{"(\\"1\\",\\"abc\\\\\\\\\'\\\\\\",\\")"}'
    @db.bound_variable_arg([@m::HashRow.subclass(nil, [:a, :b]).call(:a=>"1", :b=>"abc\\'\",")], nil).should == '{"(\\"1\\",\\"abc\\\\\\\\\'\\\\\\",\\")"}'
  end

  it "should handle nils in bound variables" do
    @db.bound_variable_arg(@m::ArrayRow.call([nil, nil]), nil).should == '(,)'
    @db.bound_variable_arg(@m::HashRow.subclass(nil, [:a, :b]).call(:a=>nil, :b=>nil), nil).should == '(,)'
    @db.bound_variable_arg([@m::ArrayRow.call([nil, nil])], nil).should == '{"(,)"}'
    @db.bound_variable_arg([@m::HashRow.subclass(nil, [:a, :b]).call(:a=>nil, :b=>nil)], nil).should == '{"(,)"}'
  end
  
  it "should allow registering row type parsers by introspecting system tables" do
    @db.conversion_procs[4] = p4 = proc{|s| s.to_i}
    @db.conversion_procs[5] = p5 = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    @db.register_row_type(:foo)
    @db.sqls.should == ["SELECT pg_type.oid, typrelid, typarray FROM pg_type WHERE ((typtype = 'c') AND (typname = 'foo')) LIMIT 1",
      "SELECT attname, atttypid FROM pg_attribute WHERE ((attrelid = 2) AND (attnum > 0) AND NOT attisdropped) ORDER BY attnum"]
    p1 = @db.conversion_procs[1]
    p1.columns.should == [:bar, :baz]
    p1.column_oids.should == [4, 5]
    p1.column_converters.should == [p4, p5]
    p1.oid.should == 1
    @db.send(:schema_column_type, 'foo').should == :pg_row_foo
    @db.send(:schema_column_type, 'integer').should == :integer

    c = p1.converter
    c.superclass.should == @m::HashRow
    c.columns.should == [:bar, :baz]
    c.db_type.should == :foo
    p1.typecaster.should == c

    p1.call('(1,b)').should == {:bar=>1, :baz=>'bb'}
    @db.typecast_value(:pg_row_foo, %w'1 b').should be_a_kind_of(@m::HashRow)
    @db.typecast_value(:pg_row_foo, %w'1 b').should == {:bar=>'1', :baz=>'b'}
    @db.typecast_value(:pg_row_foo, :bar=>'1', :baz=>'b').should == {:bar=>'1', :baz=>'b'}
    @db.literal(p1.call('(1,b)')).should == "ROW(1, 'bb')::foo"
  end

  it "should allow registering row type parsers for schema qualify types" do
    @db.conversion_procs[4] = p4 = proc{|s| s.to_i}
    @db.conversion_procs[5] = p5 = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    @db.register_row_type(:foo__bar)
    @db.sqls.should == ["SELECT pg_type.oid, typrelid, typarray FROM pg_type INNER JOIN pg_namespace ON ((pg_namespace.oid = pg_type.typnamespace) AND (pg_namespace.nspname = 'foo')) WHERE ((typtype = 'c') AND (typname = 'bar')) LIMIT 1",
      "SELECT attname, atttypid FROM pg_attribute WHERE ((attrelid = 2) AND (attnum > 0) AND NOT attisdropped) ORDER BY attnum"]
    p1 = @db.conversion_procs[1]
    p1.columns.should == [:bar, :baz]
    p1.column_oids.should == [4, 5]
    p1.column_converters.should == [p4, p5]
    p1.oid.should == 1

    c = p1.converter
    c.superclass.should == @m::HashRow
    c.columns.should == [:bar, :baz]
    c.db_type.should == :foo__bar
    p1.typecaster.should == c

    p1.call('(1,b)').should == {:bar=>1, :baz=>'bb'}
    @db.typecast_value(:pg_row_foo__bar, %w'1 b').should == {:bar=>'1', :baz=>'b'}
    @db.typecast_value(:pg_row_foo__bar, :bar=>'1', :baz=>'b').should == {:bar=>'1', :baz=>'b'}
    @db.literal(p1.call('(1,b)')).should == "ROW(1, 'bb')::foo.bar"
  end

  it "should allow registering with a custom converter" do
    @db.conversion_procs[4] = proc{|s| s.to_i}
    @db.conversion_procs[5] = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    c = proc{|h| [h]}
    @db.register_row_type(:foo, :converter=>c)
    o = @db.conversion_procs[1].call('(1,b)')
    o.should == [{:bar=>1, :baz=>'bb'}]
    o.first.should be_a_kind_of(Hash)
  end

  it "should allow registering with a custom typecaster" do
    @db.conversion_procs[4] = proc{|s| s.to_i}
    @db.conversion_procs[5] = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    @db.register_row_type(:foo, :typecaster=>proc{|h| {:bar=>(h[:bar]||0).to_i, :baz=>(h[:baz] || 'a')*2}})
    @db.typecast_value(:pg_row_foo, %w'1 b').should be_a_kind_of(Hash)
    @db.typecast_value(:pg_row_foo, %w'1 b').should == {:bar=>1, :baz=>'bb'}
    @db.typecast_value(:pg_row_foo, :bar=>'1', :baz=>'b').should == {:bar=>1, :baz=>'bb'}
    @db.typecast_value(:pg_row_foo, 'bar'=>'1', 'baz'=>'b').should == {:bar=>0, :baz=>'aa'}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    @db.register_row_type(:foo, :typecaster=>proc{|h| {:bar=>(h[:bar] || h['bar'] || 0).to_i, :baz=>(h[:baz] || h['baz'] || 'a')*2}})
    @db.typecast_value(:pg_row_foo, %w'1 b').should == {:bar=>1, :baz=>'bb'}
    @db.typecast_value(:pg_row_foo, :bar=>'1', :baz=>'b').should == {:bar=>1, :baz=>'bb'}
    @db.typecast_value(:pg_row_foo, 'bar'=>'1', 'baz'=>'b').should == {:bar=>1, :baz=>'bb'}
  end

  it "should handle conversion procs that aren't added until later" do
    @db.conversion_procs[5] = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    c = proc{|h| [h]}
    @db.register_row_type(:foo, :converter=>c)
    @db.conversion_procs[4] = proc{|s| s.to_i}
    @db.conversion_procs[1].call('(1,b)').should == [{:bar=>1, :baz=>'bb'}]
  end

  it "should handle nil values when converting columns" do
    @db.conversion_procs[5] = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}]]
    called = false
    @db.conversion_procs[4] = proc{|s| called = true; s}
    @db.register_row_type(:foo)
    @db.conversion_procs[1].call('()').should == {:bar=>nil}
    called.should be_false
  end

  it "should registering array type for row type if type has an array oid" do
    @db.conversion_procs[4] = proc{|s| s.to_i}
    @db.conversion_procs[5] = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    @db.register_row_type(:foo, :typecaster=>proc{|h| {:bar=>(h[:bar]||0).to_i, :baz=>(h[:baz] || 'a')*2}})
    p3 = @db.conversion_procs[3]

    p3.call('{"(1,b)"}').should == [{:bar=>1, :baz=>'bb'}]
    @db.literal(p3.call('{"(1,b)"}')).should == "ARRAY[ROW(1, 'bb')::foo]::foo[]"
    @db.typecast_value(:foo_array, [{:bar=>'1', :baz=>'b'}]).should == [{:bar=>1, :baz=>'bb'}]
  end

  it "should allow creating unregisted row types via Database#row_type" do
    @db.literal(@db.row_type(:foo, [1, 2])).should == 'ROW(1, 2)::foo'
  end

  it "should allow typecasting of registered row types via Database#row_type" do
    @db.conversion_procs[4] = proc{|s| s.to_i}
    @db.conversion_procs[5] = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    @db.register_row_type(:foo, :typecaster=>proc{|h| @m::HashRow.subclass(:foo, [:bar, :baz]).new({:bar=>(h[:bar]||0).to_i, :baz=>(h[:baz] || 'a')*2})})
    @db.literal(@db.row_type(:foo, ['1', 'b'])).should == "ROW(1, 'bb')::foo"
    @db.literal(@db.row_type(:foo, {:bar=>'1', :baz=>'b'})).should == "ROW(1, 'bb')::foo"
  end

  it "should allow parsing when typecasting registered row types via Database#row_type" do
    @db.conversion_procs[4] = proc{|s| s.to_i}
    @db.conversion_procs[5] = proc{|s| s * 2}
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}], [{:attname=>'bar', :atttypid=>4}, {:attname=>'baz', :atttypid=>5}]]
    @db.register_row_type(:foo, :typecaster=>proc{|h| @m::HashRow.subclass(:foo, [:bar, :baz]).new(:bar=>(h[:bar]||0).to_i, :baz=>(h[:baz] || 'a')*2)})
    @db.literal(@db.row_type(:foo, ['1', 'b'])).should == "ROW(1, 'bb')::foo"
  end

  it "should raise an error if attempt to use Database#row_type with an unregistered type and hash" do
    proc{@db.literal(@db.row_type(:foo, {:bar=>'1', :baz=>'b'}))}.should raise_error(Sequel::Error)
  end

  it "should raise an error if attempt to use Database#row_type with an unhandled type" do
    proc{@db.literal(@db.row_type(:foo, 1))}.should raise_error(Sequel::Error)
  end

  it "should return ArrayRow and HashRow values as-is" do
    h = @m::HashRow.call(:a=>1)
    a = @m::ArrayRow.call([1])
    @db.row_type(:foo, h).should equal(h)
    @db.row_type(:foo, a).should equal(a)
  end

  it "should have Sequel.pg_row return a plain ArrayRow" do
    @db.literal(Sequel.pg_row([1, 2, 3])).should == 'ROW(1, 2, 3)'
  end

  it "should raise an error if attempting to typecast a hash for a parser without columns" do
    proc{@m::Parser.new.typecast(:a=>1)}.should raise_error(Sequel::Error)
  end

  it "should raise an error if attempting to typecast a unhandled value for a parser" do
    proc{@m::Parser.new.typecast(1)}.should raise_error(Sequel::Error)
  end

  it "should handle typecasting for a parser without a typecaster" do
    @m::Parser.new.typecast([1]).should == [1]
  end

  it "should raise an error if no columns are returned when registering a custom row type" do
    @db.fetch = [[{:oid=>1, :typrelid=>2, :typarray=>3}]]
    proc{@db.register_row_type(:foo)}.should raise_error(Sequel::Error)
  end

  it "should raise an error when registering a custom row type if the type is found found" do
    @db.fetch = []
    proc{@db.register_row_type(:foo)}.should raise_error(Sequel::Error)
  end
end
