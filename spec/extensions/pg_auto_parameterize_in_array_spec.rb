require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_auto_parameterize_in_array extension" do
  def db(opts={:treat_string_list_as_text_array => "t"})
    @db = Sequel.connect('mock://postgres')
    @db.synchronize{|c| def c.escape_bytea(v) v*2 end}
    @db.opts.merge!(opts)
    @db.extension :pg_auto_parameterize_in_array
  end

  before do
    @db = db
  end

  types = [
    ["strings if treat_string_list_as_text_array Database option is true", proc{|x| x.to_s}, "text"],
    ["BigDecimals", proc{|x| BigDecimal(x)}, "numeric"],
    ["dates", proc{|x| Date.new(2021, x)}, "date"],
    ["times", proc{|x| Time.local(2021, x)}, "timestamp"],
    ["SQLTimes", proc{|x| Sequel::SQLTime.create(x, 0, 0)}, "time"],
    ["datetimes", proc{|x| DateTime.new(2021, x)}, "timestamp"],
    ["floats", proc{|x| Float(x)}, "numeric"],
    ["blobs", proc{|x| Sequel.blob(x.to_s)}, "bytea"],
  ]

  if RUBY_VERSION >= '2.4'
    types << ["integers", proc{|x| x}, "int8"]
  else
    it "should fallback to pg_auto_parameterize extension behavior when switching column IN/NOT IN to = ANY/!= ALL for integers" do
      v = [1, 2, 3]
      nv = [1, nil, 3]
      set = Set[1, 2, 3]
      type = "int8"

      sql = @db[:table].where(:a=>v).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY(CAST($1 AS #{type}[])))'
      sql.args.must_equal ['{1,2,3}']

      sql = @db[:table].where(:a=>set).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY(CAST($1 AS #{type}[])))'
      sql.args.must_equal ['{1,2,3}']

      sql = @db[:table].where(:a=>nv).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY(CAST($1 AS #{type}[])))'
      sql.args.must_equal ['{1,NULL,3}']

      sql = @db[:table].exclude(:a=>v).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" != ALL(CAST($1 AS #{type}[])))'
      sql.args.must_equal ['{1,2,3}']

      sql = @db[:table].exclude(:a=>set).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" != ALL(CAST($1 AS #{type}[])))'
      sql.args.must_equal ['{1,2,3}']

      sql = @db[:table].exclude(:a=>nv).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" != ALL(CAST($1 AS #{type}[])))'
      sql.args.must_equal ['{1,NULL,3}']
    end
  end

  types.each do |desc, conv, type|
    it "should automatically switch column IN/NOT IN to = ANY/!= ALL for #{desc}" do
      v = [1,2,3].map(&conv)
      set = v.to_set
      nv = (v + [nil]).freeze

      sql = @db[:table].where(:a=>v).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY($1::#{type}[]))'
      sql.args.must_equal [v]

      sql = @db[:table].where(:a=>set).sql
      sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY($1::#{type}[]))'
      sql.args.must_equal [v]

      sql = @db[:table].where(:a=>nv).sql
      sql.must_equal %'SELECT * FROM "table" WHERE ("a" = ANY($1::#{type}[]))'
      sql.args.must_equal [nv]

      sql = @db[:table].exclude(:a=>v).sql
      sql.must_equal %'SELECT * FROM "table" WHERE ("a" != ALL($1::#{type}[]))'
      sql.args.must_equal [v]

      sql = @db[:table].exclude(:a=>set).sql
      sql.must_equal %'SELECT * FROM "table" WHERE ("a" != ALL($1::#{type}[]))'
      sql.args.must_equal [v]

      sql = @db[:table].exclude(:a=>nv).sql
      sql.must_equal %'SELECT * FROM "table" WHERE ("a" != ALL($1::#{type}[]))'
      sql.args.must_equal [nv]
    end
  end

  it "should automatically switch column IN/NOT IN to = ANY/!= ALL without array cast for strings if treat_string_list_as_untyped_array Database option is true" do
    @db = db(:treat_string_list_as_untyped_array => 't')
    v = %w[1 2]
    nv = (v + [nil]).freeze

    sql = @db[:table].where(:a=>v).sql
    sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY($1))'
    sql.args.must_equal [v]

    sql = @db[:table].where(:a=>nv).sql
    sql.must_equal %'SELECT * FROM "table" WHERE ("a" = ANY($1))'
    sql.args.must_equal [nv]

    sql = @db[:table].exclude(:a=>v).sql
    sql.must_equal %'SELECT * FROM "table" WHERE ("a" != ALL($1))'
    sql.args.must_equal [v]

    sql = @db[:table].exclude(:a=>nv).sql
    sql.must_equal %'SELECT * FROM "table" WHERE ("a" != ALL($1))'
    sql.args.must_equal [nv]

    # Check that other arrays are still typed
    v = [1,2,3].map{|x| Date.new(2021, x)}
    sql = @db[:table].where(:a=>v).sql
    sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY($1::date[]))'
    sql.args.must_equal [v]
  end

  it "should automatically switch column IN/NOT IN to = ANY/!= ALL for infinite/NaN floats" do
    v = [1.0, 1.0/0.0, -1.0/0.0, 0.0/0.0]
    nv = (v + [nil]).freeze
    type = "double precision"

    sql = @db[:table].where(:a=>v).sql
    sql.must_equal %'SELECT * FROM \"table\" WHERE ("a" = ANY($1::#{type}[]))'
    sql.args.must_equal [v]

    sql = @db[:table].where(:a=>nv).sql
    sql.must_equal %'SELECT * FROM "table" WHERE ("a" = ANY($1::#{type}[]))'
    sql.args.must_equal [nv]

    sql = @db[:table].exclude(:a=>v).sql
    sql.must_equal %'SELECT * FROM "table" WHERE ("a" != ALL($1::#{type}[]))'
    sql.args.must_equal [v]

    sql = @db[:table].exclude(:a=>nv).sql
    sql.must_equal %'SELECT * FROM "table" WHERE ("a" != ALL($1::#{type}[]))'
    sql.args.must_equal [nv]
  end

  it "should not automatically switch column IN/NOT IN to = ANY/!= ALL for strings by default" do
    @db = db({})
    v = %w'1 2'
    sql = @db[:table].where([:a, :b]=>v).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") IN ($1, $2))'
    sql.args.must_equal v

    sql = @db[:table].exclude([:a, :b]=>v).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") NOT IN ($1, $2))'
    sql.args.must_equal v
  end

  it "should not convert IN/NOT IN expressions that use unsupported types" do
    v = [Sequel.lit('1'), Sequel.lit('2')].freeze
    sql = @db[:table].where([:a, :b]=>v).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") IN (1, 2))'
    sql.args.must_be_nil

    sql = @db[:table].exclude([:a, :b]=>v).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") NOT IN (1, 2))'
    sql.args.must_be_nil
  end

  it "should not convert multiple column IN expressions" do
    sql = @db[:table].where([:a, :b]=>[[1.0, 2.0]]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") IN (($1::numeric, $2::numeric)))'
    sql.args.must_equal [1, 2]

    sql = @db[:table].exclude([:a, :b]=>[[1.0, 2.0]]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") NOT IN (($1::numeric, $2::numeric)))'
    sql.args.must_equal [1, 2]
  end

  it "should not convert single value expressions" do
    sql = @db[:table].where(:a=>[1.0]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" IN ($1::numeric))'
    sql.args.must_equal [1]

    sql = @db[:table].exclude(:a=>[1.0]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" NOT IN ($1::numeric))'
    sql.args.must_equal [1]
  end

  it "should convert single value expressions in pg_auto_parameterize_min_array_size: 1" do
    @db = db(:pg_auto_parameterize_min_array_size => 1)
    sql = @db[:table].where(:a=>[1.0]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" = ANY($1::numeric[]))'
    sql.args.must_equal [[1]]

    sql = @db[:table].exclude(:a=>[1.0]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" != ALL($1::numeric[]))'
    sql.args.must_equal [[1]]
  end

  it "should not convert expressions with mixed types" do
    sql = @db[:table].where(:a=>[1, 2.0]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" IN ($1::int4, $2::numeric))'
    sql.args.must_equal [1, 2.0]

    sql = @db[:table].where(:a=>[1, 2.0]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" IN ($1::int4, $2::numeric))'
    sql.args.must_equal [1, 2.0]
  end

  it "should not convert other expressions" do
    sql = @db[:table].where(:a=>1).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" = $1::int4)'
    sql.args.must_equal [1]

    sql = @db[:table].where(:a=>@db[:table]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" IN (SELECT * FROM "table"))'
    sql.args.must_be_nil
  end
end
