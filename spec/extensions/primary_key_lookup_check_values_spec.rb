require_relative "spec_helper"

describe "primary_key_lookup_check_values plugin" do
  int_pk_schema = {:id=>{:primary_key=>true, :type=>:integer, :min_value=>0, :max_value=>10}.freeze}.freeze

  def model(schema)
    fetch = {}
    schema.keys.each_with_index{|k,i| fetch[k] = i+1}
    db = Sequel.mock(:fetch=>fetch)
    schema = schema.to_a
    db.define_singleton_method(:schema){|*| schema}
    def db.supports_schema_parsing?; true end
    c = Class.new(Sequel::Model(db[:a]))
    c.plugin :primary_key_lookup_check_values
    db.sqls
    c
  end

  it "should be loadable on a model class without a dataset" do
    db = model(int_pk_schema).db
    c = Class.new(Sequel::Model(db))
    c.plugin :primary_key_lookup_check_values
    c.dataset = :a
    c.db.sqls
    c['1'].must_equal c.load(:id=>1)
    db.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
  end

  it "should work in subclasses" do
    c = Class.new(model(int_pk_schema))
    c.db.sqls
    c['1'].must_equal c.load(:id=>1)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]

    c.dataset = Sequel.mock(:fetch=>{:id=>1})[:a]
    c.db.sqls
    c['1'].must_equal c.load(:id=>1)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE id = '1'"]
  end

  it "should handle lookup for single primary key if database type is not known" do
    c = model(:id=>{:primary_key=>true})
    c['1'].must_equal c.load(:id=>1)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE id = '1'"]
  end

  it "should handle lookup for single primary key when value can be type checked but not value checked" do
    c = model(:id=>{:primary_key=>true, :type=>:integer})
    c['1'].must_equal c.load(:id=>1)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
  end

  it "should handle lookup for composite primary key if no columns can be type checked" do
    c = model(:id1=>{:primary_key=>true}, :id2=>{:primary_key=>true})
    c[['1', '2']].must_equal c.load(:id1=>1, :id2=>2)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE ((id1 = '1') AND (id2 = '2')) LIMIT 1"]
  end

  it "should handle lookup for composite primary key where only a subset of columns can be type checked" do
    c = model(:id1=>{:primary_key=>true, :type=>:integer}, :id2=>{:primary_key=>true})
    c[['a', '2']].must_be_nil
    c.db.sqls.must_equal []
    c[['1', '2']].must_equal c.load(:id1=>1, :id2=>2)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE ((id1 = 1) AND (id2 = '2')) LIMIT 1"]
  end

  it "should handle lookup for composite primary key where only a subset of columns can be type checked and value checked" do
    c = model(:id1=>{:primary_key=>true, :type=>:integer, :min_value=>0, :max_value=>10}, :id2=>{:primary_key=>true})
    c[['a', '2']].must_be_nil
    c.db.sqls.must_equal []
    c[['12', '2']].must_be_nil
    c.db.sqls.must_equal []
    c[['1', '2']].must_equal c.load(:id1=>1, :id2=>2)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE ((id1 = 1) AND (id2 = '2')) LIMIT 1"]
  end

  it "should handle lookup for composite primary key where only a subset of columns can be value checked" do
    c = model(:id1=>{:primary_key=>true, :type=>:integer, :min_value=>0, :max_value=>10}, :id2=>{:primary_key=>true, :type=>:integer})
    c[['a', '2']].must_be_nil
    c.db.sqls.must_equal []
    c[['12', '2']].must_be_nil
    c.db.sqls.must_equal []
    c[['1', '2']].must_equal c.load(:id1=>1, :id2=>2)
    c.db.sqls.must_equal ["SELECT * FROM a WHERE ((id1 = 1) AND (id2 = 2)) LIMIT 1"]
  end

  describe "with single pk with type and value" do
    before do
      @c = model(int_pk_schema)
      @c.db.sqls
    end

    it "should work when setting the dataset for an existing class" do
      @c.dataset = :b
      @c.db.sqls
      @c['1'].must_equal @c.load(:id=>1)
      @c.db.sqls.must_equal ["SELECT * FROM b WHERE id = 1"]
    end

    it "should skip query when nil is given" do
      @c[nil].must_be_nil
      @c.db.sqls.must_equal []
    end

    it "should skip query for single primary key with array value" do
      @c[[1]].must_be_nil
      @c.db.sqls.must_equal []
    end

    it "should skip query for single primary key when typecasting fails" do
      @c['a'].must_be_nil
      @c.db.sqls.must_equal []
    end

    it "should skip query for single primary key when value check fails" do
      @c[12].must_be_nil
      @c[-1].must_be_nil
      @c.db.sqls.must_equal []
    end

    it "should handle lookup for single primary key when value can be type and value checked" do
      @c['1']
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
    end

    it "should handle lookup when given value is a symbol" do
      @c[:b]
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE id = b"]
    end

    it "should handle lookup when given value is an literal string" do
      @c[Sequel.lit('b')]
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE id = b"]
    end

    it "should handle lookup when given value is an SQL expression" do
      @c[Sequel.identifier('b')]
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE id = b"]
    end

    it "should affect Model.with_pk lookups" do
      @c.with_pk(nil).must_be_nil
      @c.db.sqls.must_equal []
      @c.with_pk('1').must_equal @c.load(:id=>1)
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
    end

    it "should affect Model.with_pk! lookups" do
      proc{@c.with_pk!(nil)}.must_raise Sequel::NoMatchingRow
      @c.db.sqls.must_equal []
      @c.with_pk!('1').must_equal @c.load(:id=>1)
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
    end

    it "should affect Model Dataset#[] lookups with integers" do
      @c.dataset[12].must_be_nil
      @c.db.sqls.must_equal []
      @c.dataset[1].must_equal @c.load(:id=>1)
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
    end

    it "should affect Model Dataset#with_pk lookups" do
      @c.dataset.with_pk(nil).must_be_nil
      @c.db.sqls.must_equal []
      @c.dataset.with_pk('1').must_equal @c.load(:id=>1)
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
    end

    it "should affect Model Dataset#with_pk! lookups" do
      proc{@c.dataset.with_pk!(nil)}.must_raise Sequel::NoMatchingRow
      @c.db.sqls.must_equal []
      @c.dataset.with_pk!('1').must_equal @c.load(:id=>1)
      @c.db.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
    end
  end

  describe "with composite pk with type and value for all columns" do
    comp_pk_schema = {
      :id1=>{:primary_key=>true, :type=>:integer, :min_value=>0, :max_value=>10}.freeze,
      :id2=>{:primary_key=>true, :type=>:integer, :min_value=>12, :max_value=>20}.freeze
    }.freeze

    before do
      @ds = model(comp_pk_schema).dataset
    end

    it "should skip query for composite primary key with non-array value" do
      @ds.with_pk(1).must_be_nil
      @ds.db.sqls.must_equal []
    end

    it "should skip query for composite primary key with incorrect array value size" do
      @ds.with_pk([1]).must_be_nil
      @ds.with_pk([1, 2, 3]).must_be_nil
      @ds.db.sqls.must_equal []
    end

    it "should skip query for composite primary key with nil value" do
      @ds.with_pk([nil, nil]).must_be_nil
      @ds.with_pk([nil, 14]).must_be_nil
      @ds.with_pk([1, nil]).must_be_nil
      @ds.db.sqls.must_equal []
    end

    it "should skip query for composite primary key where typecasting fails for any value" do
      @ds.with_pk(['a', 'a']).must_be_nil
      @ds.with_pk(['a', 14]).must_be_nil
      @ds.with_pk([1, 'a']).must_be_nil
      @ds.db.sqls.must_equal []
    end

    it "should skip query for composite primary key where where value check fails for any value" do
      @ds.with_pk([-1, 5]).must_be_nil
      @ds.with_pk([5, 5]).must_be_nil
      @ds.with_pk([5, 25]).must_be_nil
      @ds.with_pk([-1, 14]).must_be_nil
      @ds.with_pk([11, 14]).must_be_nil
      @ds.db.sqls.must_equal []
    end

    it "should handle lookup for composite primary key when all values can be type and value checked" do
      @ds.with_pk(['5', '15']).must_equal @ds.model.load(:id1=>1, :id2=>2)
      @ds.db.sqls.must_equal ["SELECT * FROM a WHERE ((a.id1 = 5) AND (a.id2 = 15)) LIMIT 1"]
    end
  end
end
