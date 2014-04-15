require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Model()" do
  before do
    @db = Sequel::Model.db
  end

  it "should return a model subclass with the given dataset if given a dataset" do
    ds = @db[:blah]
    c = Sequel::Model(ds)
    c.superclass.should == Sequel::Model
    c.dataset.should == ds
  end

  it "should return a model subclass with a dataset with the default database and given table name if given a Symbol" do
    c = Sequel::Model(:blah)
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == :blah
  end

  it "should return a model subclass with a dataset with the default database and given table name if given a LiteralString" do
    c = Sequel::Model(Sequel.lit('blah'))
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == Sequel.lit('blah')
  end

  it "should return a model subclass with a dataset with the default database and given table name if given an SQL::Identifier" do
    c = Sequel::Model(Sequel.identifier(:blah))
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == Sequel.identifier(:blah)
  end

  it "should return a model subclass with a dataset with the default database and given table name if given an SQL::QualifiedIdentifier" do
    c = Sequel::Model(Sequel.qualify(:boo, :blah))
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == Sequel.qualify(:boo, :blah)
  end

  it "should return a model subclass with a dataset with the default database and given table name if given an SQL::AliasedExpression" do
    c = Sequel::Model(Sequel.as(:blah, :boo))
    c.superclass.should == Sequel::Model
    c.db.should == @db
    c.table_name.should == :boo
  end

  it "should return a model subclass with the given dataset if given a dataset using an SQL::Identifier" do
    ds = @db[Sequel.identifier(:blah)]
    c = Sequel::Model(ds)
    c.superclass.should == Sequel::Model
    c.dataset.should == ds
  end

  it "should return a model subclass associated to the given database if given a database" do
    db = Sequel.mock
    c = Sequel::Model(db)
    c.superclass.should == Sequel::Model
    c.db.should == db
    proc{c.dataset}.should raise_error(Sequel::Error)
    class SmBlahTest < c
    end
    SmBlahTest.db.should == db
    SmBlahTest.table_name.should == :sm_blah_tests
  end

  describe "reloading" do
    before do
      Sequel.cache_anonymous_models = true
    end
    after do
      Sequel.cache_anonymous_models = false
      Object.send(:remove_const, :Album) if defined?(::Album)
    end

    it "should work without raising an exception with a symbol" do
      proc do
        class ::Album < Sequel::Model(:table); end
        class ::Album < Sequel::Model(:table); end
      end.should_not raise_error
    end

    it "should work without raising an exception with an SQL::Identifier " do
      proc do
        class ::Album < Sequel::Model(Sequel.identifier(:table)); end
        class ::Album < Sequel::Model(Sequel.identifier(:table)); end
      end.should_not raise_error
    end

    it "should work without raising an exception with an SQL::QualifiedIdentifier " do
      proc do
        class ::Album < Sequel::Model(Sequel.qualify(:schema, :table)); end
        class ::Album < Sequel::Model(Sequel.qualify(:schema, :table)); end
      end.should_not raise_error
    end

    it "should work without raising an exception with an SQL::AliasedExpression" do
      proc do
        class ::Album < Sequel::Model(Sequel.as(:table, :alias)); end
        class ::Album < Sequel::Model(Sequel.as(:table, :alias)); end
      end.should_not raise_error
    end

    it "should work without raising an exception with an LiteralString" do
      proc do
        class ::Album < Sequel::Model(Sequel.lit('table')); end
        class ::Album < Sequel::Model(Sequel.lit('table')); end
      end.should_not raise_error
    end

    it "should work without raising an exception with a database" do
      proc do
        class ::Album < Sequel::Model(@db); end
        class ::Album < Sequel::Model(@db); end
      end.should_not raise_error
    end

    it "should work without raising an exception with a dataset" do
      proc do
        class ::Album < Sequel::Model(@db[:table]); end
        class ::Album < Sequel::Model(@db[:table]); end
      end.should_not raise_error
    end

    it "should work without raising an exception with a dataset with an SQL::Identifier" do
      proc do
        class ::Album < Sequel::Model(@db[Sequel.identifier(:table)]); end
        class ::Album < Sequel::Model(@db[Sequel.identifier(:table)]); end
      end.should_not raise_error
    end

    it "should raise an exception if anonymous model caching is disabled" do
      Sequel.cache_anonymous_models = false
      proc do
        class ::Album < Sequel::Model(@db[Sequel.identifier(:table)]); end
        class ::Album < Sequel::Model(@db[Sequel.identifier(:table)]); end
      end.should raise_error
    end
  end
end

describe Sequel::Model do
  it "should have class method aliased as model" do
    Sequel::Model.instance_methods.collect{|x| x.to_s}.should include("model")

    model_a = Class.new(Sequel::Model(:items))
    model_a.new.model.should be(model_a)
  end

  it "should be associated with a dataset" do
    model_a = Class.new(Sequel::Model) { set_dataset DB[:as] }

    model_a.dataset.should be_a_kind_of(Sequel::Mock::Dataset)
    model_a.dataset.opts[:from].should == [:as]

    model_b = Class.new(Sequel::Model) { set_dataset DB[:bs] }

    model_b.dataset.should be_a_kind_of(Sequel::Mock::Dataset)
    model_b.dataset.opts[:from].should == [:bs]

    model_a.dataset.opts[:from].should == [:as]
  end
end

describe Sequel::Model do
  before do
    @model = Class.new(Sequel::Model(:items))
  end

  it "has table_name return name of table" do
    @model.table_name.should == :items
  end

  it "defaults to primary key of id" do
    @model.primary_key.should == :id
  end

  it "allow primary key change" do
    @model.set_primary_key :ssn
    @model.primary_key.should == :ssn
  end

  it "allows dataset change" do
    @model.set_dataset(DB[:foo])
    @model.table_name.should == :foo
  end

  it "allows set_dataset to accept a Symbol" do
    @model.db = DB
    @model.set_dataset(:foo)
    @model.table_name.should == :foo
  end

  it "allows set_dataset to accept a LiteralString" do
    @model.db = DB
    @model.set_dataset(Sequel.lit('foo'))
    @model.table_name.should == Sequel.lit('foo')
  end

  it "allows set_dataset to acceptan SQL::Identifier" do
    @model.db = DB
    @model.set_dataset(Sequel.identifier(:foo))
    @model.table_name.should == Sequel.identifier(:foo)
  end

  it "allows set_dataset to acceptan SQL::QualifiedIdentifier" do
    @model.db = DB
    @model.set_dataset(Sequel.qualify(:bar, :foo))
    @model.table_name.should == Sequel.qualify(:bar, :foo)
  end

  it "allows set_dataset to acceptan SQL::AliasedExpression" do
    @model.db = DB
    @model.set_dataset(Sequel.as(:foo, :bar))
    @model.table_name.should == :bar
  end

  it "table_name should respect table aliases" do
    @model.set_dataset(:foo___x)
    @model.table_name.should == :x
  end
  
  it "set_dataset should raise an error unless given a Symbol or Dataset" do
    proc{@model.set_dataset(Object.new)}.should raise_error(Sequel::Error)
  end

  it "set_dataset should add the destroy method to the dataset that destroys each object" do
    ds = DB[:foo]
    ds.should_not respond_to(:destroy)
    @model.set_dataset(ds)
    ds.should respond_to(:destroy)
    DB.sqls
    ds._fetch = [{:id=>1}, {:id=>2}]
    ds.destroy.should == 2
    DB.sqls.should == ["SELECT * FROM foo", "DELETE FROM foo WHERE id = 1", "DELETE FROM foo WHERE id = 2"]
  end

  it "set_dataset should add the destroy method that respects sharding with transactions" do
    db = Sequel.mock(:servers=>{:s1=>{}})
    ds = db[:foo].server(:s1)
    @model.use_transactions = true
    @model.set_dataset(ds)
    db.sqls
    ds.destroy.should == 0
    db.sqls.should == ["BEGIN -- s1", "SELECT * FROM foo -- s1", "COMMIT -- s1"]
  end

  it "should raise an error on set_dataset if there is an error connecting to the database" do
    def @model.columns() raise Sequel::DatabaseConnectionError end
    proc{@model.set_dataset(Sequel::Database.new[:foo].join(:blah))}.should raise_error
  end

  it "should not raise an error if there is a problem getting the columns for a dataset" do
    def @model.columns() raise Sequel::Error end
    proc{@model.set_dataset(DB[:foo].join(:blah))}.should_not raise_error
  end

  it "doesn't raise an error on set_dataset if there is an error raised getting the schema" do
    def @model.get_db_schema(*) raise Sequel::Error end
    proc{@model.set_dataset(DB[:foo])}.should_not raise_error
  end

  it "doesn't raise an error on inherited if there is an error setting the dataset" do
    def @model.set_dataset(*) raise Sequel::Error end
    proc{Class.new(@model)}.should_not raise_error
  end

  it "should raise if bad inherited instance variable value is used" do
    def @model.inherited_instance_variables() super.merge(:@a=>:foo) end
    @model.instance_eval{@a=1}
    proc{Class.new(@model)}.should raise_error(Sequel::Error)
  end

  it "copy inherited instance variables into subclass if set" do
    def @model.inherited_instance_variables() super.merge(:@a=>nil, :@b=>:dup, :@c=>:hash_dup, :@d=>proc{|v| v * 2}) end
    @model.instance_eval{@a=1; @b=[2]; @c={3=>[4]}; @d=10}
    m = Class.new(@model)
    @model.instance_eval{@a=5; @b << 6; @c[3] << 7; @c[8] = [9]; @d=40}
    m.instance_eval do
      @a.should == 1
      @b.should == [2]
      @c.should == {3=>[4]}
      @d.should == 20
    end
  end
end

describe Sequel::Model, "constructors" do
  before do
    @m = Class.new(Sequel::Model)
    @m.columns :a, :b
  end

  it "should accept a hash" do
    m = @m.new(:a => 1, :b => 2)
    m.values.should == {:a => 1, :b => 2}
    m.should be_new
  end
  
  it "should accept a block and yield itself to the block" do
    block_called = false
    m = @m.new {|i| block_called = true; i.should be_a_kind_of(@m); i.values[:a] = 1}
    
    block_called.should == true
    m.values[:a].should == 1
  end
  
  it "should have dataset row_proc create an existing object" do
    @m.dataset = Sequel.mock.dataset
    o = @m.dataset.row_proc.call(:a=>1)
    o.should be_a_kind_of(@m)
    o.values.should == {:a=>1}
    o.new?.should == false
  end
  
  it "should have .call create an existing object" do
    o = @m.call(:a=>1)
    o.should be_a_kind_of(@m)
    o.values.should == {:a=>1}
    o.new?.should == false
  end
  
  it "should have .load create an existing object" do
    o = @m.load(:a=>1)
    o.should be_a_kind_of(@m)
    o.values.should == {:a=>1}
    o.new?.should == false
  end
end

describe Sequel::Model, "new" do
  before do
    @m = Class.new(Sequel::Model) do
      set_dataset DB[:items]
      columns :x, :id
    end
  end

  it "should be marked as new?" do
    o = @m.new
    o.should be_new
  end

  it "should not be marked as new? once it is saved" do
    o = @m.new(:x => 1)
    o.should be_new
    o.save
    o.should_not be_new
  end

  it "should use the last inserted id as primary key if not in values" do
    @m.instance_dataset._fetch = @m.dataset._fetch = {:x => 1, :id => 1234}
    @m.instance_dataset.autoid = @m.dataset.autoid = 1234

    o = @m.new(:x => 1)
    o.save
    o.id.should == 1234

    o = @m.load(:x => 1, :id => 333)
    o.save
    o.id.should == 333
  end
end

describe Sequel::Model, ".subset" do
  before do
    @c = Class.new(Sequel::Model(:items))
    DB.reset
  end

  specify "should create a filter on the underlying dataset" do
    proc {@c.new_only}.should raise_error(NoMethodError)
    
    @c.subset(:new_only){age < 'new'}
    
    @c.new_only.sql.should == "SELECT * FROM items WHERE (age < 'new')"
    @c.dataset.new_only.sql.should == "SELECT * FROM items WHERE (age < 'new')"
    
    @c.subset(:pricey){price > 100}
    
    @c.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    @c.dataset.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    
    @c.pricey.new_only.sql.should == "SELECT * FROM items WHERE ((price > 100) AND (age < 'new'))"
    @c.new_only.pricey.sql.should == "SELECT * FROM items WHERE ((age < 'new') AND (price > 100))"
  end

  specify "should not override existing model methods" do
    def @c.active() true end
    @c.subset(:active, :active)
    @c.active.should == true
  end
end

describe Sequel::Model, ".find" do
  before do
    @c = Class.new(Sequel::Model(:items))
    @c.dataset._fetch = {:name => 'sharon', :id => 1}
    DB.reset
  end
  
  it "should return the first record matching the given filter" do
    @c.find(:name => 'sharon').should be_a_kind_of(@c)
    DB.sqls.should == ["SELECT * FROM items WHERE (name = 'sharon') LIMIT 1"]

    @c.find(Sequel.expr(:name).like('abc%')).should be_a_kind_of(@c)
    DB.sqls.should == ["SELECT * FROM items WHERE (name LIKE 'abc%' ESCAPE '\\') LIMIT 1"]
  end
  
  specify "should accept filter blocks" do
    @c.find{id > 1}.should be_a_kind_of(@c)
    DB.sqls.should == ["SELECT * FROM items WHERE (id > 1) LIMIT 1"]

    @c.find{(x > 1) & (y < 2)}.should be_a_kind_of(@c)
    DB.sqls.should == ["SELECT * FROM items WHERE ((x > 1) AND (y < 2)) LIMIT 1"]
  end
end

describe Sequel::Model, ".finder" do
  before do
    @h = {:id=>1}
    @db = Sequel.mock(:fetch=>@h)
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.instance_eval do
      def foo(a, b)
        where(:bar=>a).order(b)
      end
    end
    @o = @c.load(@h)
    @db.sqls
  end

  specify "should create a method that calls the method given and returns the first instance" do
    @c.finder :foo
    @c.first_foo(1, 2).should == @o
    @c.first_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4 LIMIT 1"]
  end

  specify "should work correctly when subclassing" do
    @c.finder(:foo)
    @sc = Class.new(@c)
    @sc.set_dataset :foos
    @db.sqls
    @sc.first_foo(1, 2).should == @sc.load(@h)
    @sc.first_foo(3, 4).should == @sc.load(@h)
    @db.sqls.should == ["SELECT * FROM foos WHERE (bar = 1) ORDER BY 2 LIMIT 1", "SELECT * FROM foos WHERE (bar = 3) ORDER BY 4 LIMIT 1"]
  end

  specify "should work correctly when dataset is modified" do
    @c.finder(:foo)
    @c.first_foo(1, 2).should == @o
    @c.set_dataset :foos
    @c.first_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1", "SELECT * FROM foos LIMIT 1", "SELECT * FROM foos WHERE (bar = 3) ORDER BY 4 LIMIT 1"]
  end

  specify "should create a method based on the given block if no method symbol provided" do
    @c.finder(:name=>:first_foo){|pl, ds| ds.where(pl.arg).limit(1)}
    @c.first_foo(:id=>1).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (id = 1) LIMIT 1"]
  end

  specify "should raise an error if both a block and method symbol given" do
    proc{@c.finder(:foo, :name=>:first_foo){|pl, ds| ds.where(pl.arg)}}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if two option hashes are provided" do
    proc{@c.finder({:name2=>:foo}, :name=>:first_foo){|pl, ds| ds.where(pl.arg)}}.should raise_error(Sequel::Error)
  end

  specify "should support :type option" do
    @c.finder :foo, :type=>:all
    @c.finder :foo, :type=>:each
    @c.finder :foo, :type=>:get

    a = []
    @c.all_foo(1, 2){|r| a << r}.should == [@o]
    a.should == [@o]
   
    a = []
    @c.each_foo(3, 4){|r| a << r}
    a.should == [@o]

    @c.get_foo(5, 6).should == [:id, 1]

    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4", "SELECT * FROM items WHERE (bar = 5) ORDER BY 6 LIMIT 1"]
  end

  specify "should support :name option" do
    @c.finder :foo, :name=>:find_foo
    @c.find_foo(1, 2).should == @o
    @c.find_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4 LIMIT 1"]
  end

  specify "should support :arity option" do
    def @c.foobar(*b)
      ds = dataset
      b.each_with_index do |a, i|
        ds = ds.where(i=>a)
      end
      ds
    end
    @c.finder :foobar, :arity=>1, :name=>:find_foobar_1
    @c.finder :foobar, :arity=>2, :name=>:find_foobar_2
    @c.find_foobar_1(:a)
    @c.find_foobar_2(:a, :b)
    @db.sqls.should == ["SELECT * FROM items WHERE (0 = a) LIMIT 1", "SELECT * FROM items WHERE ((0 = a) AND (1 = b)) LIMIT 1"]
  end

  specify "should support :mod option" do
    m = Module.new
    @c.finder :foo, :mod=>m
    proc{@c.first_foo}.should raise_error
    @c.extend m
    @c.first_foo(1, 2).should == @o
    @c.first_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4 LIMIT 1"]
  end

  specify "should raise error when calling with the wrong arity" do
    @c.finder :foo
    proc{@c.first_foo(1)}.should raise_error
    proc{@c.first_foo(1,2,3)}.should raise_error
  end
end

describe Sequel::Model, ".prepared_finder" do
  before do
    @h = {:id=>1}
    @db = Sequel.mock(:fetch=>@h)
    @db.extend_datasets do
      def select_sql
        sql = super
        sql << ' -- prepared' if is_a?(Sequel::Dataset::PreparedStatementMethods)
        sql
      end
    end
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.instance_eval do
      def foo(a, b)
        where(:bar=>a).order(b)
      end
    end
    @o = @c.load(@h)
    @db.sqls
  end

  specify "should create a method that calls the method given and returns the first instance" do
    @c.prepared_finder :foo
    @c.first_foo(1, 2).should == @o
    @c.first_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1 -- prepared", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4 LIMIT 1 -- prepared"]
  end

  specify "should work correctly when subclassing" do
    @c.prepared_finder(:foo)
    @sc = Class.new(@c)
    @sc.set_dataset :foos
    @db.sqls
    @sc.first_foo(1, 2).should == @sc.load(@h)
    @sc.first_foo(3, 4).should == @sc.load(@h)
    @db.sqls.should == ["SELECT * FROM foos WHERE (bar = 1) ORDER BY 2 LIMIT 1 -- prepared", "SELECT * FROM foos WHERE (bar = 3) ORDER BY 4 LIMIT 1 -- prepared"]
  end

  specify "should work correctly when dataset is modified" do
    @c.prepared_finder(:foo)
    @c.first_foo(1, 2).should == @o
    @c.set_dataset :foos
    @c.first_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1 -- prepared", "SELECT * FROM foos LIMIT 1", "SELECT * FROM foos WHERE (bar = 3) ORDER BY 4 LIMIT 1 -- prepared"]
  end

  specify "should create a method based on the given block if no method symbol provided" do
    @c.prepared_finder(:name=>:first_foo){|a1| where(:id=>a1).limit(1)}
    @c.first_foo(1).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (id = 1) LIMIT 1 -- prepared"]
  end

  specify "should raise an error if both a block and method symbol given" do
    proc{@c.prepared_finder(:foo, :name=>:first_foo){|pl, ds| ds.where(pl.arg)}}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if two option hashes are provided" do
    proc{@c.prepared_finder({:name2=>:foo}, :name=>:first_foo){|pl, ds| ds.where(pl.arg)}}.should raise_error(Sequel::Error)
  end

  specify "should support :type option" do
    @c.prepared_finder :foo, :type=>:all
    @c.prepared_finder :foo, :type=>:each

    a = []
    @c.all_foo(1, 2){|r| a << r}.should == [@o]
    a.should == [@o]
   
    a = []
    @c.each_foo(3, 4){|r| a << r}
    a.should == [@o]

    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 -- prepared", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4 -- prepared"]
  end

  specify "should support :name option" do
    @c.prepared_finder :foo, :name=>:find_foo
    @c.find_foo(1, 2).should == @o
    @c.find_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1 -- prepared", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4 LIMIT 1 -- prepared"]
  end

  specify "should support :arity option" do
    def @c.foobar(*b)
      ds = dataset
      b.each_with_index do |a, i|
        ds = ds.where(i=>a)
      end
      ds
    end
    @c.prepared_finder :foobar, :arity=>1, :name=>:find_foobar_1
    @c.prepared_finder :foobar, :arity=>2, :name=>:find_foobar_2
    @c.find_foobar_1(:a)
    @c.find_foobar_2(:a, :b)
    @db.sqls.should == ["SELECT * FROM items WHERE (0 = a) LIMIT 1 -- prepared", "SELECT * FROM items WHERE ((0 = a) AND (1 = b)) LIMIT 1 -- prepared"]
  end

  specify "should support :mod option" do
    m = Module.new
    @c.prepared_finder :foo, :mod=>m
    proc{@c.first_foo}.should raise_error
    @c.extend m
    @c.first_foo(1, 2).should == @o
    @c.first_foo(3, 4).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1 -- prepared", "SELECT * FROM items WHERE (bar = 3) ORDER BY 4 LIMIT 1 -- prepared"]
  end

  specify "should handle models with names" do
    def @c.name; 'foobar' end
    @c.prepared_finder :foo
    @c.first_foo(1, 2).should == @o
    @db.sqls.should == ["SELECT * FROM items WHERE (bar = 1) ORDER BY 2 LIMIT 1 -- prepared"]
  end
end

describe Sequel::Model, ".fetch" do
  before do
    DB.reset
    @c = Class.new(Sequel::Model(:items))
  end
  
  it "should return instances of Model" do
    @c.fetch("SELECT * FROM items").first.should be_a_kind_of(@c)
  end

  it "should return true for .empty? and not raise an error on empty selection" do
    rows = @c.fetch("SELECT * FROM items WHERE FALSE")
    @c.send(:define_method, :fetch_rows){|sql| yield({:count => 0})}
    proc {rows.empty?}.should_not raise_error
  end
end

describe Sequel::Model, ".find_or_create" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:items])) do
      set_primary_key :id
      columns :x
    end
    @db.sqls
  end

  it "should find the record" do
    @db.fetch = [{:x=>1, :id=>1}]
    @db.autoid = 1
    @c.find_or_create(:x => 1).should == @c.load(:x=>1, :id=>1)
    @db.sqls.should == ["SELECT * FROM items WHERE (x = 1) LIMIT 1"]
  end
  
  it "should create the record if not found" do
    @db.fetch = [[], {:x=>1, :id=>1}]
    @db.autoid = 1
    @c.find_or_create(:x => 1).should == @c.load(:x=>1, :id=>1)
    @db.sqls.should == ["SELECT * FROM items WHERE (x = 1) LIMIT 1",
      "INSERT INTO items (x) VALUES (1)",
      "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
  end

  it "should pass the new record to be created to the block if no record is found" do
    @db.fetch = [[], {:x=>1, :id=>1}]
    @db.autoid = 1
    @c.find_or_create(:x => 1){|x| x[:y] = 2}.should == @c.load(:x=>1, :id=>1)
    sqls = @db.sqls
    sqls.first.should == "SELECT * FROM items WHERE (x = 1) LIMIT 1"
    ["INSERT INTO items (x, y) VALUES (1, 2)", "INSERT INTO items (y, x) VALUES (2, 1)"].should include(sqls[1])
    sqls.last.should == "SELECT * FROM items WHERE (id = 1) LIMIT 1"
  end
end

describe Sequel::Model, ".all" do
  it "should return all records in the dataset" do
    c = Class.new(Sequel::Model(:items))
    c.all.should == [c.load(:x=>1, :id=>1)]
  end
end

describe Sequel::Model, "A model class without a primary key" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :x
      no_primary_key
    end
    DB.reset
  end

  it "should be able to insert records without selecting them back" do
    i = nil
    proc {i = @c.create(:x => 1)}.should_not raise_error
    i.class.should be(@c)
    i.values.to_hash.should == {:x => 1}

    DB.sqls.should == ['INSERT INTO items (x) VALUES (1)']
  end

  it "should raise when deleting" do
    proc{@c.load(:x=>1).delete}.should raise_error
  end

  it "should raise when updating" do
    proc{@c.load(:x=>1).update(:x=>2)}.should raise_error
  end

  it "should insert a record when saving" do
    o = @c.new(:x => 2)
    o.should be_new
    o.save
    DB.sqls.should == ['INSERT INTO items (x) VALUES (2)']
  end
end

describe Sequel::Model, "attribute accessors" do
  before do
    db = Sequel.mock
    def db.supports_schema_parsing?() true end
    def db.schema(*)
      [[:x, {:type=>:integer}], [:z, {:type=>:integer}]]
    end
    @dataset = db[:items].columns(:x, :z)
    @c = Class.new(Sequel::Model)
    DB.reset
  end

  it "should be created on set_dataset" do
    %w'x z x= z='.each do |x|
      @c.instance_methods.collect{|z| z.to_s}.should_not include(x)
    end
    @c.set_dataset(@dataset)
    %w'x z x= z='.each do |x|
      @c.instance_methods.collect{|z| z.to_s}.should include(x)
    end
    o = @c.new
    %w'x z x= z='.each do |x|
      o.methods.collect{|z| z.to_s}.should include(x)
    end

    o.x.should be_nil
    o.x = 34
    o.x.should == 34
  end

  it "should be only accept one argument for the write accessor" do
    @c.set_dataset(@dataset)
    o = @c.new

    o.x = 34
    o.x.should == 34
    proc{o.send(:x=)}.should raise_error
    proc{o.send(:x=, 3, 4)}.should raise_error
  end

  it "should have a working typecasting setter even if the column is not selected" do
    @c.set_dataset(@dataset.select(:z).columns(:z))
    o = @c.new

    o.x = '34'
    o.x.should == 34
  end

  it "should typecast if the new value is the same as the existing but has a different class" do
    @c.set_dataset(@dataset.select(:z).columns(:z))
    o = @c.new

    o.x = 34
    o.x = 34.0
    o.x.should == 34.0
    o.x = 34
    o.x.should == 34
  end
end

describe Sequel::Model, ".[]" do
  before do
    @c = Class.new(Sequel::Model(:items))
    @c.dataset._fetch = {:name => 'sharon', :id => 1}
    DB.reset
  end

  it "should return the first record for the given pk" do
    @c[1].should == @c.load(:name => 'sharon', :id => 1)
    DB.sqls.should == ["SELECT * FROM items WHERE id = 1"]
    @c[9999].should == @c.load(:name => 'sharon', :id => 1)
    DB.sqls.should == ["SELECT * FROM items WHERE id = 9999"]
  end

  it "should have #[] return nil if no rows match" do
    @c.dataset._fetch = []
    @c[1].should == nil
    DB.sqls.should == ["SELECT * FROM items WHERE id = 1"]
  end

  it "should work correctly for custom primary key" do
    @c.set_primary_key :name
    @c['sharon'].should == @c.load(:name => 'sharon', :id => 1)
    DB.sqls.should == ["SELECT * FROM items WHERE name = 'sharon'"]
  end

  it "should return the first record for the given pk for a filtered dataset" do
    @c.dataset = @c.dataset.filter(:active=>true)
    @c[1].should == @c.load(:name => 'sharon', :id => 1)
    DB.sqls.should == ["SELECT * FROM items WHERE ((active IS TRUE) AND (id = 1)) LIMIT 1"]
  end

  it "should work correctly for composite primary key specified as array" do
    @c.set_primary_key [:node_id, :kind]
    @c[3921, 201].should be_a_kind_of(@c)
    sqls = DB.sqls
    sqls.length.should == 1
    sqls.first.should =~ /^SELECT \* FROM items WHERE \((\(node_id = 3921\) AND \(kind = 201\))|(\(kind = 201\) AND \(node_id = 3921\))\) LIMIT 1$/
  end
end

describe "Model#inspect" do
  specify "should include the class name and the values" do
    Sequel::Model.load(:x => 333).inspect.should == '#<Sequel::Model @values={:x=>333}>'
  end
end

describe "Model.db_schema" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      def self.columns; orig_columns; end
    end
    @db = Sequel.mock
    def @db.supports_schema_parsing?() true end
    @dataset = @db[:items]
  end
  
  specify "should not call database's schema if it isn't supported" do
    def @db.supports_schema_parsing?() false end
    def @db.schema(table, opts = {})
      raise Sequel::Error
    end
    @dataset.instance_variable_set(:@columns, [:x, :y])

    @c.dataset = @dataset
    @c.db_schema.should == {:x=>{}, :y=>{}}
    @c.columns.should == [:x, :y]
    @c.dataset.instance_variable_get(:@columns).should == [:x, :y]
  end

  specify "should use the database's schema and set the columns and dataset columns" do
    def @db.schema(table, opts = {})
      [[:x, {:type=>:integer}], [:y, {:type=>:string}]]
    end
    @c.dataset = @dataset
    @c.db_schema.should == {:x=>{:type=>:integer}, :y=>{:type=>:string}}
    @c.columns.should == [:x, :y]
    @c.dataset.instance_variable_get(:@columns).should == [:x, :y]
  end

  specify "should not restrict the schema for datasets with a :select option" do
    def @c.columns; [:x, :z]; end
    def @db.schema(table, opts = {})
      [[:x, {:type=>:integer}], [:y, {:type=>:string}]]
    end
    @c.dataset = @dataset.select(:x, :y___z)
    @c.db_schema.should == {:x=>{:type=>:integer}, :z=>{}, :y=>{:type=>:string}}
  end

  specify "should fallback to fetching records if schema raises an error" do
    def @db.schema(table, opts={})
      raise Sequel::Error
    end
    @c.dataset = @dataset.join(:x, :id).columns(:id, :x)
    @c.db_schema.should == {:x=>{}, :id=>{}}
  end
  
  specify "should automatically set a singular primary key based on the schema" do
    ds = @dataset
    d = ds.db
    def d.schema(table, *opts) [[:x, {:primary_key=>true}]] end
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>true}}
    @c.primary_key.should == :x
  end
  
  specify "should automatically set the composite primary key based on the schema" do
    ds = @dataset
    d = ds.db
    def d.schema(table, *opts) [[:x, {:primary_key=>true}], [:y, {:primary_key=>true}]] end
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>true}, :y=>{:primary_key=>true}}
    @c.primary_key.should == [:x, :y]
  end

  specify "should set an immutable composite primary key based on the schema" do
    ds = @dataset
    d = ds.db
    def d.schema(table, *opts) [[:x, {:primary_key=>true}], [:y, {:primary_key=>true}]] end
    @c.dataset = ds
    @c.primary_key.should == [:x, :y]
    proc{@c.primary_key.pop}.should raise_error
  end
  
  specify "should automatically set no primary key based on the schema" do
    ds = @dataset
    d = ds.db
    def d.schema(table, *opts) [[:x, {:primary_key=>false}], [:y, {:primary_key=>false}]] end
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>false}, :y=>{:primary_key=>false}}
    @c.primary_key.should == nil
  end
  
  specify "should automatically set primary key for dataset selecting table.*" do
    ds = @dataset.select_all(:items)
    d = ds.db
    def d.schema(table, *opts) [[:x, {:primary_key=>true}]] end
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>true}}
    @c.primary_key.should == :x
  end
  
  specify "should not modify the primary key unless all column schema hashes have a :primary_key entry" do
    ds = @dataset
    d = ds.db
    def d.schema(table, *opts) [[:x, {:primary_key=>false}], [:y, {}]] end
    @c.primary_key.should == :id
    @c.dataset = ds
    @c.db_schema.should == {:x=>{:primary_key=>false}, :y=>{}}
    @c.primary_key.should == :id
  end
end

describe "Model#use_transactions" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end

  specify "should return class value by default" do
    @c.use_transactions = true
    @c.new.use_transactions.should == true
    @c.use_transactions = false
    @c.new.use_transactions.should == false
  end

  specify "should return set value if manually set" do
    instance = @c.new
    instance.use_transactions = false
    instance.use_transactions.should == false
    @c.use_transactions = true
    instance.use_transactions.should == false
    
    instance.use_transactions = true
    instance.use_transactions.should == true
    @c.use_transactions = false
    instance.use_transactions.should == true
  end
end
