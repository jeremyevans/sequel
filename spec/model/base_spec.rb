require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Model attribute setters" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :id, :x, :y, :"x y"
    end
    @o = @c.new
    DB.reset
  end

  it "refresh should return self" do
    @o = @c[1]
    def @o._refresh(*) [] end
    @o.refresh.must_equal @o
  end

  it "should mark the column value as changed" do
    @o.changed_columns.must_equal []

    @o.x = 2
    @o.changed_columns.must_equal [:x]

    @o.y = 3
    @o.changed_columns.must_equal [:x, :y]

    @o.changed_columns.clear

    @o[:x] = 2
    @o.changed_columns.must_equal [:x]

    @o[:y] = 3
    @o.changed_columns.must_equal [:x, :y]
  end

  it "should handle columns that can't be called like normal ruby methods" do
    @o.send(:"x y=", 3)
    @o.changed_columns.must_equal [:"x y"]
    @o.values.must_equal(:"x y"=>3)
    @o.send(:"x y").must_equal 3
  end
end

describe "Model.def_column_alias" do
  before do
    @o = Class.new(Sequel::Model(:items)) do
      columns :id
      def_column_alias(:id2, :id)
    end.load(:id=>1)
    DB.reset
  end

  it "should create an getter alias for the column" do
    @o.id2.must_equal 1
  end

  it "should create an setter alias for the column" do
    @o.id2 = 2
    @o.id2.must_equal 2
    @o.values.must_equal(:id => 2)
  end
end

describe Sequel::Model, "dataset" do
  before do
    @a = Class.new(Sequel::Model(:items))
    @b = Class.new(Sequel::Model)
    class ::Elephant < Sequel::Model(:ele1); end
    class ::Maggot < Sequel::Model; end
    class ::ShoeSize < Sequel::Model; end
    class ::BootSize < ShoeSize; end
  end
  after do
    [:Elephant, :Maggot, :ShoeSize, :BootSize].each{|x| Object.send(:remove_const, x)}
  end
  
  it "should default to the plural of the class name" do
    Maggot.dataset.sql.must_equal 'SELECT * FROM maggots'
    ShoeSize.dataset.sql.must_equal 'SELECT * FROM shoe_sizes'
  end
  
  it "should return the dataset for the superclass if available" do
    BootSize.dataset.sql.must_equal 'SELECT * FROM shoe_sizes'
  end
  
  it "should return the correct dataset if set explicitly" do
    Elephant.dataset.sql.must_equal 'SELECT * FROM ele1'
    @a.dataset.sql.must_equal 'SELECT * FROM items'
  end
  
  it "should raise if no dataset is explicitly set and the class is anonymous" do
    proc {@b.dataset}.must_raise(Sequel::Error)
  end
  
  it "should not override dataset explicitly set when subclassing" do
    sc = Class.new(::Elephant) do
      set_dataset :foo
    end
    sc.table_name.must_equal :foo
  end
end
  
describe Sequel::Model, "implicit table names" do
  after do
    Object.send(:remove_const, :BlahBlah)
  end
  it "should disregard namespaces for the table name" do
    module ::BlahBlah
      class MwaHaHa < Sequel::Model
      end
    end
    BlahBlah::MwaHaHa.dataset.sql.must_equal 'SELECT * FROM mwa_ha_has'
  end

  it "should automatically set datasets when anonymous class of Sequel::Model is used as superclass" do
    class BlahBlah < Class.new(Sequel::Model); end
    BlahBlah.dataset.sql.must_equal 'SELECT * FROM blah_blahs'
  end
end

describe Sequel::Model, ".def_dataset_method" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end
  
  it "should add a method to the dataset and model if called with a block argument" do
    @c.def_dataset_method(:return_3){3}
    @c.return_3.must_equal 3
    @c.dataset.return_3.must_equal 3
  end

  it "should handle weird method names" do
    @c.def_dataset_method(:"return 3"){3}
    @c.send(:"return 3").must_equal 3
    @c.dataset.send(:"return 3").must_equal 3
  end

  it "should not add a model method if the model already responds to the method" do
    @c.instance_eval do
      def foo
        1
      end

      private

      def bar
        2
      end

      def_dataset_method(:foo){3}
      def_dataset_method(:bar){4}
    end
    @c.foo.must_equal 1
    @c.dataset.foo.must_equal 3
    @c.send(:bar).must_equal 2
    @c.dataset.bar.must_equal 4
  end

  it "should add all passed methods to the model if called without a block argument" do
    @c.def_dataset_method(:return_3, :return_4)
    proc{@c.return_3}.must_raise(NoMethodError)
    proc{@c.return_4}.must_raise(NoMethodError)
    @c.dataset.instance_eval do
      def return_3; 3; end
      def return_4; 4; end
    end
    @c.return_3.must_equal 3
    @c.return_4.must_equal 4
  end

  it "should cache calls and readd methods if set_dataset is used" do
    @c.def_dataset_method(:return_3){3}
    @c.set_dataset :items
    @c.return_3.must_equal 3
    @c.dataset.return_3.must_equal 3
  end

  it "should readd methods to subclasses, if set_dataset is used in a subclass" do
    @c.def_dataset_method(:return_3){3}
    c = Class.new(@c)
    c.set_dataset :items
    c.return_3.must_equal 3
    c.dataset.return_3.must_equal 3
  end
end

describe Sequel::Model, ".dataset_module" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end
  
  it "should extend the dataset with the module if the model has a dataset" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset.return_3.must_equal 3
  end

  it "should also extend the instance_dataset with the module if the model has a dataset" do
    @c.dataset_module{def return_3() 3 end}
    @c.instance_dataset.return_3.must_equal 3
  end

  it "should add methods defined in the module to the class" do
    @c.dataset_module{def return_3() 3 end}
    @c.return_3.must_equal 3
  end

  it "should add methods defined in the module outside the block to the class" do
    @c.dataset_module.module_eval{def return_3() 3 end}
    @c.return_3.must_equal 3
  end

  it "should cache calls and readd methods if set_dataset is used" do
    @c.dataset_module{def return_3() 3 end}
    @c.set_dataset :items
    @c.return_3.must_equal 3
    @c.dataset.return_3.must_equal 3
  end

  it "should readd methods to subclasses, if set_dataset is used in a subclass" do
    @c.dataset_module{def return_3() 3 end}
    c = Class.new(@c)
    c.set_dataset :items
    c.return_3.must_equal 3
    c.dataset.return_3.must_equal 3
  end

  it "should only have a single dataset_module per class" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset_module{def return_3() 3 + (begin; super; rescue NoMethodError; 1; end) end}
    @c.return_3.must_equal 4
  end

  it "should not have subclasses share the dataset_module" do
    @c.dataset_module{def return_3() 3 end}
    c = Class.new(@c)
    c.dataset_module{def return_3() 3 + (begin; super; rescue NoMethodError; 1; end) end}
    c.return_3.must_equal 6
  end

  it "should accept a module object and extend the dataset with it" do
    @c.dataset_module Module.new{def return_3() 3 end}
    @c.dataset.return_3.must_equal 3
  end

  it "should be able to call dataset_module with a module multiple times" do
    @c.dataset_module Module.new{def return_3() 3 end}
    @c.dataset_module Module.new{def return_4() 4 end}
    @c.dataset.return_3.must_equal 3
    @c.dataset.return_4.must_equal 4
  end

  it "should be able mix dataset_module calls with and without arguments" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset_module Module.new{def return_4() 4 end}
    @c.dataset.return_3.must_equal 3
    @c.dataset.return_4.must_equal 4
  end

  it "should have modules provided to dataset_module extend subclass datasets" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset_module Module.new{def return_4() 4 end}
    c = Class.new(@c)
    c.set_dataset :a
    c.dataset.return_3.must_equal 3
    c.dataset.return_4.must_equal 4
  end

  it "should return the dataset module if given a block" do
    Object.new.extend(@c.dataset_module{def return_3() 3 end}).return_3.must_equal 3
  end

  it "should return the argument if given one" do
    Object.new.extend(@c.dataset_module Module.new{def return_3() 3 end}).return_3.must_equal 3
  end

  it "should have dataset_module support a subset method" do
    @c.dataset_module{subset :released, :released}
    @c.released.sql.must_equal 'SELECT * FROM items WHERE released'
    @c.where(:foo).released.sql.must_equal 'SELECT * FROM items WHERE (foo AND released)'
  end

  it "should raise error if called with both an argument and ablock" do
    proc{@c.dataset_module(Module.new{def return_3() 3 end}){}}.must_raise(Sequel::Error)
  end
end

describe "A model class with implicit table name" do
  before do
    class ::Donkey < Sequel::Model
    end
  end
  after do
    Object.send(:remove_const, :Donkey)
  end
  
  it "should have a dataset associated with the model class" do
    Donkey.dataset.model.must_equal Donkey
  end
end

describe "A model inheriting from a model" do
  before do
    class ::Feline < Sequel::Model; end
    class ::Leopard < Feline; end
  end
  after do
    Object.send(:remove_const, :Leopard)
    Object.send(:remove_const, :Feline)
  end
  
  it "should have a dataset associated with itself" do
    Feline.dataset.model.must_equal Feline
    Leopard.dataset.model.must_equal Leopard
  end
end

describe "Model.primary_key" do
  before do
    @c = Class.new(Sequel::Model)
  end
  
  it "should default to id" do
    @c.primary_key.must_equal :id
  end

  it "should be overridden by set_primary_key" do
    @c.set_primary_key :cid
    @c.primary_key.must_equal :cid

    @c.set_primary_key([:id1, :id2])
    @c.primary_key.must_equal [:id1, :id2]
  end
  
  it "should use nil for no primary key" do
    @c.no_primary_key
    @c.primary_key.must_equal nil
  end
end

describe "Model.primary_key_hash" do
  before do
    @c = Class.new(Sequel::Model)
  end
  
  it "should handle a single primary key" do
    @c.primary_key_hash(1).must_equal(:id=>1)
  end

  it "should handle a composite primary key" do
    @c.set_primary_key([:id1, :id2])
    @c.primary_key_hash([1, 2]).must_equal(:id1=>1, :id2=>2)
  end

  it "should raise an error for no primary key" do
    @c.no_primary_key
    proc{@c.primary_key_hash(1)}.must_raise(Sequel::Error)
  end
end

describe "Model.qualified_primary_key_hash" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end
  
  it "should handle a single primary key" do
    @c.qualified_primary_key_hash(1).must_equal(Sequel.qualify(:items, :id)=>1)
  end

  it "should handle a composite primary key" do
    @c.set_primary_key([:id1, :id2])
    @c.qualified_primary_key_hash([1, 2]).must_equal(Sequel.qualify(:items, :id1)=>1, Sequel.qualify(:items, :id2)=>2)
  end

  it "should raise an error for no primary key" do
    @c.no_primary_key
    proc{@c.qualified_primary_key_hash(1)}.must_raise(Sequel::Error)
  end

  it "should allow specifying a different qualifier" do
    @c.qualified_primary_key_hash(1, :apple).must_equal(Sequel.qualify(:apple, :id)=>1)
    @c.set_primary_key([:id1, :id2])
    @c.qualified_primary_key_hash([1, 2], :bear).must_equal(Sequel.qualify(:bear, :id1)=>1, Sequel.qualify(:bear, :id2)=>2)
  end
end

describe "Model.db" do
  before do
    @db = Sequel.mock
    @databases = Sequel::DATABASES.dup
    @model_db = Sequel::Model.db
    Sequel::Model.db = nil
    Sequel::DATABASES.clear
  end
  after do
    Sequel::Model.instance_variable_get(:@db).must_equal nil
    Sequel::DATABASES.replace(@databases)
    Sequel::Model.db = @model_db
  end

  it "should be required when creating named model classes" do
    begin
      proc{class ModelTest < Sequel::Model; end}.must_raise(Sequel::Error)
    ensure
      Object.send(:remove_const, :ModelTest)
    end
  end

  it "should be required when creating anonymous model classes without a database" do
    proc{Sequel::Model(:foo)}.must_raise(Sequel::Error)
  end

  it "should not be required when creating anonymous model classes with a database" do
    Sequel::Model(@db).db.must_equal @db
    Sequel::Model(@db[:foo]).db.must_equal @db
  end

  it "should work correctly when subclassing anonymous model classes with a database" do
    begin
      Class.new(Sequel::Model(@db)).db.must_equal @db
      Class.new(Sequel::Model(@db[:foo])).db.must_equal @db
      class ModelTest < Sequel::Model(@db)
        db.must_equal @db
      end
      class ModelTest2 < Sequel::Model(@db[:foo])
        db.must_equal @db
      end
      ModelTest.instance_variable_set(:@db, nil)
      ModelTest.db.must_equal @db
    ensure
      Object.send(:remove_const, :ModelTest)
      Object.send(:remove_const, :ModelTest2)
    end
  end
end

describe "Model.db=" do
  before do
    @db1 = Sequel.mock
    @db2 = Sequel.mock
    
    @m = Class.new(Sequel::Model(@db1[:blue].filter(:x=>1)))
  end
  
  it "should affect the underlying dataset" do
    @m.db = @db2
    
    @m.dataset.db.must_equal @db2
    @m.dataset.db.wont_equal @db1
  end

  it "should keep the same dataset options" do
    @m.db = @db2
    @m.dataset.sql.must_equal 'SELECT * FROM blue WHERE (x = 1)'
  end

  it "should use the database for subclasses" do
    @m.db = @db2
    Class.new(@m).db.must_equal @db2
  end
end

describe Sequel::Model, ".(allowed|restricted)_columns " do
  before do
    @c = Class.new(Sequel::Model(:blahblah)) do
      columns :x, :y, :z
    end
    @c.strict_param_setting = false
    @c.instance_variable_set(:@columns, [:x, :y, :z])
    DB.reset
  end
  
  it "should set the allowed columns correctly" do
    @c.allowed_columns.must_equal nil
    @c.set_allowed_columns :x
    @c.allowed_columns.must_equal [:x]
    @c.set_allowed_columns :x, :y
    @c.allowed_columns.must_equal [:x, :y]
  end

  it "should only set allowed columns by default" do
    @c.set_allowed_columns :x, :y
    i = @c.new(:x => 1, :y => 2, :z => 3)
    i.values.must_equal(:x => 1, :y => 2)
    i.set(:x => 4, :y => 5, :z => 6)
    i.values.must_equal(:x => 4, :y => 5)

    @c.instance_dataset._fetch = @c.dataset._fetch = {:x => 7}
    i = @c.new
    i.update(:x => 7, :z => 9)
    i.values.must_equal(:x => 7)
    DB.sqls.must_equal ["INSERT INTO blahblah (x) VALUES (7)", "SELECT * FROM blahblah WHERE (id = 10) LIMIT 1"]
  end
end

describe Sequel::Model, ".(un)?restrict_primary_key\\??" do
  before do
    @c = Class.new(Sequel::Model(:blahblah)) do
      set_primary_key :id
      columns :x, :y, :z, :id
    end
    @c.strict_param_setting = false
  end
  
  it "should restrict updates to primary key by default" do
    i = @c.new(:x => 1, :y => 2, :id => 3)
    i.values.must_equal(:x => 1, :y => 2)
    i.set(:x => 4, :y => 5, :id => 6)
    i.values.must_equal(:x => 4, :y => 5)
  end

  it "should allow updates to primary key if unrestrict_primary_key is used" do
    @c.unrestrict_primary_key
    i = @c.new(:x => 1, :y => 2, :id => 3)
    i.values.must_equal(:x => 1, :y => 2, :id=>3)
    i.set(:x => 4, :y => 5, :id => 6)
    i.values.must_equal(:x => 4, :y => 5, :id=>6)
  end

  it "should have restrict_primary_key? return true or false depending" do
    @c.restrict_primary_key?.must_equal true
    @c.unrestrict_primary_key
    @c.restrict_primary_key?.must_equal false
    c1 = Class.new(@c)
    c1.restrict_primary_key?.must_equal false
    @c.restrict_primary_key
    @c.restrict_primary_key?.must_equal true
    c1.restrict_primary_key?.must_equal false
    c2 = Class.new(@c)
    c2.restrict_primary_key?.must_equal true
  end
end

describe Sequel::Model, ".strict_param_setting" do
  before do
    @c = Class.new(Sequel::Model(:blahblah)) do
      columns :x, :y, :z, :id
      set_allowed_columns :x, :y
    end
  end
  
  it "should be enabled by default" do
    @c.strict_param_setting.must_equal true
  end

  it "should raise an error if a missing/restricted column/method is accessed" do
    proc{@c.new(:z=>1)}.must_raise(Sequel::MassAssignmentRestriction)
    proc{@c.create(:z=>1)}.must_raise(Sequel::MassAssignmentRestriction)
    c = @c.new
    proc{c.set(:z=>1)}.must_raise(Sequel::MassAssignmentRestriction)
    proc{c.set_all(:use_after_commit_rollback => false)}.must_raise(Sequel::MassAssignmentRestriction)
    proc{c.set_only({:x=>1}, :y)}.must_raise(Sequel::MassAssignmentRestriction)
    proc{c.update(:z=>1)}.must_raise(Sequel::MassAssignmentRestriction)
    proc{c.update_all(:use_after_commit_rollback=>false)}.must_raise(Sequel::MassAssignmentRestriction)
    proc{c.update_only({:x=>1}, :y)}.must_raise(Sequel::MassAssignmentRestriction)
  end

  it "should be disabled by strict_param_setting = false" do
    @c.strict_param_setting = false
    @c.strict_param_setting.must_equal false
    @c.new(:z=>1)
  end
end

describe Sequel::Model, ".require_modification" do
  before do
    @ds1 = DB[:items]
    def @ds1.provides_accurate_rows_matched?() false end
    @ds2 = DB[:items]
    def @ds2.provides_accurate_rows_matched?() true end
  end
  after do
    Sequel::Model.require_modification = nil
  end

  it "should depend on whether the dataset provides an accurate number of rows matched by default" do
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.must_equal false
    Class.new(Sequel::Model).set_dataset(@ds2).require_modification.must_equal true
  end

  it "should obey global setting regardless of dataset support if set" do
    Sequel::Model.require_modification = true
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.must_equal true
    Class.new(Sequel::Model).set_dataset(@ds2).require_modification.must_equal true
    
    Sequel::Model.require_modification = false
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.must_equal false
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.must_equal false
  end
end

describe Sequel::Model, ".[] optimization" do
  before do
    @db = Sequel.mock
    @db.quote_identifiers = true
    def @db.schema(*) [[:id, {:primary_key=>true}]] end
    def @db.supports_schema_parsing?() true end
    @c = Class.new(Sequel::Model(@db))
  end

  it "should set simple_pk to the literalized primary key column name if a single primary key" do
    @c.set_primary_key :id
    @c.simple_pk.must_equal '"id"'
    @c.set_primary_key :b
    @c.simple_pk.must_equal '"b"'
    @c.set_primary_key Sequel.identifier(:b__a)
    @c.simple_pk.must_equal '"b__a"'
  end

  it "should have simple_pk be blank if compound or no primary key" do
    @c.no_primary_key
    @c.simple_pk.must_equal nil
    @c.set_primary_key [:b, :a]
    @c.simple_pk.must_equal nil
  end

  it "should have simple table set if passed a Symbol to set_dataset" do
    @c.set_dataset :a
    @c.simple_table.must_equal '"a"'
    @c.set_dataset :b
    @c.simple_table.must_equal '"b"'
    @c.set_dataset :b__a
    @c.simple_table.must_equal '"b"."a"'
  end

  it "should have simple_table set if passed a simple select all dataset to set_dataset" do
    @c.set_dataset @db[:a]
    @c.simple_table.must_equal '"a"'
    @c.set_dataset @db[:b]
    @c.simple_table.must_equal '"b"'
    @c.set_dataset @db[:b__a]
    @c.simple_table.must_equal '"b"."a"'
  end

  it "should have simple_pk and simple_table respect dataset's identifier input methods" do
    ds = @db[:ab]
    ds.identifier_input_method = :reverse
    @c.set_dataset ds
    @c.simple_table.must_equal '"ba"'
    @c.set_primary_key :cd
    @c.simple_pk.must_equal '"dc"'

    @c.set_dataset ds.from(:ef__gh)
    @c.simple_table.must_equal '"fe"."hg"'
  end

  it "should have simple_table = nil if passed a non-simple select all dataset to set_dataset" do
    @c.set_dataset @c.db[:a].filter(:active)
    @c.simple_table.must_equal nil
  end

  it "should have simple_table inherit superclass's setting" do
    Class.new(@c).simple_table.must_equal nil
    @c.set_dataset :a
    Class.new(@c).simple_table.must_equal '"a"'
  end

  it "should use Dataset#with_sql if simple_table and simple_pk are true" do
    @c.set_dataset :a
    @c.instance_dataset._fetch = @c.dataset._fetch = {:id => 1}
    @c[1].must_equal @c.load(:id=>1)
    @db.sqls.must_equal ['SELECT * FROM "a" WHERE "id" = 1']
  end

  it "should not use Dataset#with_sql if either simple_table or simple_pk is nil" do
    @c.set_dataset @db[:a].filter(:active)
    @c.dataset._fetch = {:id => 1}
    @c[1].must_equal @c.load(:id=>1)
    @db.sqls.must_equal ['SELECT * FROM "a" WHERE ("active" AND ("id" = 1)) LIMIT 1']
  end
end

describe "Model datasets #with_pk with #with_pk!" do
  before do
    @c = Class.new(Sequel::Model(:a))
    @ds = @c.dataset
    @ds._fetch = {:id=>1}
    DB.reset
  end

  it "should be callable on the model class with optimized SQL" do
    @c.with_pk(1).must_equal @c.load(:id=>1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
    @c.with_pk!(1).must_equal @c.load(:id=>1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
  end

  it "should return the first record where the primary key matches" do
    @ds.with_pk(1).must_equal @c.load(:id=>1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
    @ds.with_pk!(1).must_equal @c.load(:id=>1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
  end

  it "should handle existing filters" do
    @ds.filter(:a=>2).with_pk(1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE ((a = 2) AND (a.id = 1)) LIMIT 1"]
    @ds.filter(:a=>2).with_pk!(1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE ((a = 2) AND (a.id = 1)) LIMIT 1"]
  end

  it "should work with string values" do
    @ds.with_pk("foo")
    DB.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 'foo') LIMIT 1"]
    @ds.with_pk!("foo")
    DB.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 'foo') LIMIT 1"]
  end

  it "should handle an array for composite primary keys" do
    @c.set_primary_key [:id1, :id2]
    @ds.with_pk([1, 2])
    sqls = DB.sqls
    ["SELECT * FROM a WHERE ((a.id1 = 1) AND (a.id2 = 2)) LIMIT 1",
    "SELECT * FROM a WHERE ((a.id2 = 2) AND (a.id1 = 1)) LIMIT 1"].must_include(sqls.pop)
    sqls.must_equal []

    @ds.with_pk!([1, 2])
    sqls = DB.sqls
    ["SELECT * FROM a WHERE ((a.id1 = 1) AND (a.id2 = 2)) LIMIT 1",
    "SELECT * FROM a WHERE ((a.id2 = 2) AND (a.id1 = 1)) LIMIT 1"].must_include(sqls.pop)
    sqls.must_equal []
  end

  it "should have with_pk return nil and with_pk! raise if no rows match" do
    @ds._fetch = []
    @ds.with_pk(1).must_equal nil
    DB.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
    proc{@ds.with_pk!(1)}.must_raise(Sequel::NoMatchingRow)
    DB.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
  end

  it "should have with_pk return nil and with_pk! raise if no rows match when calling the class method" do
    @ds._fetch = []
    @c.with_pk(1).must_equal nil
    DB.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
    proc{@c.with_pk!(1)}.must_raise(Sequel::NoMatchingRow)
    DB.sqls.must_equal ["SELECT * FROM a WHERE id = 1"]
  end

  it "should have #[] consider an integer as a primary key lookup" do
    @ds[1].must_equal @c.load(:id=>1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
  end

  it "should not have #[] consider a string as a primary key lookup" do
    @ds['foo'].must_equal @c.load(:id=>1)
    DB.sqls.must_equal ["SELECT * FROM a WHERE (foo) LIMIT 1"]
  end
end

describe "Model::include" do
  it "shouldn't change the signature of Module::include" do
    mod1 = Module.new
    mod2 = Module.new
    including_class = Class.new(Sequel::Model(:items)) do
      include(mod1, mod2)
    end
    including_class.included_modules.must_include(mod1, mod2)
  end
end
