require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Model attribute setters" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :id, :x, :y, :"x y"
    end
    @o = @c.new
    MODEL_DB.reset
  end

  specify "refresh should return self" do
    @o = @c[1]
    @o.stub(:_refresh).and_return([])
    @o.refresh.should == @o
  end

  it "should mark the column value as changed" do
    @o.changed_columns.should == []

    @o.x = 2
    @o.changed_columns.should == [:x]

    @o.y = 3
    @o.changed_columns.should == [:x, :y]

    @o.changed_columns.clear

    @o[:x] = 2
    @o.changed_columns.should == [:x]

    @o[:y] = 3
    @o.changed_columns.should == [:x, :y]
  end

  it "should have columns that can't be called like normal ruby methods" do
    @o.send(:"x y=", 3)
    @o.changed_columns.should == [:"x y"]
    @o.values.should == {:"x y"=>3}
    @o.send(:"x y").should == 3
  end
end

describe "Model.def_column_alias" do
  before do
    @o = Class.new(Sequel::Model(:items)) do
      columns :id
      def_column_alias(:id2, :id)
    end.load(:id=>1)
    MODEL_DB.reset
  end

  it "should create an getter alias for the column" do
    @o.id2.should == 1
  end

  it "should create an setter alias for the column" do
    @o.id2 = 2
    @o.id2.should == 2
    @o.values.should == {:id => 2}
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

  specify "should default to the plural of the class name" do
    Maggot.dataset.sql.should == 'SELECT * FROM maggots'
    ShoeSize.dataset.sql.should == 'SELECT * FROM shoe_sizes'
  end

  specify "should return the dataset for the superclass if available" do
    BootSize.dataset.sql.should == 'SELECT * FROM shoe_sizes'
  end

  specify "should return the correct dataset if set explicitly" do
    Elephant.dataset.sql.should == 'SELECT * FROM ele1'
    @a.dataset.sql.should == 'SELECT * FROM items'
  end

  specify "should raise if no dataset is explicitly set and the class is anonymous" do
    proc {@b.dataset}.should raise_error(Sequel::Error)
  end

  specify "should disregard namespaces for the table name" do
    begin
      module ::BlahBlah
        class MwaHaHa < Sequel::Model
        end
      end

      BlahBlah::MwaHaHa.dataset.sql.should == 'SELECT * FROM mwa_ha_has'
    ensure
      Object.send(:remove_const, :BlahBlah)
    end
  end
end

describe Sequel::Model, ".def_dataset_method" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end

  it "should add a method to the dataset and model if called with a block argument" do
    @c.def_dataset_method(:return_3){3}
    @c.return_3.should == 3
    @c.dataset.return_3.should == 3
  end

  it "should handle weird method names" do
    @c.def_dataset_method(:"return 3"){3}
    @c.send(:"return 3").should == 3
    @c.dataset.send(:"return 3").should == 3
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
    @c.foo.should == 1
    @c.dataset.foo.should == 3
    @c.send(:bar).should == 2
    @c.dataset.bar.should == 4
  end

  it "should add all passed methods to the model if called without a block argument" do
    @c.def_dataset_method(:return_3, :return_4)
    proc{@c.return_3}.should raise_error(NoMethodError)
    proc{@c.return_4}.should raise_error(NoMethodError)
    @c.dataset.instance_eval do
      def return_3; 3; end
      def return_4; 4; end
    end
    @c.return_3.should == 3
    @c.return_4.should == 4
  end

  it "should cache calls and readd methods if set_dataset is used" do
    @c.def_dataset_method(:return_3){3}
    @c.set_dataset :items
    @c.return_3.should == 3
    @c.dataset.return_3.should == 3
  end

  it "should readd methods to subclasses, if set_dataset is used in a subclass" do
    @c.def_dataset_method(:return_3){3}
    c = Class.new(@c)
    c.set_dataset :items
    c.return_3.should == 3
    c.dataset.return_3.should == 3
  end
end

describe Sequel::Model, ".dataset_module" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end

  it "should extend the dataset with the module if the model has a dataset" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset.return_3.should == 3
  end

  it "should add methods defined in the module to the class" do
    @c.dataset_module{def return_3() 3 end}
    @c.return_3.should == 3
  end

  it "should cache calls and readd methods if set_dataset is used" do
    @c.dataset_module{def return_3() 3 end}
    @c.set_dataset :items
    @c.return_3.should == 3
    @c.dataset.return_3.should == 3
  end

  it "should readd methods to subclasses, if set_dataset is used in a subclass" do
    @c.dataset_module{def return_3() 3 end}
    c = Class.new(@c)
    c.set_dataset :items
    c.return_3.should == 3
    c.dataset.return_3.should == 3
  end

  it "should only have a single dataset_module per class" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset_module{def return_3() 3 + (begin; super; rescue NoMethodError; 1; end) end}
    @c.return_3.should == 4
  end

  it "should not have subclasses share the dataset_module" do
    @c.dataset_module{def return_3() 3 end}
    c = Class.new(@c)
    c.dataset_module{def return_3() 3 + (begin; super; rescue NoMethodError; 1; end) end}
    c.return_3.should == 6
  end

  it "should accept a module object and extend the dataset with it" do
    @c.dataset_module Module.new{def return_3() 3 end}
    @c.dataset.return_3.should == 3
  end

  it "should be able to call dataset_module with a module multiple times" do
    @c.dataset_module Module.new{def return_3() 3 end}
    @c.dataset_module Module.new{def return_4() 4 end}
    @c.dataset.return_3.should == 3
    @c.dataset.return_4.should == 4
  end

  it "should be able mix dataset_module calls with and without arguments" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset_module Module.new{def return_4() 4 end}
    @c.dataset.return_3.should == 3
    @c.dataset.return_4.should == 4
  end

  it "should have modules provided to dataset_module extend subclass datasets" do
    @c.dataset_module{def return_3() 3 end}
    @c.dataset_module Module.new{def return_4() 4 end}
    c = Class.new(@c)
    c.set_dataset :a
    c.dataset.return_3.should == 3
    c.dataset.return_4.should == 4
  end

  it "should return the dataset module if given a block" do
    Object.new.extend(@c.dataset_module{def return_3() 3 end}).return_3.should == 3
  end

  it "should return the argument if given one" do
    Object.new.extend(@c.dataset_module Module.new{def return_3() 3 end}).return_3.should == 3
  end

  it "should raise error if called with both an argument and ablock" do
    proc{@c.dataset_module(Module.new{def return_3() 3 end}){}}.should raise_error(Sequel::Error)
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

  specify "should have a dataset associated with the model class" do
    Donkey.dataset.model.should == Donkey
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

  specify "should have a dataset associated with itself" do
    Feline.dataset.model.should == Feline
    Leopard.dataset.model.should == Leopard
  end
end

describe "Model.primary_key" do
  before do
    @c = Class.new(Sequel::Model)
  end

  specify "should default to id" do
    @c.primary_key.should == :id
  end

  specify "should be overridden by set_primary_key" do
    @c.set_primary_key :cid
    @c.primary_key.should == :cid

    @c.set_primary_key([:id1, :id2])
    @c.primary_key.should == [:id1, :id2]
  end

  specify "should use nil for no primary key" do
    @c.no_primary_key
    @c.primary_key.should == nil
  end
end

describe "Model.primary_key_hash" do
  before do
    @c = Class.new(Sequel::Model)
  end

  specify "should handle a single primary key" do
    @c.primary_key_hash(1).should == {:id=>1}
  end

  specify "should handle a composite primary key" do
    @c.set_primary_key([:id1, :id2])
    @c.primary_key_hash([1, 2]).should == {:id1=>1, :id2=>2}
  end

  specify "should raise an error for no primary key" do
    @c.no_primary_key
    proc{@c.primary_key_hash(1)}.should raise_error(Sequel::Error)
  end
end

describe "Model.qualified_primary_key_hash" do
  before do
    @c = Class.new(Sequel::Model(:items))
  end

  specify "should handle a single primary key" do
    @c.qualified_primary_key_hash(1).should == {:id.qualify(:items)=>1}
  end

  specify "should handle a composite primary key" do
    @c.set_primary_key([:id1, :id2])
    @c.qualified_primary_key_hash([1, 2]).should == {:id1.qualify(:items)=>1, :id2.qualify(:items)=>2}
  end

  specify "should raise an error for no primary key" do
    @c.no_primary_key
    proc{@c.qualified_primary_key_hash(1)}.should raise_error(Sequel::Error)
  end

  specify "should allow specifying a different qualifier" do
    @c.qualified_primary_key_hash(1, :apple).should == {:id.qualify(:apple)=>1}
    @c.set_primary_key([:id1, :id2])
    @c.qualified_primary_key_hash([1, 2], :bear).should == {:id1.qualify(:bear)=>1, :id2.qualify(:bear)=>2}
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
    Sequel::Model.instance_variable_get(:@db).should == nil
    Sequel::DATABASES.replace(@databases)
    Sequel::Model.db = @model_db
  end

  specify "should be required when create named model classes" do
    begin
      proc{class ModelTest < Sequel::Model; end}.should raise_error(Sequel::Error)
    ensure
      Object.send(:remove_const, :ModelTest)
    end
  end

  specify "should be required when creating anonymous model classes without a database" do
    proc{Sequel::Model(:foo)}.should raise_error(Sequel::Error)
  end

  specify "should not be required when creating anonymous model classes with a database" do
    Sequel::Model(@db).db.should == @db
    Sequel::Model(@db[:foo]).db.should == @db
  end

  specify "should work correctly when subclassing anonymous model classes with a database" do
    begin
      Class.new(Sequel::Model(@db)).db.should == @db
      Class.new(Sequel::Model(@db[:foo])).db.should == @db
      class ModelTest < Sequel::Model(@db)
        db.should == @db
      end
      class ModelTest2 < Sequel::Model(@db[:foo])
        db.should == @db
      end
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

  specify "should affect the underlying dataset" do
    @m.db = @db2

    @m.dataset.db.should === @db2
    @m.dataset.db.should_not === @db1
  end

  specify "should keep the same dataset options" do
    @m.db = @db2
    @m.dataset.sql.should == 'SELECT * FROM blue WHERE (x = 1)'
  end

  specify "should use the database for subclasses" do
    @m.db = @db2
    Class.new(@m).db.should === @db2
  end
end

describe Sequel::Model, ".(allowed|restricted)_columns " do
  before do
    @c = Class.new(Sequel::Model(:blahblah)) do
      columns :x, :y, :z
    end
    @c.strict_param_setting = false
    @c.instance_variable_set(:@columns, [:x, :y, :z])
  end

  it "should set the allowed columns correctly" do
    @c.allowed_columns.should == nil
    @c.set_allowed_columns :x
    @c.allowed_columns.should == [:x]
    @c.set_allowed_columns :x, :y
    @c.allowed_columns.should == [:x, :y]
  end

  it "should set the restricted columns correctly" do
    @c.restricted_columns.should == nil
    @c.set_restricted_columns :x
    @c.restricted_columns.should == [:x]
    @c.set_restricted_columns :x, :y
    @c.restricted_columns.should == [:x, :y]
  end

  it "should only set allowed columns by default" do
    @c.set_allowed_columns :x, :y
    i = @c.new(:x => 1, :y => 2, :z => 3)
    i.values.should == {:x => 1, :y => 2}
    i.set(:x => 4, :y => 5, :z => 6)
    i.values.should == {:x => 4, :y => 5}

    @c.instance_dataset._fetch = @c.dataset._fetch = {:x => 7}
    i = @c.new
    i.update(:x => 7, :z => 9)
    i.values.should == {:x => 7}
    MODEL_DB.sqls.should == ["INSERT INTO blahblah (x) VALUES (7)", "SELECT * FROM blahblah WHERE (id = 10) LIMIT 1"]
  end

  it "should not set restricted columns by default" do
    @c.set_restricted_columns :z
    i = @c.new(:x => 1, :y => 2, :z => 3)
    i.values.should == {:x => 1, :y => 2}
    i.set(:x => 4, :y => 5, :z => 6)
    i.values.should == {:x => 4, :y => 5}

    @c.instance_dataset._fetch = @c.dataset._fetch = {:x => 7}
    i = @c.new
    i.update(:x => 7, :z => 9)
    i.values.should == {:x => 7}
    MODEL_DB.sqls.should == ["INSERT INTO blahblah (x) VALUES (7)", "SELECT * FROM blahblah WHERE (id = 10) LIMIT 1"]
  end

  it "should have allowed take precedence over restricted" do
    @c.set_allowed_columns :x, :y
    @c.set_restricted_columns :y, :z
    i = @c.new(:x => 1, :y => 2, :z => 3)
    i.values.should == {:x => 1, :y => 2}
    i.set(:x => 4, :y => 5, :z => 6)
    i.values.should == {:x => 4, :y => 5}

    @c.instance_dataset._fetch = @c.dataset._fetch = {:y => 7}
    i = @c.new
    i.update(:y => 7, :z => 9)
    i.values.should == {:y => 7}
    MODEL_DB.sqls.should == ["INSERT INTO blahblah (y) VALUES (7)", "SELECT * FROM blahblah WHERE (id = 10) LIMIT 1"]
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
    i.values.should == {:x => 1, :y => 2}
    i.set(:x => 4, :y => 5, :id => 6)
    i.values.should == {:x => 4, :y => 5}
  end

  it "should allow updates to primary key if unrestrict_primary_key is used" do
    @c.unrestrict_primary_key
    i = @c.new(:x => 1, :y => 2, :id => 3)
    i.values.should == {:x => 1, :y => 2, :id=>3}
    i.set(:x => 4, :y => 5, :id => 6)
    i.values.should == {:x => 4, :y => 5, :id=>6}
  end

  it "should have restrict_primary_key? return true or false depending" do
    @c.restrict_primary_key?.should == true
    @c.unrestrict_primary_key
    @c.restrict_primary_key?.should == false
    c1 = Class.new(@c)
    c1.restrict_primary_key?.should == false
    @c.restrict_primary_key
    @c.restrict_primary_key?.should == true
    c1.restrict_primary_key?.should == false
    c2 = Class.new(@c)
    c2.restrict_primary_key?.should == true
  end
end

describe Sequel::Model, ".strict_param_setting" do
  before do
    @c = Class.new(Sequel::Model(:blahblah)) do
      columns :x, :y, :z, :id
      set_restricted_columns :z
    end
  end

  it "should be enabled by default" do
    @c.strict_param_setting.should == true
  end

  it "should raise an error if a missing/restricted column/method is accessed" do
    proc{@c.new(:z=>1)}.should raise_error(Sequel::Error)
    proc{@c.create(:z=>1)}.should raise_error(Sequel::Error)
    c = @c.new
    proc{c.set(:z=>1)}.should raise_error(Sequel::Error)
    proc{c.set_all(:id=>1)}.should raise_error(Sequel::Error)
    proc{c.set_only({:x=>1}, :y)}.should raise_error(Sequel::Error)
    proc{c.set_except({:x=>1}, :x)}.should raise_error(Sequel::Error)
    proc{c.update(:z=>1)}.should raise_error(Sequel::Error)
    proc{c.update_all(:id=>1)}.should raise_error(Sequel::Error)
    proc{c.update_only({:x=>1}, :y)}.should raise_error(Sequel::Error)
    proc{c.update_except({:x=>1}, :x)}.should raise_error(Sequel::Error)
  end

  it "should be disabled by strict_param_setting = false" do
    @c.strict_param_setting = false
    @c.strict_param_setting.should == false
    proc{@c.new(:z=>1)}.should_not raise_error
  end
end

describe Sequel::Model, ".require_modification" do
  before do
    @ds1 = MODEL_DB[:items]
    @ds1.meta_def(:provides_accurate_rows_matched?){false}
    @ds2 = MODEL_DB[:items]
    @ds2.meta_def(:provides_accurate_rows_matched?){true}
  end
  after do
    Sequel::Model.require_modification = nil
  end

  it "should depend on whether the dataset provides an accurate number of rows matched by default" do
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.should == false
    Class.new(Sequel::Model).set_dataset(@ds2).require_modification.should == true
  end

  it "should obey global setting regardless of dataset support if set" do
    Sequel::Model.require_modification = true
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.should == true
    Class.new(Sequel::Model).set_dataset(@ds2).require_modification.should == true

    Sequel::Model.require_modification = false
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.should == false
    Class.new(Sequel::Model).set_dataset(@ds1).require_modification.should == false
  end
end

describe Sequel::Model, ".[] optimization" do
  before do
    @db = MODEL_DB.clone
    @db.quote_identifiers = true
    @c = Class.new(Sequel::Model(@db))
  end

  it "should set simple_pk to the literalized primary key column name if a single primary key" do
    @c.set_primary_key :id
    @c.simple_pk.should == '"id"'
    @c.set_primary_key :b
    @c.simple_pk.should == '"b"'
    @c.set_primary_key :b__a.identifier
    @c.simple_pk.should == '"b__a"'
  end

  it "should have simple_pk be blank if compound or no primary key" do
    @c.no_primary_key
    @c.simple_pk.should == nil
    @c.set_primary_key :b, :a
    @c.simple_pk.should == nil
    @c.set_primary_key [:b, :a]
    @c.simple_pk.should == nil
  end

  it "should have simple table set if passed a Symbol to set_dataset" do
    @c.set_dataset :a
    @c.simple_table.should == '"a"'
    @c.set_dataset :b
    @c.simple_table.should == '"b"'
    @c.set_dataset :b__a
    @c.simple_table.should == '"b"."a"'
  end

  it "should have simple_table set if passed a simple select all dataset to set_dataset" do
    @c.set_dataset @db[:a]
    @c.simple_table.should == '"a"'
    @c.set_dataset @db[:b]
    @c.simple_table.should == '"b"'
    @c.set_dataset @db[:b__a]
    @c.simple_table.should == '"b"."a"'
  end

  it "should simple_pk and simple_table respect dataset's identifier input methods" do
    ds = @db[:ab]
    ds.identifier_input_method = :reverse
    @c.set_dataset ds
    @c.simple_table.should == '"ba"'
    @c.set_primary_key :cd
    @c.simple_pk.should == '"dc"'

    @c.set_dataset ds.from(:ef__gh)
    @c.simple_table.should == '"fe"."hg"'
  end

  it "should have simple_table = nil if passed a non-simple select all dataset to set_dataset" do
    @c.set_dataset @c.db[:a].filter(:active)
    @c.simple_table.should == nil
  end

  it "should have simple_table superclasses setting if inheriting" do
    Class.new(@c).simple_table.should == nil
    @c.set_dataset :a
    Class.new(@c).simple_table.should == '"a"'
  end

  it "should use Dataset#with_sql if simple_table and simple_pk are true" do
    @c.set_dataset :a
    @c.instance_dataset._fetch = @c.dataset._fetch = {:id => 1}
    @c[1].should == @c.load(:id=>1)
    @db.sqls.should == ['SELECT * FROM "a" WHERE "id" = 1']
  end

  it "should not use Dataset#with_sql if either simple_table or simple_pk is nil" do
    @c.set_dataset @db[:a].filter(:active)
    @c.dataset._fetch = {:id => 1}
    @c[1].should == @c.load(:id=>1)
    @db.sqls.should == ['SELECT * FROM "a" WHERE ("active" AND ("id" = 1)) LIMIT 1']
  end
end

describe "Model datasets #with_pk" do
  before do
    @c = Class.new(Sequel::Model(:a))
    @ds = @c.dataset
    @ds._fetch = {:id=>1}
    MODEL_DB.reset
  end

  it "should return the first record where the primary key matches" do
    @ds.with_pk(1).should == @c.load(:id=>1)
    MODEL_DB.sqls.should == ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
  end

  it "should handle existing filters" do
    @ds.filter(:a=>2).with_pk(1)
    MODEL_DB.sqls.should == ["SELECT * FROM a WHERE ((a = 2) AND (a.id = 1)) LIMIT 1"]
  end

  it "should work with string values" do
    @ds.with_pk("foo")
    MODEL_DB.sqls.should == ["SELECT * FROM a WHERE (a.id = 'foo') LIMIT 1"]
  end

  it "should handle an array for composite primary keys" do
    @c.set_primary_key :id1, :id2
    @ds.with_pk([1, 2])
    sqls = MODEL_DB.sqls
    ["SELECT * FROM a WHERE ((a.id1 = 1) AND (a.id2 = 2)) LIMIT 1",
    "SELECT * FROM a WHERE ((a.id2 = 2) AND (a.id1 = 1)) LIMIT 1"].should include(sqls.pop)
    sqls.should == []
  end

  it "should have #[] consider an integer as a primary key lookup" do
    @ds[1].should == @c.load(:id=>1)
    MODEL_DB.sqls.should == ["SELECT * FROM a WHERE (a.id = 1) LIMIT 1"]
  end

  it "should not have #[] consider a string as a primary key lookup" do
    @ds['foo'].should == @c.load(:id=>1)
    MODEL_DB.sqls.should == ["SELECT * FROM a WHERE (foo) LIMIT 1"]
  end
end
