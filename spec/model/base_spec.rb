require File.join(File.dirname(__FILE__), "spec_helper")

describe "Model attribute setters" do

  before(:each) do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      columns :id, :x, :y
    end
  end

  it "should mark the column value as changed" do
    o = @c.new
    o.changed_columns.should == []

    o.x = 2
    o.changed_columns.should == [:x]

    o.y = 3
    o.changed_columns.should == [:x, :y]

    o.changed_columns.clear

    o[:x] = 2
    o.changed_columns.should == [:x]

    o[:y] = 3
    o.changed_columns.should == [:x, :y]
  end

end

describe "Model#serialize" do

  before(:each) do
    MODEL_DB.reset
  end

  deprec_specify "should translate values to YAML when creating records" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      serialize :abc
      columns :abc
    end

    @c.create(:abc => 1)
    @c.create(:abc => "hello")

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('--- 1\n')", \
      "INSERT INTO items (abc) VALUES ('--- hello\n')", \
    ]
  end

  deprec_specify "should support calling after the class is defined" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      columns :def
    end

    @c.serialize :def

    @c.create(:def => 1)
    @c.create(:def => "hello")

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (def) VALUES ('--- 1\n')", \
      "INSERT INTO items (def) VALUES ('--- hello\n')", \
    ]
  end

  deprec_specify "should support using the Marshal format" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      serialize :abc, :format => :marshal
      columns :abc
    end

    @c.create(:abc => 1)
    @c.create(:abc => "hello")
    x = [Marshal.dump("hello")].pack('m')

    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('BAhpBg==\n')", \
      "INSERT INTO items (abc) VALUES ('#{x}')", \
    ]
  end

  deprec_specify "should translate values to and from YAML using accessor methods" do
    @c = Class.new(Sequel::Model(:items)) do
      serialize :abc, :def
      columns :abc, :def
    end

    ds = @c.dataset
    ds.extend(Module.new {
      attr_accessor :raw

      def fetch_rows(sql, &block)
        block.call(@raw)
      end

      @@sqls = nil

      def insert(*args)
        @@sqls = insert_sql(*args)
      end

      def update(*args)
        @@sqls = update_sql(*args)
      end

      def sqls
        @@sqls
      end

      def columns
        [:id, :abc, :def]
      end
    }
    )

    ds.raw = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}
    o = @c.first
    o.id.should == 1
    o.abc.should == 1
    o.def.should == "hello"

    o.update(:abc => 23)
    ds.sqls.should == "UPDATE items SET abc = '#{23.to_yaml}' WHERE (id = 1)"

    ds.raw = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}
    o = @c.create(:abc => [1, 2, 3])
    ds.sqls.should == "INSERT INTO items (abc) VALUES ('#{[1, 2, 3].to_yaml}')"
  end

end

describe Sequel::Model, "dataset" do
  before do
    @a = Class.new(Sequel::Model(:items))
    @b = Class.new(Sequel::Model)
    
    class Elephant < Sequel::Model(:ele1)
    end
    
    class Maggot < Sequel::Model
    end

    class ShoeSize < Sequel::Model
    end
    
    class BootSize < ShoeSize
    end
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
    module BlahBlah
      class MwaHaHa < Sequel::Model
      end
    end
    
    BlahBlah::MwaHaHa.dataset.sql.should == 'SELECT * FROM mwa_ha_has'
  end
end

describe Sequel::Model, ".def_dataset_method" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
    end
  end
  
  it "should add a method to the dataset and model if called with a block argument" do
    @c.instance_eval do
      def_dataset_method(:return_3){3}
    end
    @c.return_3.should == 3
    @c.dataset.return_3.should == 3
  end

  it "should add all passed methods to the model if called without a block argument" do
    @c.instance_eval do
      def_dataset_method(:return_3, :return_4)
    end
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
    @c.instance_eval do
      def_dataset_method(:return_3){3}
    end
    @c.set_dataset :items
    @c.return_3.should == 3
    @c.dataset.return_3.should == 3
  end

  it "should readd methods to subclasses, if set_dataset is used in a subclass" do
    @c.instance_eval do
      def_dataset_method(:return_3){3}
    end
    c = Class.new(@c)
    c.set_dataset :items
    c.return_3.should == 3
    c.dataset.return_3.should == 3
  end
end

describe "A model class with implicit table name" do
  before do
    class Donkey < Sequel::Model
    end
  end
  
  specify "should have a dataset associated with the model class" do
    Donkey.dataset.model.should == Donkey
  end
end

describe "A model inheriting from a model" do
  before do
    class Feline < Sequel::Model
    end
    
    class Leopard < Feline
    end
  end
  
  specify "should have a dataset associated with itself" do
    Feline.dataset.model.should == Feline
    Leopard.dataset.model.should == Leopard
  end
end

describe "Model.db=" do
  before do
    $db1 = MockDatabase.new
    $db2 = MockDatabase.new
    
    class BlueBlue < Sequel::Model(:items)
      set_dataset $db1[:blue].filter(:x=>1)
    end
  end
  
  specify "should affect the underlying dataset" do
    BlueBlue.db = $db2
    
    BlueBlue.dataset.db.should === $db2
    BlueBlue.dataset.db.should_not === $db1
  end

  specify "should keep the same dataset options" do
    BlueBlue.db = $db2
    BlueBlue.dataset.sql.should == 'SELECT * FROM blue WHERE (x = 1)'
  end

  specify "should use the database for subclasses" do
    BlueBlue.db = $db2
    Class.new(BlueBlue).db.should === $db2
  end
end

describe Sequel::Model, ".(allowed|restricted)_columns " do
  before do
    @c = Class.new(Sequel::Model(:blahblah)) do
      columns :x, :y, :z
      def refresh
        self
      end
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
    i.update(:x => 7, :y => 8, :z => 9)
    i.values.delete(:id) # stupid specs
    i.values.should == {:x => 7, :y => 8}
  end

  it "should not set restricted columns by default" do
    @c.set_restricted_columns :z
    i = @c.new(:x => 1, :y => 2, :z => 3)
    i.values.should == {:x => 1, :y => 2}
    i.set(:x => 4, :y => 5, :z => 6)
    i.values.should == {:x => 4, :y => 5}
    i.update(:x => 7, :y => 8, :z => 9)
    i.values.delete(:id) # stupid specs
    i.values.should == {:x => 7, :y => 8}
  end

  it "should have allowed take precedence over restricted" do
    @c.set_allowed_columns :x, :y
    @c.set_restricted_columns :y, :z
    i = @c.new(:x => 1, :y => 2, :z => 3)
    i.values.should == {:x => 1, :y => 2}
    i.set(:x => 4, :y => 5, :z => 6)
    i.values.should == {:x => 4, :y => 5}
    i.update(:x => 7, :y => 8, :z => 9)
    i.values.delete(:id) # stupid specs
    i.values.should == {:x => 7, :y => 8}
  end
end

describe Sequel::Model, ".(un)?restrict_primary_key\\??" do
  before do
    @c = Class.new(Sequel::Model(:blahblah)) do
      set_primary_key :id
      columns :x, :y, :z, :id
      def refresh
        self
      end
    end
    @c.strict_param_setting = false
    @c.instance_variable_set(:@columns, [:x, :y, :z])
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
      def refresh
        self
      end
    end
    @c.instance_variable_set(:@columns, [:x, :y, :z])
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

describe Sequel::Model, ".[] optimization" do
  before do
    @c = Class.new(Sequel::Model(:a))
    @c.instance_eval do
      def simple_table
        @simple_table
      end
    end
  end

  it "should set simple_pk to the literalized primary key column name if a single primary key" do
    @c.simple_pk.should == 'id'
    @c.set_primary_key :b
    @c.simple_pk.should == 'b'
    @c.set_primary_key :b__a.identifier
    @c.simple_pk.should == 'b__a'
  end

  it "should have simple_pk be blank if compound or no primary key" do
    @c.no_primary_key
    @c.simple_pk.should == nil
    @c.set_primary_key :b, :a
    @c.simple_pk.should == nil
  end

  it "should have simple table set if passed a Symbol to set_dataset" do
    @c.set_dataset :a
    @c.simple_table.should == 'a'
    @c.set_dataset :b
    @c.simple_table.should == 'b'
    @c.set_dataset :b__a
    @c.simple_table.should == 'b.a'
  end

  it "should have simple_table = nil if passed a dataset to set_dataset" do
    @c.set_dataset @c.db[:a]
    @c.simple_table.should == nil
  end

  it "should have simple_table superclasses setting if inheriting" do
    @c.set_dataset :a
    Class.new(@c).simple_table.should == 'a'
    @c.instance_variable_set(:@simple_table, nil)
    Class.new(@c).simple_table.should == nil
    @c.instance_variable_set(:@simple_table, "'b'")
    Class.new(@c).simple_table.should == "'b'"
  end

  specify "should have simple_table = nil if inheriting and sti_key is set" do
    @c.plugin :single_table_inheritance, :x
    Class.new(@c).simple_table.should == nil
  end

  it "should use Dataset#with_sql if simple_table and simple_pk are true" do
    @c.set_dataset :a
    @c.dataset.should_receive(:with_sql).and_return(@c.dataset)
    @c[1]
  end

  it "should not use Dataset#with_sql if either simple_table or simple_pk is nil" do
    @c.set_dataset @c.dataset
    @c.dataset.should_not_receive(:with_sql)
    @c[1]
  end
end
