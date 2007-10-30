require File.join(File.dirname(__FILE__), 'spec_helper')

Sequel::Model.db = MODEL_DB = MockDatabase.new

describe Sequel::Model do

  it "should have class method aliased as model" do
    Sequel::Model.instance_methods.should include('model')

    model_a = Class.new Sequel::Model
    model_a.new.model.should be(model_a)
  end

  it "should be associated with a dataset" do
    model_a = Class.new(Sequel::Model) { set_dataset MODEL_DB[:as] }
    
    model_a.dataset.should be_a_kind_of(MockDataset)
    model_a.dataset.opts[:from].should == [:as]

    model_b = Class.new(Sequel::Model) { set_dataset MODEL_DB[:bs] }
    
    model_b.dataset.should be_a_kind_of(MockDataset)
    model_b.dataset.opts[:from].should == [:bs]

    model_a.dataset.opts[:from].should == [:as]
  end

end

describe Sequel::Model, 'w/ primary key' do
  it "should default to ':id'" do
    model_a = Class.new Sequel::Model
    model_a.primary_key.should be_equal(:id)
  end

  it "should be changed through 'set_primary_key'" do
    model_a = Class.new(Sequel::Model) { set_primary_key :a }
    model_a.primary_key.should be_equal(:a)
  end

  it "should support multi argument composite keys" do
    model_a = Class.new(Sequel::Model) { set_primary_key :a, :b }
    model_a.primary_key.should be_eql([:a, :b])
  end

  it "should accept single argument composite keys" do
    model_a = Class.new(Sequel::Model) { set_primary_key [:a, :b] }
    model_a.primary_key.should be_eql([:a, :b])
  end
end

describe Sequel::Model, 'w/o primary key' do
  it "should return nil for primary key" do
    Class.new(Sequel::Model) { no_primary_key }.primary_key.should be_nil
  end

  it "should raise a SequelError on 'this'" do
    instance = Class.new(Sequel::Model) { no_primary_key }.new
    proc { instance.this }.should raise_error(SequelError)
  end
end

describe Sequel::Model, 'with this' do

  before { @example = Class.new Sequel::Model(:examples) }

  it "should return a dataset identifying the record" do
    instance = @example.new :id => 3
    instance.this.sql.should be_eql("SELECT * FROM examples WHERE (id = 3) LIMIT 1")
  end

  it "should support arbitary primary keys" do
    @example.set_primary_key :a

    instance = @example.new :a => 3
    instance.this.sql.should be_eql("SELECT * FROM examples WHERE (a = 3) LIMIT 1")
  end

  it "should support composite primary keys" do
    @example.set_primary_key :x, :y
    instance = @example.new :x => 4, :y => 5

    parts = ['SELECT * FROM examples WHERE %s LIMIT 1',
      '(x = 4) AND (y = 5)', '(y = 5) AND (x = 4)'
    ].map { |expr| Regexp.escape expr }
    regexp = Regexp.new parts.first % "(?:#{parts[1]}|#{parts[2]})"

    instance.this.sql.should match(regexp)
  end

end

describe Sequel::Model, 'with hooks' do

  before do
    MODEL_DB.reset
    Sequel::Model.hooks.clear

    @hooks = %w{
      before_save before_create before_update before_destroy
      after_save after_create after_update after_destroy
    }.select { |hook| !hook.empty? }
  end

  it "should have hooks for everything" do
    Sequel::Model.methods.should include('hooks')
    Sequel::Model.methods.should include(*@hooks)
    @hooks.each do |hook|
      Sequel::Model.hooks[hook.to_sym].should be_an_instance_of(Array)
    end
  end
  it "should be inherited" do
    pending 'soon'

    @hooks.each do |hook|
      Sequel::Model.send(hook.to_sym) { nil }
    end

    model = Class.new Sequel::Model(:models)
    model.hooks.should == Sequel::Model.hooks
  end

  it "should run hooks" do
    pending 'soon'

    test = mock 'Test'
    test.should_receive(:run).exactly(@hooks.length)

    @hooks.each do |hook|
      Sequel::Model.send(hook.to_sym) { test.run }
    end

    model = Class.new Sequel::Model(:models)
    model.hooks.should == Sequel::Model.hooks

    model_instance = model.new
    @hooks.each { |hook| model_instance.run_hooks(hook) }
  end
  it "should run hooks around save and create" do
    pending 'test execution'
  end
  it "should run hooks around save and update" do
    pending 'test execution'
  end
  it "should run hooks around delete" do
    pending 'test execution'
  end

end

context "A new model instance" do
  setup do
    @m = Class.new(Sequel::Model) do
      set_dataset MODEL_DB[:items]
    end
  end
  
  specify "should be marked as new?" do
    o = @m.new
    o.should be_new
  end
  
  specify "should not be marked as new? once it is saved" do
    o = @m.new(:x => 1)
    o.should be_new
    o.save
    o.should_not be_new
  end
  
  specify "should use the last inserted id as primary key if not in values" do
    d = @m.dataset
    def d.insert(*args)
      super
      1234
    end
    
    def d.first
      {:x => 1, :id => 1234}
    end
    
    o = @m.new(:x => 1)
    o.save
    o.id.should == 1234
    
    o = @m.new(:x => 1, :id => 333)
    o.save
    o.id.should == 333
  end
end

describe Sequel::Model do
  before do
    @model = Class.new(Sequel::Model(:items))
  end
  
  it "creates dynamic model subclass with set table name" do
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
    @model.set_dataset(MODEL_DB[:foo])
    @model.table_name.should == :foo
  end
  
  it "sets schema with implicit table name" do
    @model.set_schema do
      primary_key :ssn, :string
    end
    @model.primary_key.should == :ssn
    @model.table_name.should == :items
  end
  
  it "sets schema with explicit table name" do
    @model.set_schema :foo do
      primary_key :id
    end
    @model.primary_key.should == :id
    @model.table_name.should == :foo
  end

  it "puts the lotion in the basket or it gets the hose again" do
    # just kidding!
  end
end

class DummyModelBased < Sequel::Model(:blog)
end

context "Sequel::Model()" do
  specify "should allow reopening of descendant classes" do
    proc do
      eval "class DummyModelBased < Sequel::Model(:blog); end"
    end.should_not raise_error
  end
end

context "A model class" do
  setup do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items))
  end
  
  specify "should be able to create rows in the associated table" do
    o = @c.create(:x => 1)
    o.class.should == @c
    MODEL_DB.sqls.should == ['INSERT INTO items (x) VALUES (1);',  "SELECT * FROM items WHERE (id IN ('INSERT INTO items (x) VALUES (1);')) LIMIT 1"]
  end
  
  specify "should be able to create rows without any values specified" do
    o = @c.create
    o.class.should == @c
    MODEL_DB.sqls.should == ["INSERT INTO items DEFAULT VALUES;", "SELECT * FROM items WHERE (id IN ('INSERT INTO items DEFAULT VALUES;')) LIMIT 1"]
  end
end

context "A model class without a primary key" do
  setup do
    MODEL_DB.reset
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
    end
  end
  
  specify "should be able to insert records without selecting them back" do
    i = nil
    proc {i = @c.create(:x => 1)}.should_not raise_error
    i.class.should be(@c)
    i.values.to_hash.should == {:x => 1}
    
    MODEL_DB.sqls.should == ['INSERT INTO items (x) VALUES (1);']
  end
  
  specify "should raise when deleting" do
    o = @c.new
    proc {o.delete}.should raise_error
  end

  specify "should insert a record when saving" do
    o = @c.new(:x => 2)
    o.should be_new
    o.save
    MODEL_DB.sqls.should == ['INSERT INTO items (x) VALUES (2);']
  end
end

context "Model#serialize" do
  setup do
    MODEL_DB.reset
  end
  
  specify "should translate values to YAML when creating records" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      serialize :abc
    end

    @c.create(:abc => 1)
    @c.create(:abc => "hello")
    
    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('--- 1\n');", \
      "INSERT INTO items (abc) VALUES ('--- hello\n');", \
    ]
  end
  

  specify "should support calling after the class is defined" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
    end
    
    @c.serialize :def

    @c.create(:def => 1)
    @c.create(:def => "hello")
    
    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (def) VALUES ('--- 1\n');", \
      "INSERT INTO items (def) VALUES ('--- hello\n');", \
    ]
  end
  
  specify "should support using the Marshal format" do
    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      serialize :abc, :format => :marshal
    end

    @c.create(:abc => 1)
    @c.create(:abc => "hello")
    
    MODEL_DB.sqls.should == [ \
      "INSERT INTO items (abc) VALUES ('\004\bi\006');", \
      "INSERT INTO items (abc) VALUES ('\004\b\"\nhello');", \
    ]
  end
  
  specify "should translate values to and from YAML using accessor methods" do
    @c = Class.new(Sequel::Model(:items)) do
      serialize :abc, :def
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
    })
      
    ds.raw = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}
    o = @c.first
    o.id.should == 1
    o.abc.should == 1
    o.def.should == "hello"
    
    o.set(:abc => 23)
    ds.sqls.should == "UPDATE items SET abc = '#{23.to_yaml}' WHERE (id = 1)"
    
    ds.raw = {:id => 1, :abc => "--- 1\n", :def => "--- hello\n"}
    o = @c.create(:abc => [1, 2, 3])
    ds.sqls.should == "INSERT INTO items (abc) VALUES ('#{[1, 2, 3].to_yaml}');"
  end
end

context "Model attribute accessors" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end
  end
  
  specify "should be created dynamically" do
    o = @c.new
    
    o.should_not be_respond_to(:x)
    o.x.should be_nil
    o.should be_respond_to(:x)
    
    o.should_not be_respond_to(:x=)
    o.x = 34
    o.x.should == 34
    o.should be_respond_to(:x=)
  end
  
  specify "should raise for a column that doesn't exist in the dataset" do
    o = @c.new
    
    proc {o.x}.should_not raise_error
    proc {o.xx}.should raise_error(SequelError)
    
    proc {o.x = 3}.should_not raise_error
    proc {o.yy = 4}.should raise_error(SequelError)

    proc {o.yy?}.should raise_error(NoMethodError)
  end
  
  specify "should not raise for a column not in the dataset, but for which there's a value" do
    o = @c.new
    
    proc {o.xx}.should raise_error(SequelError)
    proc {o.yy}.should raise_error(SequelError)
    
    o.values[:xx] = 123
    o.values[:yy] = nil
    
    proc {o.xx; o.yy}.should_not raise_error(SequelError)
    
    o.xx.should == 123
    o.yy.should == nil
    
    proc {o.xx = 3}.should raise_error(SequelError)
  end
end

context "Model#new?" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
    end
  end
  
  specify "should be true for a new instance" do
    n = @c.new(:x => 1)
    n.should be_new
  end
  
  specify "should be false after saving" do
    n = @c.new(:x => 1)
    n.save
    n.should_not be_new
  end
end

context "Model.after_create" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      def columns
        [:id, :x, :y]
      end
    end
    
    ds = @c.dataset
    def ds.insert(*args)
      super(*args)
      1
    end
  end

  specify "should be called after creation" do
    s = []
    
    @c.after_create do
      s = MODEL_DB.sqls.dup
    end
    
    n = @c.create(:x => 1)
    MODEL_DB.sqls.should == ["INSERT INTO items (x) VALUES (1);", "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
    s.should == ["INSERT INTO items (x) VALUES (1);", "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
  end
  
  specify "should allow calling save in the hook" do
    @c.after_create do
      values.delete(:x)
      self.id = 2
      save
    end
    
    n = @c.create(:id => 1)
    MODEL_DB.sqls.should == ["INSERT INTO items (id) VALUES (1);", "SELECT * FROM items WHERE (id = 1) LIMIT 1", "UPDATE items SET id = 2 WHERE (id = 1)"]
  end
end

context "Model.subset" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
  end

  specify "should create a filter on the underlying dataset" do
    proc {@c.new_only}.should raise_error(NoMethodError)
    
    @c.subset(:new_only) {:age == 'new'}
    
    @c.new_only.sql.should == "SELECT * FROM items WHERE (age = 'new')"
    @c.dataset.new_only.sql.should == "SELECT * FROM items WHERE (age = 'new')"
    
    @c.subset(:pricey) {:price > 100}
    
    @c.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    @c.dataset.pricey.sql.should == "SELECT * FROM items WHERE (price > 100)"
    
    # check if subsets are composable
    @c.pricey.new_only.sql.should == "SELECT * FROM items WHERE (price > 100) AND (age = 'new')"
    @c.new_only.pricey.sql.should == "SELECT * FROM items WHERE (age = 'new') AND (price > 100)"
  end
end

context "Model.find" do
  setup do
    MODEL_DB.reset
    
    @c = Class.new(Sequel::Model(:items))
    
    $cache_dataset_row = {:name => 'sharon', :id => 1}
    @dataset = @c.dataset
    $sqls = []
    @dataset.extend(Module.new {
      def fetch_rows(sql)
        $sqls << sql
        yield $cache_dataset_row
      end
    })
  end
  
  specify "should return the first record matching the given filter" do
    @c.find(:name => 'sharon').should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (name = 'sharon') LIMIT 1"

    @c.find {"name LIKE 'abc%'".lit}.should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE name LIKE 'abc%' LIMIT 1"
  end
  
  specify "should accept filter blocks" do
    @c.find {:id == 1}.should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id = 1) LIMIT 1"

    @c.find {:x > 1 && :y < 2}.should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE ((x > 1) AND (y < 2)) LIMIT 1"
  end
end

context "Model.[]" do
  setup do
    MODEL_DB.reset
    
    @c = Class.new(Sequel::Model(:items))
    
    $cache_dataset_row = {:name => 'sharon', :id => 1}
    @dataset = @c.dataset
    $sqls = []
    @dataset.extend(Module.new {
      def fetch_rows(sql)
        $sqls << sql
        yield $cache_dataset_row
      end
    })
  end
  
  specify "should return the first record for the given pk" do
    @c[1].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id = 1) LIMIT 1"
    @c[9999].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id = 9999) LIMIT 1"
  end
  
  specify "should work correctly for custom primary key" do
    @c.set_primary_key :name
    @c['sharon'].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (name = 'sharon') LIMIT 1"
  end
  
  specify "should work correctly for composite primary key" do
    @c.set_primary_key [:node_id, :kind]
    @c[3921, 201].should be_a_kind_of(@c)
    $sqls.last.should =~ \
      /^SELECT \* FROM items WHERE (\(node_id = 3921\) AND \(kind = 201\))|(\(kind = 201\) AND \(node_id = 3921\)) LIMIT 1$/
  end
  
  specify "should act as shortcut to find if a hash is given" do
    @c[:id => 1].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (id = 1) LIMIT 1"
    
    @c[:name => ['abc', 'def']].should be_a_kind_of(@c)
    $sqls.last.should == "SELECT * FROM items WHERE (name IN ('abc', 'def')) LIMIT 1"
  end
end

context "A cached model" do
  setup do
    MODEL_DB.reset
    
    @cache_class = Class.new(Hash) do
      attr_accessor :ttl
      def set(k, v, ttl); self[k] = v; @ttl = ttl; end
      def get(k); self[k]; end
    end
    cache = @cache_class.new
    @cache = cache
    
    @c = Class.new(Sequel::Model(:items)) do
      set_cache cache
      
      def self.columns
        [:name, :id]
      end
    end
    
    $cache_dataset_row = {:name => 'sharon', :id => 1}
    @dataset = @c.dataset
    $sqls = []
    @dataset.extend(Module.new {
      def fetch_rows(sql)
        $sqls << sql
        yield $cache_dataset_row
      end
      
      def update(values)
        $sqls << update_sql(values)
        $cache_dataset_row.merge!(values)
      end
      
      def delete
        $sqls << delete_sql
      end
    })
  end
  
  specify "should set the model's cache store" do
    @c.cache_store.should be(@cache)
  end
  
  specify "should have a default ttl of 3600" do
    @c.cache_ttl.should == 3600
  end
  
  specify "should take a ttl option" do
    @c.set_cache @cache, :ttl => 1234
    @c.cache_ttl.should == 1234
  end
  
  specify "should offer a set_cache_ttl method for setting the ttl" do
    @c.cache_ttl.should == 3600
    @c.set_cache_ttl 1234
    @c.cache_ttl.should == 1234
  end
  
  specify "should generate a cache key appropriate to the class" do
    m = @c.new
    m.values[:id] = 1
    m.cache_key.should == "#{m.class}:1"
    
    # custom primary key
    @c.set_primary_key :ttt
    m = @c.new
    m.values[:ttt] = 333
    m.cache_key.should == "#{m.class}:333"
    
    # composite primary key
    @c.set_primary_key [:a, :b, :c]
    m = @c.new
    m.values[:a] = 123
    m.values[:c] = 456
    m.values[:b] = 789
    m.cache_key.should == "#{m.class}:123,789,456"
  end
  
  specify "should raise error if attempting to generate cache_key and primary key value is null" do
    m = @c.new
    proc {m.cache_key}.should raise_error(SequelError)
    
    m.values[:id] = 1
    proc {m.cache_key}.should_not raise_error(SequelError)
  end
  
  specify "should set the cache when reading from the database" do
    $sqls.should == []
    @cache.should be_empty
    
    m = @c[1]
    $sqls.should == ['SELECT * FROM items WHERE (id = 1) LIMIT 1']
    m.values.should == $cache_dataset_row
    @cache[m.cache_key].should == m
    
    # read from cache
    m2 = @c[1]
    $sqls.should == ['SELECT * FROM items WHERE (id = 1) LIMIT 1']
    m2.should == m
    m2.values.should == $cache_dataset_row
  end
  
  specify "should delete the cache when writing to the database" do
    # fill the cache
    m = @c[1]
    @cache[m.cache_key].should == m
    
    m.set(:name => 'tutu')
    @cache.has_key?(m.cache_key).should be_false
    $sqls.last.should == "UPDATE items SET name = 'tutu' WHERE (id = 1)"
    
    m = @c[1]
    @cache[m.cache_key].should == m
    m.name = 'hey'
    m.save
    @cache.has_key?(m.cache_key).should be_false
    $sqls.last.should == "UPDATE items SET name = 'hey', id = 1 WHERE (id = 1)"
  end
  
  specify "should delete the cache when deleting the record" do
    # fill the cache
    m = @c[1]
    @cache[m.cache_key].should == m
    
    m.delete
    @cache.has_key?(m.cache_key).should be_false
    $sqls.last.should == "DELETE FROM items WHERE (id = 1)"
  end
  
  specify "should support #[] as a shortcut to #find with hash" do
    m = @c[:id => 3]
    @cache[m.cache_key].should be_nil
    $sqls.last.should == "SELECT * FROM items WHERE (id = 3) LIMIT 1"
    
    m = @c[1]
    @cache[m.cache_key].should == m
    $sqls.should == ["SELECT * FROM items WHERE (id = 3) LIMIT 1", \
      "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
    
    @c[:id => 4]
    $sqls.should == ["SELECT * FROM items WHERE (id = 3) LIMIT 1", \
      "SELECT * FROM items WHERE (id = 1) LIMIT 1", \
      "SELECT * FROM items WHERE (id = 4) LIMIT 1"]
  end
end

context "Model.one_to_one" do
  setup do
    MODEL_DB.reset
    
    @c1 = Class.new(Sequel::Model(:attributes)) do
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
    end
    
    @dataset = @c2.dataset

    $sqls = []
    @dataset.extend(Module.new {
      def fetch_rows(sql)
        $sqls << sql
        yield({:hey => 1})
      end
      
      def update(values)
        $sqls << update_sql(values)
      end
    })
  end
  
  specify "should use implicit key if omitted" do
    @c2.one_to_one :parent, :from => @c2
    
    d = @c2.new(:id => 1, :parent_id => 234)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:hey => 1}
    
    $sqls.should == ["SELECT * FROM nodes WHERE (id = 234) LIMIT 1"]
  end
  
  specify "should use explicit key if given" do
    @c2.one_to_one :parent, :from => @c2, :key => :blah
    
    d = @c2.new(:id => 1, :blah => 567)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:hey => 1}
    
    $sqls.should == ["SELECT * FROM nodes WHERE (id = 567) LIMIT 1"]
  end
  
  specify "should support plain dataset in the from option" do
    @c2.one_to_one :parent, :from => MODEL_DB[:xyz]

    d = @c2.new(:id => 1, :parent_id => 789)
    p = d.parent
    p.class.should == Hash
    
    MODEL_DB.sqls.should == ["SELECT * FROM xyz WHERE (id = 789) LIMIT 1"]
  end

  specify "should support table name in the from option" do
    @c2.one_to_one :parent, :from => :abc

    d = @c2.new(:id => 1, :parent_id => 789)
    p = d.parent
    p.class.should == Hash
    
    MODEL_DB.sqls.should == ["SELECT * FROM abc WHERE (id = 789) LIMIT 1"]
  end
  
  specify "should return nil if key value is nil" do
    @c2.one_to_one :parent, :from => @c2
    
    d = @c2.new(:id => 1)
    d.parent.should == nil
  end
  
  specify "should define a setter method" do
    @c2.one_to_one :parent, :from => @c2
    
    d = @c2.new(:id => 1)
    d.parent = {:id => 4321}
    d.values.should == {:id => 1, :parent_id => 4321}
    $sqls.last.should == "UPDATE nodes SET parent_id = 4321 WHERE (id = 1)"
    
    d.parent = nil
    d.values.should == {:id => 1, :parent_id => nil}
    $sqls.last.should == "UPDATE nodes SET parent_id = NULL WHERE (id = 1)"
    
    e = @c2.new(:id => 6677)
    d.parent = e
    d.values.should == {:id => 1, :parent_id => 6677}
    $sqls.last.should == "UPDATE nodes SET parent_id = 6677 WHERE (id = 1)"
  end
end

context "Model.one_to_many" do
  setup do
    MODEL_DB.reset
    
    @c1 = Class.new(Sequel::Model(:attributes)) do
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
    end
  end
  
  specify "should define a getter method" do
    @c2.one_to_many :attributes, :from => @c1, :key => :node_id
    
    n = @c2.new(:id => 1234)
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT * FROM attributes WHERE (node_id = 1234)'
  end
  
  specify "should support plain dataset in the from option" do
    @c2.one_to_many :attributes, :from => MODEL_DB[:xyz], :key => :node_id
    
    n = @c2.new(:id => 1234)
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT * FROM xyz WHERE (node_id = 1234)'
  end

  specify "should support table name in the from option" do
    @c2.one_to_many :attributes, :from => :abc, :key => :node_id
    
    n = @c2.new(:id => 1234)
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT * FROM abc WHERE (node_id = 1234)'
  end
end

context "Model#pk" do
  setup do
    @m = Class.new(Sequel::Model)
  end
  
  specify "should be default return the value of the :id column" do
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk.should == 111
  end

  specify "should be return the primary key value for custom primary key" do
    @m.set_primary_key :x
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk.should == 2
  end

  specify "should be return the primary key value for composite primary key" do
    @m.set_primary_key [:y, :x]
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk.should == [3, 2]
  end

  specify "should raise if no primary key" do
    @m.set_primary_key nil
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk}.should raise_error(SequelError)

    @m.no_primary_key
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk}.should raise_error(SequelError)
  end
end

context "Model#pk_hash" do
  setup do
    @m = Class.new(Sequel::Model)
  end
  
  specify "should be default return the value of the :id column" do
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_hash.should == {:id => 111}
  end

  specify "should be return the primary key value for custom primary key" do
    @m.set_primary_key :x
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_hash.should == {:x => 2}
  end

  specify "should be return the primary key value for composite primary key" do
    @m.set_primary_key [:y, :x]
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_hash.should == {:y => 3, :x => 2}
  end

  specify "should raise if no primary key" do
    @m.set_primary_key nil
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk_hash}.should raise_error(SequelError)

    @m.no_primary_key
    m = @m.new(:id => 111, :x => 2, :y => 3)
    proc {m.pk_hash}.should raise_error(SequelError)
  end
end

context "A Model constructor" do
  setup do
    @m = Class.new(Sequel::Model)
  end

  specify "should accept a hash" do
    m = @m.new(:a => 1, :b => 2)
    m.values.should == {:a => 1, :b => 2}
    m.should be_new
  end
  
  specify "should accept a block and yield itself to the block" do
    block_called = false
    m = @m.new {|i| block_called = true; i.should be_a_kind_of(@m); i.values[:a] = 1}
    
    block_called.should be_true
    m.values[:a].should == 1
  end
end

context "Model magic methods" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      @@sqls = []
    
      def self.sqls; @@sqls; end
    
      def fetch_rows(sql)
        @@sqls << sql
        yield({:id => 123, :name => 'hey'})
      end
    end
  
    @m = Class.new(Sequel::Model(@c.new(nil).from(:items)))
  end
  
  specify "should support order_by_xxx" do
    @m.order_by_name.should be_a_kind_of(@c)
    @m.order_by_name.sql.should == "SELECT * FROM items ORDER BY name"
  end

  specify "should support group_by_xxx" do
    @m.group_by_name.should be_a_kind_of(@c)
    @m.group_by_name.sql.should == "SELECT * FROM items GROUP BY name"
  end

  specify "should support filter_by_xxx" do
    @m.filter_by_name('sharon').should be_a_kind_of(@c)
    @m.filter_by_name('sharon').sql.should == "SELECT * FROM items WHERE (name = 'sharon')"
  end
  
  specify "should support all_by_xxx" do
    all = @m.all_by_name('sharon')
    all.class.should == Array
    all.size.should == 1
    all.first.should be_a_kind_of(@m)
    all.first.values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items WHERE (name = 'sharon')"]
  end
  
  specify "should support find_by_xxx" do
    @m.find_by_name('sharon').should be_a_kind_of(@m)
    @m.find_by_name('sharon').values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items WHERE (name = 'sharon') LIMIT 1"] * 2
  end

  specify "should support first_by_xxx" do
    @m.first_by_name('sharon').should be_a_kind_of(@m)
    @m.first_by_name('sharon').values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items ORDER BY name LIMIT 1"] * 2
  end

  specify "should support last_by_xxx" do
    @m.last_by_name('sharon').should be_a_kind_of(@m)
    @m.last_by_name('sharon').values.should == {:id => 123, :name => 'hey'}
    @c.sqls.should == ["SELECT * FROM items ORDER BY name DESC LIMIT 1"] * 2
  end
end