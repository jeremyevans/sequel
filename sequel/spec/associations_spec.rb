require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "associate" do
  it "should use explicit class if given a class, symbol, or string" do
    MODEL_DB.reset
    klass = Class.new(Sequel::Model(:nodes))
    class ParParent < Sequel::Model
    end
    
    klass.associate :many_to_one, :par_parent0, :class=>ParParent
    klass.associate :one_to_many, :par_parent1s, :class=>'ParParent'
    klass.associate :many_to_many, :par_parent2s, :class=>:ParParent
    
    klass.association_reflection(:"par_parent0").associated_class.should == ParParent
    klass.association_reflection(:"par_parent1s").associated_class.should == ParParent
    klass.association_reflection(:"par_parent2s").associated_class.should == ParParent
  end
end

describe Sequel::Model, "many_to_one" do

  before(:each) do
    MODEL_DB.reset

    @c2 = Class.new(Sequel::Model(:nodes)) do
      columns :id, :parent_id, :par_parent_id, :blah
      def self.create_new(values)
        obj = self.new(values)
        obj.instance_variable_set(:@new, false)
        obj
      end
    end

    @dataset = @c2.dataset
  end

  it "should use implicit key if omitted" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.new(:id => 1, :parent_id => 234)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT nodes.* FROM nodes WHERE (id = 234) LIMIT 1"]
  end
  
  it "should use implicit class if omitted" do
    class ParParent < Sequel::Model
    end
    
    @c2.many_to_one :par_parent
    
    d = @c2.new(:id => 1, :par_parent_id => 234)
    p = d.par_parent
    p.class.should == ParParent
    
    MODEL_DB.sqls.should == ["SELECT par_parents.* FROM par_parents WHERE (id = 234) LIMIT 1"]
  end

  it "should use class inside module if given as a string" do
    module Par 
      class Parent < Sequel::Model
      end
    end
    
    @c2.many_to_one :par_parent, :class=>"Par::Parent"
    
    d = @c2.new(:id => 1, :par_parent_id => 234)
    p = d.par_parent
    p.class.should == Par::Parent
    
    MODEL_DB.sqls.should == ["SELECT parents.* FROM parents WHERE (id = 234) LIMIT 1"]
  end

  it "should use explicit key if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah

    d = @c2.new(:id => 1, :blah => 567)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT nodes.* FROM nodes WHERE (id = 567) LIMIT 1"]
  end

  it "should use :select option if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :select=>[:id, :name]
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT id, name FROM nodes WHERE (id = 567) LIMIT 1"]
  end

  it "should return nil if key value is nil" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.new(:id => 1)
    d.parent.should == nil
  end

  it "should cache negative lookup" do
    @c2.many_to_one :parent, :class => @c2
    ds = @c2.dataset
    def ds.fetch_rows(sql, &block)
      MODEL_DB.sqls << sql
    end

    d = @c2.new(:id => 1, :parent_id=>555)
    MODEL_DB.sqls.should == []
    d.parent.should == nil
    MODEL_DB.sqls.should == ['SELECT nodes.* FROM nodes WHERE (id = 555) LIMIT 1']
    d.parent.should == nil
    MODEL_DB.sqls.should == ['SELECT nodes.* FROM nodes WHERE (id = 555) LIMIT 1']
  end

  it "should define a setter method" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.new(:id => 1)
    d.parent = @c2.new(:id => 4321)
    d.values.should == {:id => 1, :parent_id => 4321}

    d.parent = nil
    d.values.should == {:id => 1, :parent_id => nil}

    e = @c2.new(:id => 6677)
    d.parent = e
    d.values.should == {:id => 1, :parent_id => 6677}
  end
  
  it "should not persist changes until saved" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create_new(:id => 1)
    MODEL_DB.reset
    d.parent = @c2.new(:id => 345)
    MODEL_DB.sqls.should == []
    d.save_changes
    MODEL_DB.sqls.should == ['UPDATE nodes SET parent_id = 345 WHERE (id = 1)']
  end

  it "should set cached instance variable when accessed" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create_new(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.instance_variable_get("@parent").should == nil
    ds = @c2.dataset
    def ds.fetch_rows(sql, &block); MODEL_DB.sqls << sql; yield({:id=>234}) end
    e = d.parent 
    MODEL_DB.sqls.should == ["SELECT nodes.* FROM nodes WHERE (id = 234) LIMIT 1"]
    d.instance_variable_get("@parent").should == e
  end

  it "should set cached instance variable when assigned" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.instance_variable_get("@parent").should == nil
    d.parent = @c2.new(:id => 234)
    e = d.parent 
    d.instance_variable_get("@parent").should == e
    MODEL_DB.sqls.should == []
  end

  it "should use cached instance variable if available" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1, :parent_id => 234)
    MODEL_DB.reset
    d.instance_variable_set(:@parent, 42)
    d.parent.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.instance_variable_set(:@parent, 42)
    d.parent(true).should_not == 42 
    MODEL_DB.sqls.should == ["SELECT nodes.* FROM nodes WHERE (id = 234) LIMIT 1"]
  end
  
  it "should have the setter add to the reciprocal one_to_many cached association list if it exists" do
    @c2.many_to_one :parent, :class => @c2
    @c2.one_to_many :children, :class => @c2, :key=>:parent_id
    ds = @c2.dataset
    def ds.fetch_rows(sql, &block)
      MODEL_DB.sqls << sql
    end

    d = @c2.new(:id => 1)
    e = @c2.new(:id => 2)
    MODEL_DB.sqls.should == []
    d.parent = e
    e.children.should_not(include(d))
    MODEL_DB.sqls.should == ['SELECT nodes.* FROM nodes WHERE (parent_id = 2)']

    MODEL_DB.reset
    d = @c2.new(:id => 1)
    e = @c2.new(:id => 2)
    e.children.should_not(include(d))
    MODEL_DB.sqls.should == ['SELECT nodes.* FROM nodes WHERE (parent_id = 2)']
    d.parent = e
    e.children.should(include(d))
    MODEL_DB.sqls.should == ['SELECT nodes.* FROM nodes WHERE (parent_id = 2)']
  end

  it "should have the setter remove the object from the previous associated object's reciprocal one_to_many cached association list if it exists" do
    @c2.many_to_one :parent, :class => @c2
    @c2.one_to_many :children, :class => @c2, :key=>:parent_id
    ds = @c2.dataset
    def ds.fetch_rows(sql, &block)
      MODEL_DB.sqls << sql
    end

    d = @c2.new(:id => 1)
    e = @c2.new(:id => 2)
    f = @c2.new(:id => 3)
    e.children.should_not(include(d))
    f.children.should_not(include(d))
    MODEL_DB.reset
    d.parent = e
    e.children.should(include(d))
    d.parent = f
    f.children.should(include(d))
    e.children.should_not(include(d))
    d.parent = nil
    f.children.should_not(include(d))
    MODEL_DB.sqls.should == []
  end

  it "should have belongs_to alias" do
    @c2.belongs_to :parent, :class => @c2

    d = @c2.create_new(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.instance_variable_get("@parent").should == nil
    ds = @c2.dataset
    def ds.fetch_rows(sql, &block); MODEL_DB.sqls << sql; yield({:id=>234}) end
    e = d.parent 
    MODEL_DB.sqls.should == ["SELECT nodes.* FROM nodes WHERE (id = 234) LIMIT 1"]
    d.instance_variable_get("@parent").should == e
  end
end

describe Sequel::Model, "one_to_many" do

  before(:each) do
    MODEL_DB.reset

    @c1 = Class.new(Sequel::Model(:attributes)) do
      columns :id, :node_id
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      attr_accessor :xxx
      
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
      columns :id
    end
    @dataset = @c2.dataset
    
    @c2.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        yield Hash.new
      end
    })

    @c1.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        yield Hash.new
      end
    })
  end

  it "should use implicit key if omitted" do
    @c2.one_to_many :attributes, :class => @c1 

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes WHERE (node_id = 1234)'
  end
  
  it "should use implicit class if omitted" do
    class HistoricalValue < Sequel::Model
    end
    
    @c2.one_to_many :historical_values
    
    n = @c2.new(:id => 1234)
    v = n.historical_values_dataset
    v.should be_a_kind_of(Sequel::Dataset)
    v.sql.should == 'SELECT historical_values.* FROM historical_values WHERE (node_id = 1234)'
    v.model_classes.should == {nil => HistoricalValue}
  end
  
  it "should use class inside a module if given as a string" do
    module Historical
      class Value < Sequel::Model
      end
    end
    
    @c2.one_to_many :historical_values, :class=>'Historical::Value'
    
    n = @c2.new(:id => 1234)
    v = n.historical_values_dataset
    v.should be_a_kind_of(Sequel::Dataset)
    v.sql.should == 'SELECT values.* FROM values WHERE (node_id = 1234)'
    v.model_classes.should == {nil => Historical::Value}
  end

  it "should use explicit key if given" do
    @c2.one_to_many :attributes, :class => @c1, :key => :nodeid
    
    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes WHERE (nodeid = 1234)'
  end

  it "should define an add_ method" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    a.save!
    MODEL_DB.reset
    a.should == n.add_attribute(a)
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = 1234 WHERE (id = 2345)']
  end

  it "should define a remove_ method" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    a.save!
    MODEL_DB.reset
    a.should == n.remove_attribute(a)
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = NULL WHERE (id = 2345)']
  end
  
  it "should support a select option" do
    @c2.one_to_many :attributes, :class => @c1, :select => [:id, :name]

    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT id, name FROM attributes WHERE (node_id = 1234)"
  end
  
  it "should support an order option" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind

    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT attributes.* FROM attributes WHERE (node_id = 1234) ORDER BY kind"
  end
  
  it "should support an array for the order option" do
    @c2.one_to_many :attributes, :class => @c1, :order => [:kind1, :kind2]

    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT attributes.* FROM attributes WHERE (node_id = 1234) ORDER BY kind1, kind2"
  end
  
  it "should return array with all members of the association" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE (node_id = 1234)']
  end
  
  it "should accept a block" do
    @c2.one_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => @xxx)
    end
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE ((node_id = 1234) AND (xxx IS NULL))']
  end
  
  it "should support order option with block" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind do |ds|
      ds.filter(:xxx => @xxx)
    end
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE ((node_id = 1234) AND (xxx IS NULL)) ORDER BY kind']
  end
  
  it "should set cached instance variable when accessed" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variables.include?("@attributes").should == false
    atts = n.attributes
    atts.should == n.instance_variable_get("@attributes")
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE (node_id = 1234)']
  end

  it "should use cached instance variable if available" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE (node_id = 1234)']
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = []
    n.instance_variable_set(:@attributes, a)
    n.add_attribute(att)
    a.should == [att]
  end

  it "should set object to item's reciprocal cached association variable when calling add_" do
    @c2.one_to_many :attributes, :class => @c1
    @c1.many_to_one :node, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    n.add_attribute(att)
    att.node.should == n
  end

  it "should remove item from cached instance variable if it exists when calling remove_" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = [att]
    n.instance_variable_set(:@attributes, a)
    n.remove_attribute(att)
    a.should == []
  end

  it "should remove item's reciprocal cached association variable when calling remove_" do
    @c2.one_to_many :attributes, :class => @c1
    @c1.many_to_one :node, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    att.instance_variable_set(:@node, n)
    att.node.should == n
    n.remove_attribute(att)
    att.node.should == nil
  end

  it "should have has_many alias" do
    @c2.has_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE (node_id = 1234)']
  end
  
  it "should populate the reciprocal many_to_one instance variable when loading the one_to_many association" do
    @c2.one_to_many :attributes, :class => @c1, :key => :node_id
    @c1.many_to_one :node, :class => @c2, :key => :node_id
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE (node_id = 1234)']
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    atts.first.node.should == n
    
    MODEL_DB.sqls.length.should == 1
  end
  
  it "should use an explicit reciprocal instance variable if given" do
    @c2.one_to_many :attributes, :class => @c1, :key => :node_id, :reciprocal=>'@wxyz'
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes WHERE (node_id = 1234)']
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    atts.first.instance_variable_get('@wxyz').should == n
    
    MODEL_DB.sqls.length.should == 1
  end
end

describe Sequel::Model, "many_to_many" do

  before(:each) do
    MODEL_DB.reset

    @c1 = Class.new(Sequel::Model(:attributes)) do
      def self.name; 'Attribute'; end
      def self.to_s; 'Attribute'; end
      columns :id
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      attr_accessor :xxx
      
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
      columns :id
    end
    @dataset = @c2.dataset

    [@c1, @c2].each do |c|
      c.dataset.extend(Module.new {
        def fetch_rows(sql)
          @db << sql
          yield Hash.new
        end
      })
    end
  end

  it "should use implicit key values and join table if omitted" do
    @c2.many_to_many :attributes, :class => @c1 

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
  it "should use implicit class if omitted" do
    class Tag < Sequel::Model
    end
    
    @c2.many_to_many :tags

    n = @c2.new(:id => 1234)
    a = n.tags_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT tags.* FROM tags INNER JOIN nodes_tags ON ((nodes_tags.tag_id = tags.id) AND (nodes_tags.node_id = 1234))'
  end
  
  it "should use class inside module if given as a string" do
    module Historical
      class Tag < Sequel::Model
      end
    end
    
    @c2.many_to_many :tags, :class=>'::Historical::Tag'

    n = @c2.new(:id => 1234)
    a = n.tags_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT tags.* FROM tags INNER JOIN nodes_tags ON ((nodes_tags.tag_id = tags.id) AND (nodes_tags.node_id = 1234))'
  end
  
  it "should use explicit key values and join table if given" do
    @c2.many_to_many :attributes, :class => @c1, :left_key => :nodeid, :right_key => :attributeid, :join_table => :attribute2node
    
    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attribute2node ON ((attribute2node.attributeid = attributes.id) AND (attribute2node.nodeid = 1234))'
  end
  
  it "should support an order option" do
    @c2.many_to_many :attributes, :class => @c1, :order => :blah

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) ORDER BY blah'
  end
  
  it "should support an array for the order option" do
    @c2.many_to_many :attributes, :class => @c1, :order => [:blah1, :blah2]

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) ORDER BY blah1, blah2'
  end
  
  it "should support a select option" do
    @c2.many_to_many :attributes, :class => @c1, :select => :blah

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT blah FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
  it "should support an array for the select option" do
    @c2.many_to_many :attributes, :class => @c1, :select => [:attributes.*, :attribute_nodes__blah2]

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.*, attribute_nodes.blah2 FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
  it "should accept a block" do
    @c2.many_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    a = n.attributes
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(@c1)
    MODEL_DB.sqls.first.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (xxx = 555)'
  end

  it "should allow the order option while accepting a block" do
    @c2.many_to_many :attributes, :class => @c1, :order=>[:blah1, :blah2] do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    a = n.attributes
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(@c1)
    MODEL_DB.sqls.first.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (xxx = 555) ORDER BY blah1, blah2'
  end

  it "should define an add_ method" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    a.should == n.add_attribute(a)
    ['INSERT INTO attributes_nodes (node_id, attribute_id) VALUES (1234, 2345)',
     'INSERT INTO attributes_nodes (attribute_id, node_id) VALUES (2345, 1234)'
    ].should(include(MODEL_DB.sqls.first))
  end

  it "should define a remove_ method" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    a.should == n.remove_attribute(a)
    MODEL_DB.sqls.first.should == 'DELETE FROM attributes_nodes WHERE ((node_id = 1234) AND (attribute_id = 2345))'
  end

  it "should provide an array with all members of the association" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)

    MODEL_DB.sqls.first.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end

  it "should set cached instance variable when accessed" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variables.include?("@attributes").should == false
    atts = n.attributes
    atts.should == n.instance_variable_get("@attributes")
    MODEL_DB.sqls.length.should == 1
  end

  it "should use cached instance variable if available" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.length.should == 1
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = []
    n.instance_variable_set(:@attributes, a)
    n.add_attribute(att)
    a.should == [att]
  end

  it "should add item to reciprocal cached instance variable if it exists when calling add_" do
    @c2.many_to_many :attributes, :class => @c1
    @c1.many_to_many :nodes, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    att.instance_variable_set(:@nodes, [])
    n.add_attribute(att)
    att.nodes.should == [n]
  end

  it "should remove item from cached instance variable if it exists when calling remove_" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = [att]
    n.instance_variable_set(:@attributes, a)
    n.remove_attribute(att)
    a.should == []
  end

  it "should remove item from reciprocal cached instance variable if it exists when calling remove_" do
    @c2.many_to_many :attributes, :class => @c1
    @c1.many_to_many :nodes, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    att.instance_variable_set(:@nodes, [n])
    n.remove_attribute(att)
    att.nodes.should == []
  end

  it "should have has_and_belongs_to_many alias" do
    @c2.has_and_belongs_to_many :attributes, :class => @c1 

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
end

describe Sequel::Model, "all_association_reflections" do
  before(:each) do
    MODEL_DB.reset
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
    end
  end
  
  it "should include all association reflection hashes" do
    @c1.all_association_reflections.should == []
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.all_association_reflections.should == [{
      :type => :many_to_one, :name => :parent, :class_name => 'Node', 
      :class => @c1, :key => :parent_id, :block => nil, :cache => true,
      :graph_join_type=>:left_outer, :graph_conditions=>[], :eager_block => nil, :model => @c1
    }]
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.should == [{
      :type => :one_to_many, :name => :children, :class_name => 'Node', 
      :class => @c1, :key => :node_id, :block => nil, :cache => true,
      :graph_join_type=>:left_outer, :graph_conditions=>[], :eager_block => nil, :model => @c1}, {
      :type => :many_to_one, :name => :parent, :class_name => 'Node',
      :class => @c1, :key => :parent_id, :block => nil, :cache => true,
      :graph_join_type=>:left_outer, :graph_conditions=>[], :eager_block => nil, :model => @c1}]
  end
end

describe Sequel::Model, "association_reflection" do
  before(:each) do
    MODEL_DB.reset
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
    end
  end

  it "should return nil for nonexistent association" do
    @c1.association_reflection(:blah).should == nil
  end

  it "should return association reflection hash if association exists" do
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.association_reflection(:parent).should == {
      :type => :many_to_one, :name => :parent, :class_name => 'Node', 
      :class => @c1, :key => :parent_id, :block => nil, :cache => true,
      :graph_join_type=>:left_outer, :graph_conditions=>[], :eager_block => nil, :model => @c1
    }
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.association_reflection(:children).should == {
      :type => :one_to_many, :name => :children, :class_name => 'Node', 
      :class => @c1, :key => :node_id, :block => nil, :cache => true,
      :graph_join_type=>:left_outer, :graph_conditions=>[], :eager_block => nil, :model => @c1
    }
  end
end

describe Sequel::Model, "associations" do
  before(:each) do
    MODEL_DB.reset
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
    end
  end

  it "should include all association names" do
    @c1.associations.should == []
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.associations.should == [:parent]
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.associations.sort_by{|x|x.to_s}.should == [:children, :parent]
  end
end
