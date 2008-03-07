require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "many_to_one" do

  before(:each) do
    MODEL_DB.reset

    @c2 = Class.new(Sequel::Model(:nodes)) do
      def columns; [:id, :parent_id]; end
    end

    @dataset = @c2.dataset
  end

  it "should use implicit key if omitted" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.new(:id => 1, :parent_id => 234)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (id = 234) LIMIT 1"]
  end
  
  it "should use implicit class if omitted" do
    class ParParent < Sequel::Model
    end
    
    @c2.many_to_one :par_parent
    
    d = @c2.new(:id => 1, :par_parent_id => 234)
    p = d.par_parent
    p.class.should == ParParent
    
    MODEL_DB.sqls.should == ["SELECT * FROM par_parents WHERE (id = 234) LIMIT 1"]
  end

  it "should use explicit key if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah

    d = @c2.new(:id => 1, :blah => 567)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (id = 567) LIMIT 1"]
  end

  it "should return nil if key value is nil" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.new(:id => 1)
    d.parent.should == nil
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

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.parent = @c2.new(:id => 345)
    MODEL_DB.sqls.should == []
    d.save_changes
    MODEL_DB.sqls.should == ['UPDATE nodes SET parent_id = 345 WHERE (id = 1)']
  end

  it "should set cached instance variable when accessed" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.instance_variables.include?("@parent").should == false
    e = d.parent 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (id = 234) LIMIT 1"]
    d.instance_variable_get("@parent").should == e
  end

  it "should set cached instance variable when assigned" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.instance_variables.include?("@parent").should == false
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
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (id = 234) LIMIT 1"]
  end
  
  it "should have belongs_to alias" do
    @c2.belongs_to :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.instance_variables.include?("@parent").should == false
    e = d.parent 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (id = 234) LIMIT 1"]
    d.instance_variable_get("@parent").should == e
  end
end

describe Sequel::Model, "one_to_many" do

  before(:each) do
    MODEL_DB.reset

    @c1 = Class.new(Sequel::Model(:attributes)) do
      def columns; [:id, :node_id]; end
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      attr_accessor :xxx
      
      def self.name
        'Node'
      end
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
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT * FROM attributes WHERE (node_id = 1234)'
  end
  
  it "should use implicit class if omitted" do
    class HistoricalValue < Sequel::Model
    end
    
    @c2.one_to_many :historical_values
    
    n = @c2.new(:id => 1234)
    v = n.historical_values
    v.should be_a_kind_of(Sequel::Dataset)
    v.sql.should == 'SELECT * FROM historical_values WHERE (node_id = 1234)'
    v.model_classes.should == {nil => HistoricalValue}
  end
  
  it "should use explicit key if given" do
    @c2.one_to_many :attributes, :class => @c1, :key => :nodeid
    
    n = @c2.new(:id => 1234)
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT * FROM attributes WHERE (nodeid = 1234)'
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
  
  it "should accept a block" do
    @c2.one_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 'blah'
    n.attributes.sql.should == "SELECT * FROM attributes WHERE (node_id = 1234) AND (xxx = 'blah')"
  end
  
  it "should support an order option" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind

    n = @c2.new(:id => 1234)
    n.attributes.sql.should == "SELECT * FROM attributes WHERE (node_id = 1234) ORDER BY kind"
  end
  
  it "should support order option with block" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.attributes.sql.should == "SELECT * FROM attributes WHERE (node_id = 1234) AND (xxx IS NULL) ORDER BY kind"
  end
  
  it "should support :cache option for returning array with all members of the association" do
    @c2.one_to_many :attributes, :class => @c1, :cache => true
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (node_id = 1234)']
  end
  
  it "should support :cache option with a block" do
    @c2.one_to_many :attributes, :class => @c1, :cache => true do |ds|
      ds.filter(:xxx => @xxx)
    end
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (node_id = 1234) AND (xxx IS NULL)']
  end
  
  it "should set cached instance variable when accessed" do
    @c2.one_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variables.include?("@attributes").should == false
    atts = n.attributes
    atts.should == n.instance_variable_get("@attributes")
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (node_id = 1234)']
  end

  it "should use cached instance variable if available" do
    @c2.one_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.one_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (node_id = 1234)']
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.one_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = []
    n.instance_variable_set(:@attributes, a)
    n.add_attribute(att)
    a.should == [att]
  end

  it "should remove item from cached instance variable if it exists when calling remove_" do
    @c2.one_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = [att]
    n.instance_variable_set(:@attributes, a)
    n.remove_attribute(att)
    a.should == []
  end

  it "should have has_many alias" do
    @c2.has_many :attributes, :class => @c1, :cache => true
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (node_id = 1234)']
  end
end

describe Sequel::Model, "many_to_many" do

  before(:each) do
    MODEL_DB.reset

    @c1 = Class.new(Sequel::Model(:attributes)) do
      def self.name
        'Attribute'
      end
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      attr_accessor :xxx
      
      def self.name
        'Node'
      end
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
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    ['SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)',
     'SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.node_id = 1234) AND (attributes_nodes.attribute_id = attributes.id)'
    ].should(include(a.sql))
  end
  
  it "should use implicit class if omitted" do
    class Tag < Sequel::Model
    end
    
    @c2.many_to_many :tags

    n = @c2.new(:id => 1234)
    a = n.tags
    a.should be_a_kind_of(Sequel::Dataset)
    ['SELECT * FROM tags INNER JOIN nodes_tags ON (nodes_tags.tag_id = tags.id) AND (nodes_tags.node_id = 1234)',
     'SELECT * FROM tags INNER JOIN nodes_tags ON (nodes_tags.node_id = 1234) AND (nodes_tags.tag_id = tags.id)'
    ].should(include(a.sql))
  end
  
  it "should use explicit key values and join table if given" do
    @c2.many_to_many :attributes, :class => @c1, :left_key => :nodeid, :right_key => :attributeid, :join_table => :attribute2node
    
    n = @c2.new(:id => 1234)
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    ['SELECT * FROM attributes INNER JOIN attribute2node ON (attribute2node.nodeid = 1234) AND (attribute2node.attributeid = attributes.id)',
     'SELECT * FROM attributes INNER JOIN attribute2node ON (attribute2node.attributeid = attributes.id) AND (attribute2node.nodeid = 1234)'
    ].should(include(a.sql))
  end
  
  it "should support order option" do
    @c2.many_to_many :attributes, :class => @c1, :order => :blah

    n = @c2.new(:id => 1234)
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    ['SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234) ORDER BY blah',
     'SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.node_id = 1234) AND (attributes_nodes.attribute_id = attributes.id) ORDER BY blah'
    ].should(include(a.sql))
  end
  
  it "should support optional dataset block" do
    @c2.many_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    ['SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234) WHERE (xxx = 555)',
     'SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.node_id = 1234) AND (attributes_nodes.attribute_id = attributes.id) WHERE (xxx = 555)'
    ].should(include(a.sql))
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
    ['DELETE FROM attributes_nodes WHERE (node_id = 1234) AND (attribute_id = 2345)',
     'DELETE FROM attributes_nodes WHERE (attribute_id = 2345) AND (node_id = 1234)'
    ].should(include(MODEL_DB.sqls.first))
  end

  it "should provide an array with all members of the association (if cache option is specified)" do
    @c2.many_to_many :attributes, :class => @c1, :cache => true
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)

    ['SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)',
     'SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.node_id = 1234) AND (attributes_nodes.attribute_id = attributes.id)'
    ].should(include(MODEL_DB.sqls.first))
  end

  it "should set cached instance variable when accessed" do
    @c2.many_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variables.include?("@attributes").should == false
    atts = n.attributes
    atts.should == n.instance_variable_get("@attributes")
    MODEL_DB.sqls.length.should == 1
  end

  it "should use cached instance variable if available" do
    @c2.many_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.many_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.instance_variable_set(:@attributes, 42)
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.length.should == 1
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.many_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = []
    n.instance_variable_set(:@attributes, a)
    n.add_attribute(att)
    a.should == [att]
  end

  it "should remove item from cached instance variable if it exists when calling remove_" do
    @c2.many_to_many :attributes, :class => @c1, :cache => true

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = [att]
    n.instance_variable_set(:@attributes, a)
    n.remove_attribute(att)
    a.should == []
  end

  it "should have has_and_belongs_to_many alias" do
    @c2.has_and_belongs_to_many :attributes, :class => @c1 

    n = @c2.new(:id => 1234)
    a = n.attributes
    a.should be_a_kind_of(Sequel::Dataset)
    ['SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)',
     'SELECT * FROM attributes INNER JOIN attributes_nodes ON (attributes_nodes.node_id = 1234) AND (attributes_nodes.attribute_id = attributes.id)'
    ].should(include(a.sql))
  end
  
end

describe Sequel::Model, "all_association_reflections" do
  before(:each) do
    MODEL_DB.reset
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name
        'Node'
      end
    end
  end
  
  it "should include all association reflection hashes" do
    @c1.all_association_reflections.should == []
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.all_association_reflections.should == [{
      :type => :many_to_one, :name => :parent, :class_name => 'Node', 
      :class => @c1, :key => :parent_id, :block => nil
    }]
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.should == [{
      :type => :one_to_many, :name => :children, :class_name => 'Node', 
      :class => @c1, :key => :node_id, :block => nil}, {
      :type => :many_to_one, :name => :parent, :class_name => 'Node',
      :class => @c1, :key => :parent_id, :block => nil}]
  end
end

describe Sequel::Model, "association_reflection" do
  before(:each) do
    MODEL_DB.reset
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name
        'Node'
      end
    end
  end

  it "should return nil for nonexistent association" do
    @c1.association_reflection(:blah).should == nil
  end

  it "should return association reflection hash if association exists" do
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.association_reflection(:parent).should == {
      :type => :many_to_one, :name => :parent, :class_name => 'Node', 
      :class => @c1, :key => :parent_id, :block => nil
    }
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.association_reflection(:children).should == {
      :type => :one_to_many, :name => :children, :class_name => 'Node', 
      :class => @c1, :key => :node_id, :block => nil
    }
  end
end

describe Sequel::Model, "associations" do
  before(:each) do
    MODEL_DB.reset
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name
        'Node'
      end
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
