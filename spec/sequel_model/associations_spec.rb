require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "associate" do
  it "should use explicit class if given a class, symbol, or string" do
    MODEL_DB.reset
    klass = Class.new(Sequel::Model(:nodes))
    class ::ParParent < Sequel::Model
    end
    
    klass.associate :many_to_one, :par_parent0, :class=>ParParent
    klass.associate :one_to_many, :par_parent1s, :class=>'ParParent'
    klass.associate :many_to_many, :par_parent2s, :class=>:ParParent
    
    klass.association_reflection(:"par_parent0").associated_class.should == ParParent
    klass.association_reflection(:"par_parent1s").associated_class.should == ParParent
    klass.association_reflection(:"par_parent2s").associated_class.should == ParParent
  end

  it "should add a model_object and association_reflection accessors to the dataset, and return it with the current model object" do
    MODEL_DB.reset
    klass = Class.new(Sequel::Model(:nodes)) do
      columns :id, :a_id
    end
    mod = Module.new do
      def blah
       filter{|o| o.__send__(association_reflection[:key]) > model_object.id*2}
      end
    end

    klass.associate :many_to_one, :a, :class=>klass
    klass.associate :one_to_many, :bs, :key=>:b_id, :class=>klass, :extend=>mod
    klass.associate :many_to_many, :cs, :class=>klass
    
    node = klass.load(:id=>1)
    node.a_dataset.model_object.should == node
    node.bs_dataset.model_object.should == node
    node.cs_dataset.model_object.should == node

    node.a_dataset.association_reflection.should == klass.association_reflection(:a)
    node.bs_dataset.association_reflection.should == klass.association_reflection(:bs)
    node.cs_dataset.association_reflection.should == klass.association_reflection(:cs)

    node.bs_dataset.blah.sql.should == 'SELECT * FROM nodes WHERE ((nodes.b_id = 1) AND (b_id > 2))'
  end

  it "should allow extending the dataset with :extend option" do
    MODEL_DB.reset
    klass = Class.new(Sequel::Model(:nodes)) do
      columns :id, :a_id
    end
    mod = Module.new do
      def blah
       1
      end
    end
    mod2 = Module.new do
      def blar
       2
      end
    end
    
    klass.associate :many_to_one, :a, :class=>klass, :extend=>mod
    klass.associate :one_to_many, :bs, :class=>klass, :extend=>[mod]
    klass.associate :many_to_many, :cs, :class=>klass, :extend=>[mod, mod2]
    
    node = klass.load(:id=>1)
    node.a_dataset.blah.should == 1
    node.bs_dataset.blah.should == 1
    node.cs_dataset.blah.should == 1
    node.cs_dataset.blar.should == 2
  end

  it "should clone an existing association with the :clone option" do
    MODEL_DB.reset
    klass = Class.new(Sequel::Model(:nodes))
    
    klass.many_to_one(:par_parent, :order=>:a){1}
    klass.one_to_many(:par_parent1s, :class=>'ParParent', :limit=>12){4}
    klass.many_to_many(:par_parent2s, :class=>:ParParent, :uniq=>true){2}

    klass.many_to_one :par, :clone=>:par_parent, :select=>:b
    klass.one_to_many :par1s, :clone=>:par_parent1s, :order=>:b, :limit=>10, :block=>nil
    klass.many_to_many(:par2s, :clone=>:par_parent2s, :order=>:c){3}
    
    klass.association_reflection(:par).associated_class.should == ParParent
    klass.association_reflection(:par1s).associated_class.should == ParParent
    klass.association_reflection(:par2s).associated_class.should == ParParent
    
    klass.association_reflection(:par)[:order].should == :a
    klass.association_reflection(:par).select.should == :b
    klass.association_reflection(:par)[:block].call.should == 1
    klass.association_reflection(:par1s)[:limit].should == 10
    klass.association_reflection(:par1s)[:order].should == :b
    klass.association_reflection(:par1s)[:block].should == nil
    klass.association_reflection(:par2s)[:after_load].length.should == 1
    klass.association_reflection(:par2s)[:order].should == :c
    klass.association_reflection(:par2s)[:block].call.should == 3
  end

end

describe Sequel::Model, "many_to_one" do
  before do
    MODEL_DB.reset
    
    @c2 = Class.new(Sequel::Model(:nodes)) do
      unrestrict_primary_key
      columns :id, :parent_id, :par_parent_id, :blah
    end

    @dataset = @c2.dataset
  end

  it "should use implicit key if omitted" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.new(:id => 1, :parent_id => 234)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 234) LIMIT 1"]
  end
  
  it "should use implicit class if omitted" do
    class ::ParParent < Sequel::Model
    end
    
    @c2.many_to_one :par_parent
    
    d = @c2.new(:id => 1, :par_parent_id => 234)
    p = d.par_parent
    p.class.should == ParParent
    
    MODEL_DB.sqls.should == ["SELECT * FROM par_parents WHERE (par_parents.id = 234) LIMIT 1"]
  end

  it "should use class inside module if given as a string" do
    module ::Par 
      class Parent < Sequel::Model
      end
    end
    
    @c2.many_to_one :par_parent, :class=>"Par::Parent"
    
    d = @c2.new(:id => 1, :par_parent_id => 234)
    p = d.par_parent
    p.class.should == Par::Parent
    
    MODEL_DB.sqls.should == ["SELECT * FROM parents WHERE (parents.id = 234) LIMIT 1"]
  end

  it "should use explicit key if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah

    d = @c2.new(:id => 1, :blah => 567)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 567) LIMIT 1"]
  end

  it "should use :primary_key option if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :primary_key => :pk
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.pk = 567) LIMIT 1"]
  end

  it "should use :select option if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :select=>[:id, :name]
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT id, name FROM nodes WHERE (nodes.id = 567) LIMIT 1"]
  end

  it "should use :conditions option if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :conditions=>{:a=>32}
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((nodes.id = 567) AND (a = 32)) LIMIT 1"]

    @c2.many_to_one :parent, :class => @c2, :key => :blah, :conditions=>:a
    MODEL_DB.sqls.clear
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((nodes.id = 567) AND a) LIMIT 1"]
  end

  it "should support :order, :limit (only for offset), and :dataset options, as well as a block" do
    c2 = @c2
    @c2.many_to_one :child_20, :class => @c2, :key=>:id, :dataset=>proc{c2.filter(:parent_id=>pk)}, :limit=>[10,20], :order=>:name do |ds|
      ds.filter(:x.sql_number > 1)
    end
    @c2.load(:id => 100).child_20
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((parent_id = 100) AND (x > 1)) ORDER BY name LIMIT 1 OFFSET 20"]
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
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.id = 555) LIMIT 1']
    d.parent.should == nil
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.id = 555) LIMIT 1']
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
  
  it "should have the setter method respect the :primary_key option" do
    @c2.many_to_one :parent, :class => @c2, :primary_key=>:blah

    d = @c2.new(:id => 1)
    d.parent = @c2.new(:id => 4321, :blah=>444)
    d.values.should == {:id => 1, :parent_id => 444}

    d.parent = nil
    d.values.should == {:id => 1, :parent_id => nil}

    e = @c2.new(:id => 6677, :blah=>8)
    d.parent = e
    d.values.should == {:id => 1, :parent_id => 8}
  end
  
  it "should not persist changes until saved" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.load(:id => 1)
    MODEL_DB.reset
    d.parent = @c2.new(:id => 345)
    MODEL_DB.sqls.should == []
    d.save_changes
    MODEL_DB.sqls.should == ['UPDATE nodes SET parent_id = 345 WHERE (id = 1)']
  end

  it "should set cached instance variable when accessed" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.load(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.associations[:parent].should == nil
    ds = @c2.dataset
    def ds.fetch_rows(sql, &block); MODEL_DB.sqls << sql; yield({:id=>234}) end
    e = d.parent 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 234) LIMIT 1"]
    d.associations[:parent].should == e
  end

  it "should set cached instance variable when assigned" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.associations[:parent].should == nil
    d.parent = @c2.new(:id => 234)
    e = d.parent 
    d.associations[:parent].should == e
    MODEL_DB.sqls.should == []
  end

  it "should use cached instance variable if available" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1, :parent_id => 234)
    MODEL_DB.reset
    d.associations[:parent] = 42
    d.parent.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.associations[:parent] = 42
    d.parent(true).should_not == 42 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 234) LIMIT 1"]
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
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.parent_id = 2)']

    MODEL_DB.reset
    d = @c2.new(:id => 1)
    e = @c2.new(:id => 2)
    e.children.should_not(include(d))
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.parent_id = 2)']
    d.parent = e
    e.children.should(include(d))
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.parent_id = 2)']
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

  it "should get all matching records and only return the first if :key option is set to nil" do
    c2 = @c2
    @c2.one_to_many :children, :class => @c2, :key=>:parent_id
    @c2.many_to_one :first_grand_parent, :class => @c2, :key=>nil, :eager_graph=>:children, :dataset=>proc{c2.filter(:children_id=>parent_id)}
    ds = @c2.dataset
    def ds.columns
      [:id, :parent_id, :par_parent_id, :blah]
    end
    def ds.fetch_rows(sql, &block)
      MODEL_DB.sqls << sql
      yield({:id=>1, :parent_id=>0, :par_parent_id=>3, :blah=>4, :children_id=>2, :children_parent_id=>1, :children_par_parent_id=>5, :children_blah=>6})
    end
    p = @c2.new(:parent_id=>2)
    fgp = p.first_grand_parent
    MODEL_DB.sqls.should == ["SELECT nodes.id, nodes.parent_id, nodes.par_parent_id, nodes.blah, children.id AS children_id, children.parent_id AS children_parent_id, children.par_parent_id AS children_par_parent_id, children.blah AS children_blah FROM nodes LEFT OUTER JOIN nodes AS children ON (children.parent_id = nodes.id) WHERE (children_id = 2)"]
    fgp.values.should == {:id=>1, :parent_id=>0, :par_parent_id=>3, :blah=>4}
    fgp.children.first.values.should == {:id=>2, :parent_id=>1, :par_parent_id=>5, :blah=>6}
  end

  it "should not create the setter method if :read_only option is used" do
    @c2.many_to_one :parent, :class => @c2, :read_only=>true
    @c2.instance_methods.collect{|x| x.to_s}.should(include('parent'))
    @c2.instance_methods.collect{|x| x.to_s}.should_not(include('parent='))
  end

  it "should not add associations methods directly to class" do
    @c2.many_to_one :parent, :class => @c2
    @c2.instance_methods.collect{|x| x.to_s}.should(include('parent'))
    @c2.instance_methods.collect{|x| x.to_s}.should(include('parent='))
    @c2.instance_methods(false).collect{|x| x.to_s}.should_not(include('parent'))
    @c2.instance_methods(false).collect{|x| x.to_s}.should_not(include('parent='))
  end

  it "should raise an error if trying to set a model object that doesn't have a valid primary key" do
    @c2.many_to_one :parent, :class => @c2
    p = @c2.new
    c = @c2.load(:id=>123)
    proc{c.parent = p}.should raise_error(Sequel::Error)
  end

  it "should have belongs_to alias" do
    @c2.belongs_to :parent, :class => @c2

    d = @c2.load(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.associations[:parent].should == nil
    ds = @c2.dataset
    def ds.fetch_rows(sql, &block); MODEL_DB.sqls << sql; yield({:id=>234}) end
    e = d.parent 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 234) LIMIT 1"]
    d.associations[:parent].should == e
  end

  it "should make the change to the foreign_key value inside a _association= method" do
    @c2.many_to_one :parent, :class => @c2
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_parent="))
    p = @c2.new
    c = @c2.load(:id=>123)
    def p._parent=(x)
      @x = x
    end
    p.should_not_receive(:parent_id=)
    p.parent = c
    p.instance_variable_get(:@x).should == c
  end

  it "should support (before|after)_(add|remove) callbacks" do
    h = []
    @c2.many_to_one :parent, :class => @c2, :before_add=>[proc{|x,y| h << x.pk; h << -y.pk}, :blah], :after_add=>proc{h << 3}, :before_remove=>:blah, :after_remove=>[:blahr]
    @c2.class_eval do
      @@blah = h
      def []=(a, v)
        a == :parent_id ? (@@blah << (v ? 4 : 5)) : super
      end
      def blah(x)
        @@blah << x.pk
      end
      def blahr(x)
        @@blah << 6
      end
    end
    p = @c2.load(:id=>10)
    c = @c2.load(:id=>123)
    h.should == []
    p.parent = c
    h.should == [10, -123, 123, 4, 3]
    p.parent = nil
    h.should == [10, -123, 123, 4, 3, 123, 5, 6]
  end

  it "should support after_load association callback" do
    h = []
    @c2.many_to_one :parent, :class => @c2, :after_load=>[proc{|x,y| h << [x.pk, y.pk]}, :al]
    @c2.class_eval do
      @@blah = h
      def al(v)
        @@blah << v.pk
      end
      def @dataset.fetch_rows(sql)
        yield({:id=>20})
      end
    end
    p = @c2.load(:id=>10, :parent_id=>20)
    parent = p.parent
    h.should == [[10, 20], 20]
    parent.pk.should == 20
  end

  it "should raise error and not call internal add or remove method if before callback returns false, even if raise_on_save_failure is false" do
    # The reason for this is that assignment in ruby always returns the argument instead of the result
    # of the method, so we can't return nil to signal that the association callback prevented the modification
    p = @c2.new
    c = @c2.load(:id=>123)
    p.raise_on_save_failure = false
    @c2.many_to_one :parent, :class => @c2, :before_add=>:ba, :before_remove=>:br
    p.should_receive(:ba).once.with(c).and_return(false)
    p.should_not_receive(:_parent=)
    proc{p.parent = c}.should raise_error(Sequel::Error)
    p.parent.should == nil
    p.associations[:parent] = c
    p.parent.should == c
    p.should_receive(:br).once.with(c).and_return(false)
    proc{p.parent = nil}.should raise_error(Sequel::Error)
  end

  it "should raise an error if a callback is not a proc or symbol" do
    @c2.many_to_one :parent, :class => @c2, :before_add=>Object.new
    proc{@c2.new.parent = @c2.load(:id=>1)}.should raise_error(Sequel::Error)
  end

  it "should call the remove callbacks for the previous object and the add callbacks for the new object" do
    c = @c2.load(:id=>123)
    d = @c2.load(:id=>321)
    p = @c2.new
    p.associations[:parent] = d
    h = []
    @c2.many_to_one :parent, :class => @c2, :before_add=>:ba, :before_remove=>:br, :after_add=>:aa, :after_remove=>:ar
    @c2.class_eval do
      @@blah = h
      def []=(a, v)
        a == :parent_id ? (@@blah << 5) : super
      end
      def ba(x)
        @@blah << x.pk
      end
      def br(x)
        @@blah << x.pk * -1
      end
      def aa(x)
        @@blah << x.pk * 2
      end
      def ar(x)
        @@blah << x.pk * -2
      end
    end
    p.parent = c
    h.should == [-321, 123, 5, 246, -642]
  end
end

describe Sequel::Model, "one_to_many" do

  before(:each) do
    MODEL_DB.reset

    @c1 = Class.new(Sequel::Model(:attributes)) do
      unrestrict_primary_key
      columns :id, :node_id
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      unrestrict_primary_key
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
    a.sql.should == 'SELECT * FROM attributes WHERE (attributes.node_id = 1234)'
  end
  
  it "should use implicit class if omitted" do
    class ::HistoricalValue < Sequel::Model
    end
    
    @c2.one_to_many :historical_values
    
    n = @c2.new(:id => 1234)
    v = n.historical_values_dataset
    v.should be_a_kind_of(Sequel::Dataset)
    v.sql.should == 'SELECT * FROM historical_values WHERE (historical_values.node_id = 1234)'
    v.model.should == HistoricalValue
  end
  
  it "should use class inside a module if given as a string" do
    module ::Historical
      class Value < Sequel::Model
      end
    end
    
    @c2.one_to_many :historical_values, :class=>'Historical::Value'
    
    n = @c2.new(:id => 1234)
    v = n.historical_values_dataset
    v.should be_a_kind_of(Sequel::Dataset)
    v.sql.should == 'SELECT * FROM values WHERE (values.node_id = 1234)'
    v.model.should == Historical::Value
  end

  it "should use explicit key if given" do
    @c2.one_to_many :attributes, :class => @c1, :key => :nodeid
    
    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT * FROM attributes WHERE (attributes.nodeid = 1234)'
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
  
  it "should have add_ method respect the :primary_key option" do
    @c2.one_to_many :attributes, :class => @c1, :primary_key=>:xxx
    
    n = @c2.new(:id => 1234, :xxx=>5)
    a = @c1.new(:id => 2345)
    a.save!
    MODEL_DB.reset
    a.should == n.add_attribute(a)
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = 5 WHERE (id = 2345)']
  end

  
  it "should raise an error in add_ and remove_ if the passed object returns false to save (is not valid)" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    def a.valid?; false; end
    proc{n.add_attribute(a)}.should raise_error(Sequel::Error)
    proc{n.remove_attribute(a)}.should raise_error(Sequel::Error)
  end

  it "should raise an error if the model object doesn't have a valid primary key" do
    @c2.one_to_many :attributes, :class => @c1 
    a = @c2.new
    n = @c1.load(:id=>123)
    proc{a.attributes_dataset}.should raise_error(Sequel::Error)
    proc{a.attributes}.should raise_error(Sequel::Error)
    proc{a.add_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_all_attributes}.should raise_error(Sequel::Error)
  end
  
  it "should use :primary_key option if given" do
    @c1.one_to_many :nodes, :class => @c2, :primary_key => :node_id, :key=>:id
    n = @c1.load(:id => 1234, :node_id=>4321)
    n.nodes_dataset.sql.should == "SELECT * FROM nodes WHERE (nodes.id = 4321)"
  end

  it "should support a select option" do
    @c2.one_to_many :attributes, :class => @c1, :select => [:id, :name]

    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT id, name FROM attributes WHERE (attributes.node_id = 1234)"
  end
  
  it "should support a conditions option" do
    @c2.one_to_many :attributes, :class => @c1, :conditions => {:a=>32}
    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (a = 32))"
    @c2.one_to_many :attributes, :class => @c1, :conditions => ~:a
    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND NOT a)"
  end
  
  it "should support an order option" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind

    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT * FROM attributes WHERE (attributes.node_id = 1234) ORDER BY kind"
  end
  
  it "should support an array for the order option" do
    @c2.one_to_many :attributes, :class => @c1, :order => [:kind1, :kind2]

    n = @c2.new(:id => 1234)
    n.attributes_dataset.sql.should == "SELECT * FROM attributes WHERE (attributes.node_id = 1234) ORDER BY kind1, kind2"
  end
  
  it "should return array with all members of the association" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
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
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (xxx IS NULL))']
  end
  
  it "should support :order option with block" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind do |ds|
      ds.filter(:xxx => @xxx)
    end
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (xxx IS NULL)) ORDER BY kind']
  end
  
  it "should have the block argument affect the _dataset method" do
    @c2.one_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => 456)
    end
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (xxx = 456))'
  end
  
  it "should support a :dataset option that is used instead of the default" do
    c1 = @c1
    @c2.one_to_many :all_other_attributes, :class => @c1, :dataset=>proc{c1.filter(:nodeid=>pk).invert}, :order=>:a, :limit=>10 do |ds|
      ds.filter(:xxx => 5)
    end
    
    @c2.new(:id => 1234).all_other_attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE ((nodeid != 1234) AND (xxx = 5)) ORDER BY a LIMIT 10'
    n = @c2.new(:id => 1234)
    atts = n.all_other_attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE ((nodeid != 1234) AND (xxx = 5)) ORDER BY a LIMIT 10']
  end
  
  it "should support a :limit option" do
    @c2.one_to_many :attributes, :class => @c1 , :limit=>10
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE (attributes.node_id = 1234) LIMIT 10'
    @c2.one_to_many :attributes, :class => @c1 , :limit=>[10,10]
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE (attributes.node_id = 1234) LIMIT 10 OFFSET 10'
  end

  it "should have the :eager option affect the _dataset method" do
    @c2.one_to_many :attributes, :class => @c2 , :eager=>:attributes
    @c2.new(:id => 1234).attributes_dataset.opts[:eager].should == {:attributes=>nil}
  end
  
  it "should set cached instance variable when accessed" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.associations.include?(:attributes).should == false
    atts = n.attributes
    atts.should == n.associations[:attributes]
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
  end

  it "should use cached instance variable if available" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.associations[:attributes] = 42
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.associations[:attributes] = 42
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.one_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = []
    n.associations[:attributes] = a
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

    n = @c2.load(:id => 1234)
    att = @c1.load(:id => 345)
    MODEL_DB.reset
    a = [att]
    n.associations[:attributes] = a
    n.remove_attribute(att)
    a.should == []
  end

  it "should remove item's reciprocal cached association variable when calling remove_" do
    @c2.one_to_many :attributes, :class => @c1
    @c1.many_to_one :node, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    att.associations[:node] = n
    att.node.should == n
    n.remove_attribute(att)
    att.node.should == nil
  end

  it "should not create the add_, remove_, or remove_all_ methods if :read_only option is used" do
    @c2.one_to_many :attributes, :class => @c1, :read_only=>true
    im = @c2.instance_methods.collect{|x| x.to_s}
    im.should(include('attributes'))
    im.should(include('attributes_dataset'))
    im.should_not(include('add_attribute'))
    im.should_not(include('remove_attribute'))
    im.should_not(include('remove_all_attributes'))
  end

  it "should not add associations methods directly to class" do
    @c2.one_to_many :attributes, :class => @c1
    im = @c2.instance_methods.collect{|x| x.to_s}
    im.should(include('attributes'))
    im.should(include('attributes_dataset'))
    im.should(include('add_attribute'))
    im.should(include('remove_attribute'))
    im.should(include('remove_all_attributes'))
    im2 = @c2.instance_methods(false).collect{|x| x.to_s}
    im2.should_not(include('attributes'))
    im2.should_not(include('attributes_dataset'))
    im2.should_not(include('add_attribute'))
    im2.should_not(include('remove_attribute'))
    im2.should_not(include('remove_all_attributes'))
  end

  it "should have has_many alias" do
    @c2.has_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
  end
  
  it "should populate the reciprocal many_to_one instance variable when loading the one_to_many association" do
    @c2.one_to_many :attributes, :class => @c1, :key => :node_id
    @c1.many_to_one :node, :class => @c2, :key => :node_id
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    atts.first.node.should == n
    
    MODEL_DB.sqls.length.should == 1
  end
  
  it "should use an explicit reciprocal instance variable if given" do
    @c2.one_to_many :attributes, :class => @c1, :key => :node_id, :reciprocal=>:wxyz
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
    atts.should be_a_kind_of(Array)
    atts.size.should == 1
    atts.first.should be_a_kind_of(@c1)
    atts.first.values.should == {}
    atts.first.associations[:wxyz].should == n
    
    MODEL_DB.sqls.length.should == 1
  end
  
  it "should have an remove_all_ method that removes all associations" do
    @c2.one_to_many :attributes, :class => @c1
    @c2.new(:id => 1234).remove_all_attributes
    MODEL_DB.sqls.first.should == 'UPDATE attributes SET node_id = NULL WHERE (node_id = 1234)'
  end

  it "should have the remove_all_ method respect the :primary_key option" do
    @c2.one_to_many :attributes, :class => @c1, :primary_key=>:xxx
    @c2.new(:id => 1234, :xxx=>5).remove_all_attributes
    MODEL_DB.sqls.first.should == 'UPDATE attributes SET node_id = NULL WHERE (node_id = 5)'
  end

  it "remove_all should set the cached instance variable to []" do
    @c2.one_to_many :attributes, :class => @c1
    node = @c2.new(:id => 1234)
    node.remove_all_attributes
    node.associations[:attributes].should == []
  end

  it "remove_all should return the array of previously associated items if the cached instance variable exists" do
    @c2.one_to_many :attributes, :class => @c1
    attrib = @c1.new(:id=>3)
    node = @c2.new(:id => 1234)
    d = @c1.dataset
    def d.fetch_rows(s); end
    node.attributes.should == []
    def attrib.save!; self end
    node.add_attribute(attrib)
    node.associations[:attributes].should == [attrib]
    node.remove_all_attributes.should == [attrib]
  end

  it "remove_all should return nil if the cached instance variable does not exist" do
    @c2.one_to_many :attributes, :class => @c1
    @c2.new(:id => 1234).remove_all_attributes.should == nil
  end

  it "remove_all should remove the current item from all reciprocal instance varaibles if it cached instance variable exists" do
    @c2.one_to_many :attributes, :class => @c1
    @c1.many_to_one :node, :class => @c2
    d = @c1.dataset
    def d.fetch_rows(s); end
    d = @c2.dataset
    def d.fetch_rows(s); end
    attrib = @c1.new(:id=>3)
    node = @c2.new(:id => 1234)
    node.attributes.should == []
    attrib.node.should == nil
    def attrib.save!; self end
    node.add_attribute(attrib)
    attrib.associations[:node].should == node 
    node.remove_all_attributes
    attrib.associations.fetch(:node, 2).should == nil
  end

  it "should add a getter method if the :one_to_one option is true" do
    @c2.one_to_many :attributes, :class => @c1, :one_to_one=>true
    att = @c2.new(:id => 1234).attribute
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
    att.should be_a_kind_of(@c1)
    att.values.should == {}
  end

  it "should not add a setter method if the :one_to_one option is true and :read_only option is true" do
    @c2.one_to_many :attributes, :class => @c1, :one_to_one=>true, :read_only=>true
    im = @c2.instance_methods.collect{|x| x.to_s}
    im.should(include('attribute'))
    im.should_not(include('attribute='))
  end

  it "should have the getter method raise an error if more than one record is found" do
    @c2.one_to_many :attributes, :class => @c1, :one_to_one=>true
    d = @c1.dataset
    def d.fetch_rows(s); 2.times{yield Hash.new} end
    proc{@c2.new(:id => 1234).attribute}.should raise_error(Sequel::Error)
  end

  it "should add a setter method if the :one_to_one option is true" do
    @c2.one_to_many :attributes, :class => @c1, :one_to_one=>true
    attrib = @c1.new(:id=>3)
    d = @c1.dataset
    def d.fetch_rows(s); yield({:id=>3}) end
    @c2.new(:id => 1234).attribute = attrib
    ['INSERT INTO attributes (node_id, id) VALUES (1234, 3)',
      'INSERT INTO attributes (id, node_id) VALUES (3, 1234)'].should(include(MODEL_DB.sqls.first))
    MODEL_DB.sqls.last.should == 'UPDATE attributes SET node_id = NULL WHERE ((node_id = 1234) AND (id != 3))'
    MODEL_DB.sqls.length.should == 2
    @c2.new(:id => 1234).attribute.should == attrib
    MODEL_DB.sqls.clear
    attrib = @c1.load(:id=>3)
    @c2.new(:id => 1234).attribute = attrib
    MODEL_DB.sqls.length.should == 2
    MODEL_DB.sqls.first.should =~ /UPDATE attributes SET (node_id = 1234, id = 3|id = 3, node_id = 1234) WHERE \(id = 3\)/
    MODEL_DB.sqls.last.should == 'UPDATE attributes SET node_id = NULL WHERE ((node_id = 1234) AND (id != 3))'
  end

  it "should have the setter method for the :one_to_one option respect the :primary_key option" do
    @c2.one_to_many :attributes, :class => @c1, :one_to_one=>true, :primary_key=>:xxx
    attrib = @c1.new(:id=>3)
    d = @c1.dataset
    def d.fetch_rows(s); yield({:id=>3}) end
    @c2.new(:id => 1234, :xxx=>5).attribute = attrib
    ['INSERT INTO attributes (node_id, id) VALUES (5, 3)',
      'INSERT INTO attributes (id, node_id) VALUES (3, 5)'].should(include(MODEL_DB.sqls.first))
    MODEL_DB.sqls.last.should == 'UPDATE attributes SET node_id = NULL WHERE ((node_id = 5) AND (id != 3))'
    MODEL_DB.sqls.length.should == 2
    @c2.new(:id => 321, :xxx=>5).attribute.should == attrib
    MODEL_DB.sqls.clear
    attrib = @c1.load(:id=>3)
    @c2.new(:id => 621, :xxx=>5).attribute = attrib
    MODEL_DB.sqls.length.should == 2
    MODEL_DB.sqls.first.should =~ /UPDATE attributes SET (node_id = 5, id = 3|id = 3, node_id = 5) WHERE \(id = 3\)/
    MODEL_DB.sqls.last.should == 'UPDATE attributes SET node_id = NULL WHERE ((node_id = 5) AND (id != 3))'
  end

  it "should raise an error if the one_to_one getter would be the same as the association name" do
    proc{@c2.one_to_many :song, :class => @c1, :one_to_one=>true}.should raise_error(Sequel::Error)
  end

  it "should not create remove_ and remove_all methods if :one_to_one option is used" do
    @c2.one_to_many :attributes, :class => @c1, :one_to_one=>true
    @c2.new.should_not(respond_to(:remove_attribute))
    @c2.new.should_not(respond_to(:remove_all_attributes))
  end

  it "should make non getter and setter methods private if :one_to_one option is used" do 
    @c2.one_to_many :attributes, :class => @c1, :one_to_one=>true do |ds| end
    meths = @c2.private_instance_methods.collect{|x| x.to_s}
    meths.should(include("attributes"))
    meths.should(include("add_attribute"))
    meths.should(include("attributes_dataset"))
  end

  it "should call an _add_ method internally to add attributes" do
    @c2.one_to_many :attributes, :class => @c1
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_add_attribute"))
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._add_attribute(x)
      @x = x
    end
    c.should_not_receive(:node_id=)
    p.add_attribute(c)
    p.instance_variable_get(:@x).should == c
  end

  it "should call a _remove_ method internally to remove attributes" do
    @c2.one_to_many :attributes, :class => @c1
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_remove_attribute"))
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._remove_attribute(x)
      @x = x
    end
    c.should_not_receive(:node_id=)
    p.remove_attribute(c)
    p.instance_variable_get(:@x).should == c
  end

  it "should support (before|after)_(add|remove) callbacks" do
    h = []
    @c2.one_to_many :attributes, :class => @c1, :before_add=>[proc{|x,y| h << x.pk; h << -y.pk}, :blah], :after_add=>proc{h << 3}, :before_remove=>:blah, :after_remove=>[:blahr]
    @c2.class_eval do
      @@blah = h
      def _add_attribute(v)
        @@blah << 4
      end
      def _remove_attribute(v)
        @@blah << 5
      end
      def blah(x)
        @@blah << x.pk
      end
      def blahr(x)
        @@blah << 6
      end
    end
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    h.should == []
    p.add_attribute(c)
    h.should == [10, -123, 123, 4, 3]
    p.remove_attribute(c)
    h.should == [10, -123, 123, 4, 3, 123, 5, 6]
  end

  it "should support after_load association callback" do
    h = []
    @c2.one_to_many :attributes, :class => @c1, :after_load=>[proc{|x,y| h << [x.pk, y.collect{|z|z.pk}]}, :al]
    @c2.class_eval do
      @@blah = h
      def al(v)
        v.each{|x| @@blah << x.pk}
      end
    end
    @c1.class_eval do
      def @dataset.fetch_rows(sql)
        yield({:id=>20})
        yield({:id=>30})
      end
    end
    p = @c2.load(:id=>10, :parent_id=>20)
    attributes = p.attributes
    h.should == [[10, [20, 30]], 20, 30]
    attributes.collect{|a| a.pk}.should == [20, 30]
  end

  it "should raise error and not call internal add or remove method if before callback returns false if raise_on_save_failure is true" do
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    @c2.one_to_many :attributes, :class => @c1, :before_add=>:ba, :before_remove=>:br
    p.should_receive(:ba).once.with(c).and_return(false)
    p.should_not_receive(:_add_attribute)
    p.should_not_receive(:_remove_attribute)
    p.associations[:attributes] = []
    proc{p.add_attribute(c)}.should raise_error(Sequel::Error)
    p.attributes.should == []
    p.associations[:attributes] = [c]
    p.should_receive(:br).once.with(c).and_return(false)
    proc{p.remove_attribute(c)}.should raise_error(Sequel::Error)
    p.attributes.should == [c]
  end

  it "should return nil and not call internal add or remove method if before callback returns false if raise_on_save_failure is false" do
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    p.raise_on_save_failure = false
    @c2.one_to_many :attributes, :class => @c1, :before_add=>:ba, :before_remove=>:br
    p.should_receive(:ba).once.with(c).and_return(false)
    p.should_not_receive(:_add_attribute)
    p.should_not_receive(:_remove_attribute)
    p.associations[:attributes] = []
    p.add_attribute(c).should == nil
    p.attributes.should == []
    p.associations[:attributes] = [c]
    p.should_receive(:br).once.with(c).and_return(false)
    p.remove_attribute(c).should == nil
    p.attributes.should == [c]
  end
end

describe Sequel::Model, "many_to_many" do

  before(:each) do
    MODEL_DB.reset

    @c1 = Class.new(Sequel::Model(:attributes)) do
      unrestrict_primary_key
      attr_accessor :yyy
      def self.name; 'Attribute'; end
      def self.to_s; 'Attribute'; end
      columns :id
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      unrestrict_primary_key
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
    class ::Tag < Sequel::Model
    end
    
    @c2.many_to_many :tags

    n = @c2.new(:id => 1234)
    a = n.tags_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT tags.* FROM tags INNER JOIN nodes_tags ON ((nodes_tags.tag_id = tags.id) AND (nodes_tags.node_id = 1234))'
  end
  
  it "should use class inside module if given as a string" do
    module ::Historical
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
  
  it "should support a conditions option" do
    @c2.many_to_many :attributes, :class => @c1, :conditions => {:a=>32}
    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (a = 32)'
    @c2.many_to_many :attributes, :class => @c1, :conditions => ['a = ?', 32]
    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (a = 32)'
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
  
  it "should support :left_primary_key and :right_primary_key options" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx, :right_primary_key=>:yyy
    @c2.new(:id => 1234, :xxx=>5).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.yyy) AND (attributes_nodes.node_id = 5))'
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

  it "should allow the :order option while accepting a block" do
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

  it "should have the block argument affect the _dataset method" do
    @c2.many_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => 456)
    end
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (xxx = 456)'
  end
  
  it "should support a :dataset option that is used instead of the default" do
    c1 = @c1
    @c2.many_to_many :attributes, :class => @c1, :dataset=>proc{c1.join_table(:natural, :an).filter(:an__nodeid=>pk)}, :order=> :a, :limit=>10, :select=>nil do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    n.attributes_dataset.sql.should == 'SELECT * FROM attributes NATURAL JOIN an WHERE ((an.nodeid = 1234) AND (xxx = 555)) ORDER BY a LIMIT 10'
    a = n.attributes
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(@c1)
    MODEL_DB.sqls.first.should == 'SELECT * FROM attributes NATURAL JOIN an WHERE ((an.nodeid = 1234) AND (xxx = 555)) ORDER BY a LIMIT 10'
  end

  it "should support a :limit option" do
    @c2.many_to_many :attributes, :class => @c1 , :limit=>10
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) LIMIT 10'
    @c2.many_to_many :attributes, :class => @c1 , :limit=>[10, 10]
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) LIMIT 10 OFFSET 10'
  end

  it "should have the :eager option affect the _dataset method" do
    @c2.many_to_many :attributes, :class => @c2 , :eager=>:attributes
    @c2.new(:id => 1234).attributes_dataset.opts[:eager].should == {:attributes=>nil}
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

  it "should have the add_ method respect the :left_primary_key and :right_primary_key options" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx, :right_primary_key=>:yyy
    
    n = @c2.new(:id => 1234, :xxx=>5)
    a = @c1.new(:id => 2345, :yyy=>8)
    a.should == n.add_attribute(a)
    ['INSERT INTO attributes_nodes (node_id, attribute_id) VALUES (5, 8)',
     'INSERT INTO attributes_nodes (attribute_id, node_id) VALUES (8, 5)'
    ].should(include(MODEL_DB.sqls.first))
  end

  it "should have the remove_ method respect the :left_primary_key and :right_primary_key options" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx, :right_primary_key=>:yyy
    
    n = @c2.new(:id => 1234, :xxx=>5)
    a = @c1.new(:id => 2345, :yyy=>8)
    a.should == n.remove_attribute(a)
    MODEL_DB.sqls.first.should == 'DELETE FROM attributes_nodes WHERE ((node_id = 5) AND (attribute_id = 8))'
  end

  it "should raise an error if the model object doesn't have a valid primary key" do
    @c2.many_to_many :attributes, :class => @c1 
    a = @c2.new
    n = @c1.load(:id=>123)
    proc{a.attributes_dataset}.should raise_error(Sequel::Error)
    proc{a.attributes}.should raise_error(Sequel::Error)
    proc{a.add_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_all_attributes}.should raise_error(Sequel::Error)
  end

  it "should raise an error if trying to add/remove a model object that doesn't have a valid primary key" do
    @c2.many_to_many :attributes, :class => @c1 
    n = @c1.new
    a = @c2.load(:id=>123)
    proc{a.add_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_attribute(n)}.should raise_error(Sequel::Error)
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
    n.associations.include?(:attributes).should == false
    atts = n.attributes
    atts.should == n.associations[:attributes]
    MODEL_DB.sqls.length.should == 1
  end

  it "should use cached instance variable if available" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.associations[:attributes] = 42
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    n.associations[:attributes] = 42
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.length.should == 1
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = []
    n.associations[:attributes] = a
    n.add_attribute(att)
    a.should == [att]
  end

  it "should add item to reciprocal cached instance variable if it exists when calling add_" do
    @c2.many_to_many :attributes, :class => @c1
    @c1.many_to_many :nodes, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    att.associations[:nodes] = []
    n.add_attribute(att)
    att.nodes.should == [n]
  end

  it "should remove item from cached instance variable if it exists when calling remove_" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    MODEL_DB.reset
    a = [att]
    n.associations[:attributes] = a
    n.remove_attribute(att)
    a.should == []
  end

  it "should remove item from reciprocal cached instance variable if it exists when calling remove_" do
    @c2.many_to_many :attributes, :class => @c1
    @c1.many_to_many :nodes, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.new(:id => 345)
    att.associations[:nodes] = [n]
    n.remove_attribute(att)
    att.nodes.should == []
  end

  it "should not create the add_, remove_, or remove_all_ methods if :read_only option is used" do
    @c2.many_to_many :attributes, :class => @c1, :read_only=>true
    im = @c2.instance_methods.collect{|x| x.to_s}
    im.should(include('attributes'))
    im.should(include('attributes_dataset'))
    im.should_not(include('add_attribute'))
    im.should_not(include('remove_attribute'))
    im.should_not(include('remove_all_attributes'))
  end

  it "should not add associations methods directly to class" do
    @c2.many_to_many :attributes, :class => @c1
    im = @c2.instance_methods.collect{|x| x.to_s}
    im.should(include('attributes'))
    im.should(include('attributes_dataset'))
    im.should(include('add_attribute'))
    im.should(include('remove_attribute'))
    im.should(include('remove_all_attributes'))
    im2 = @c2.instance_methods(false).collect{|x| x.to_s}
    im2.should_not(include('attributes'))
    im2.should_not(include('attributes_dataset'))
    im2.should_not(include('add_attribute'))
    im2.should_not(include('remove_attribute'))
    im2.should_not(include('remove_all_attributes'))
  end

  it "should have has_and_belongs_to_many alias" do
    @c2.has_and_belongs_to_many :attributes, :class => @c1 

    n = @c2.new(:id => 1234)
    a = n.attributes_dataset
    a.should be_a_kind_of(Sequel::Dataset)
    a.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
  it "should have an remove_all_ method that removes all associations" do
    @c2.many_to_many :attributes, :class => @c1
    @c2.new(:id => 1234).remove_all_attributes
    MODEL_DB.sqls.first.should == 'DELETE FROM attributes_nodes WHERE (node_id = 1234)'
  end

  it "should have the remove_all_ method respect the :left_primary_key option" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx
    @c2.new(:id => 1234, :xxx=>5).remove_all_attributes
    MODEL_DB.sqls.first.should == 'DELETE FROM attributes_nodes WHERE (node_id = 5)'
  end

  it "remove_all should set the cached instance variable to []" do
    @c2.many_to_many :attributes, :class => @c1
    node = @c2.new(:id => 1234)
    node.remove_all_attributes
    node.associations[:attributes].should == []
  end

  it "remove_all should return the array of previously associated items if the cached instance variable exists" do
    @c2.many_to_many :attributes, :class => @c1
    attrib = @c1.new(:id=>3)
    node = @c2.new(:id => 1234)
    d = @c1.dataset
    def d.fetch_rows(s); end
    node.attributes.should == []
    node.add_attribute(attrib)
    node.associations[:attributes].should == [attrib]
    node.remove_all_attributes.should == [attrib]
  end

  it "remove_all should return nil if the cached instance variable does not exist" do
    @c2.many_to_many :attributes, :class => @c1
    @c2.new(:id => 1234).remove_all_attributes.should == nil
  end

  it "remove_all should remove the current item from all reciprocal instance varaibles if it cached instance variable exists" do
    @c2.many_to_many :attributes, :class => @c1
    @c1.many_to_many :nodes, :class => @c2
    d = @c1.dataset
    def d.fetch_rows(s); end
    d = @c2.dataset
    def d.fetch_rows(s); end
    attrib = @c1.new(:id=>3)
    node = @c2.new(:id => 1234)
    node.attributes.should == []
    attrib.nodes.should == []
    node.add_attribute(attrib)
    attrib.associations[:nodes].should == [node]
    node.remove_all_attributes
    attrib.associations[:nodes].should == []
  end

  it "should call an _add_ method internally to add attributes" do
    @c2.many_to_many :attributes, :class => @c1
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_add_attribute"))
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._add_attribute(x)
      @x = x
    end
    p.add_attribute(c)
    p.instance_variable_get(:@x).should == c
    MODEL_DB.sqls.should == []
  end

  it "should call a _remove_ method internally to remove attributes" do
    @c2.many_to_many :attributes, :class => @c1
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_remove_attribute"))
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._remove_attribute(x)
      @x = x
    end
    p.remove_attribute(c)
    p.instance_variable_get(:@x).should == c
    MODEL_DB.sqls.should == []
  end

  it "should support (before|after)_(add|remove) callbacks" do
    h = []
    @c2.many_to_many :attributes, :class => @c1, :before_add=>[proc{|x,y| h << x.pk; h << -y.pk}, :blah], :after_add=>proc{h << 3}, :before_remove=>:blah, :after_remove=>[:blahr]
    @c2.class_eval do
      @@blah = h
      def _add_attribute(v)
        @@blah << 4
      end
      def _remove_attribute(v)
        @@blah << 5
      end
      def blah(x)
        @@blah << x.pk
      end
      def blahr(x)
        @@blah << 6
      end
    end
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    h.should == []
    p.add_attribute(c)
    h.should == [10, -123, 123, 4, 3]
    p.remove_attribute(c)
    h.should == [10, -123, 123, 4, 3, 123, 5, 6]
  end

  it "should support after_load association callback" do
    h = []
    @c2.many_to_many :attributes, :class => @c1, :after_load=>[proc{|x,y| h << [x.pk, y.collect{|z|z.pk}]}, :al]
    @c2.class_eval do
      @@blah = h
      def al(v)
        v.each{|x| @@blah << x.pk}
      end
    end
    @c1.class_eval do
      def @dataset.fetch_rows(sql)
        yield({:id=>20})
        yield({:id=>30})
      end
    end
    p = @c2.load(:id=>10, :parent_id=>20)
    attributes = p.attributes
    h.should == [[10, [20, 30]], 20, 30]
    attributes.collect{|a| a.pk}.should == [20, 30]
  end

  it "should raise error and not call internal add or remove method if before callback returns false if raise_on_save_failure is true" do
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    @c2.many_to_many :attributes, :class => @c1, :before_add=>:ba, :before_remove=>:br
    p.should_receive(:ba).once.with(c).and_return(false)
    p.should_not_receive(:_add_attribute)
    p.should_not_receive(:_remove_attribute)
    p.associations[:attributes] = []
    p.raise_on_save_failure = true
    proc{p.add_attribute(c)}.should raise_error(Sequel::Error)
    p.attributes.should == []
    p.associations[:attributes] = [c]
    p.should_receive(:br).once.with(c).and_return(false)
    proc{p.remove_attribute(c)}.should raise_error(Sequel::Error)
    p.attributes.should == [c]
  end

  it "should return nil and not call internal add or remove method if before callback returns false if raise_on_save_failure is false" do
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    p.raise_on_save_failure = false
    @c2.many_to_many :attributes, :class => @c1, :before_add=>:ba, :before_remove=>:br
    p.should_receive(:ba).once.with(c).and_return(false)
    p.should_not_receive(:_add_attribute)
    p.should_not_receive(:_remove_attribute)
    p.associations[:attributes] = []
    p.add_attribute(c).should == nil
    p.attributes.should == []
    p.associations[:attributes] = [c]
    p.should_receive(:br).once.with(c).and_return(false)
    p.remove_attribute(c).should == nil
    p.attributes.should == [c]
  end

  it "should support a :uniq option that removes duplicates from the association" do
    h = []
    @c2.many_to_many :attributes, :class => @c1, :uniq=>true
    @c1.class_eval do
      def @dataset.fetch_rows(sql)
        yield({:id=>20})
        yield({:id=>30})
        yield({:id=>20})
        yield({:id=>30})
      end
    end
    @c2.load(:id=>10, :parent_id=>20).attributes.should == [@c1.load(:id=>20), @c1.load(:id=>30)]
  end
end

describe Sequel::Model, " association reflection methods" do
  before do
    MODEL_DB.reset
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
    end
  end
  
  it "#all_association_reflections should include all association reflection hashes" do
    @c1.all_association_reflections.should == []

    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.all_association_reflections.collect{|v| v[:name]}.should == [:parent]
    @c1.all_association_reflections.collect{|v| v[:type]}.should == [:many_to_one]
    @c1.all_association_reflections.collect{|v| v[:class]}.should == [@c1]

    @c1.associate :one_to_many, :children, :class => @c1
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:name]}.should == [:children, :parent]
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:type]}.should == [:one_to_many, :many_to_one]
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:class]}.should == [@c1, @c1]
  end

  it "#association_reflection should return nil for nonexistent association" do
    @c1.association_reflection(:blah).should == nil
  end

  it "#association_reflection should return association reflection hash if association exists" do
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.association_reflection(:parent).should be_a_kind_of(Sequel::Model::Associations::AssociationReflection)
    @c1.association_reflection(:parent)[:name].should == :parent
    @c1.association_reflection(:parent)[:type].should == :many_to_one
    @c1.association_reflection(:parent)[:class].should == @c1

    @c1.associate :one_to_many, :children, :class => @c1
    @c1.association_reflection(:children).should be_a_kind_of(Sequel::Model::Associations::AssociationReflection)
    @c1.association_reflection(:children)[:name].should == :children
    @c1.association_reflection(:children)[:type].should == :one_to_many
    @c1.association_reflection(:children)[:class].should == @c1
  end

  it "#associations should include all association names" do
    @c1.associations.should == []
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.associations.should == [:parent]
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.associations.sort_by{|x|x.to_s}.should == [:children, :parent]
  end

  it "association reflections should be copied upon subclasing" do
    @c1.associate :many_to_one, :parent, :class => @c1
    c = Class.new(@c1)
    @c1.associations.should == [:parent]
    c.associations.should == [:parent]
    c.associate :many_to_one, :parent2, :class => @c1
    @c1.associations.should == [:parent]
    c.associations.sort_by{|x| x.to_s}.should == [:parent, :parent2]
    c.instance_methods.map{|x| x.to_s}.should include('parent')
  end
end
