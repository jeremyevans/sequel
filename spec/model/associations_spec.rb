require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "associate" do
  it "should use explicit class if given a class, symbol, or string" do
    begin
      klass = Class.new(Sequel::Model(:nodes))
      class ::ParParent < Sequel::Model; end
      
      klass.associate :many_to_one, :par_parent0, :class=>ParParent
      klass.associate :one_to_many, :par_parent1s, :class=>'ParParent'
      klass.associate :many_to_many, :par_parent2s, :class=>:ParParent
      
      klass.association_reflection(:"par_parent0").associated_class.should == ParParent
      klass.association_reflection(:"par_parent1s").associated_class.should == ParParent
      klass.association_reflection(:"par_parent2s").associated_class.should == ParParent
    ensure
      Object.send(:remove_const, :ParParent)
    end
  end

  it "should default to associating to other models in the same scope" do
    begin
      class ::AssociationModuleTest
        class Album < Sequel::Model
          many_to_one :artist
          many_to_many :tags
        end
        class Artist< Sequel::Model
          one_to_many :albums
        end
        class Tag < Sequel::Model
          many_to_many :albums
        end
      end
      
      ::AssociationModuleTest::Album.association_reflection(:artist).associated_class.should == ::AssociationModuleTest::Artist
      ::AssociationModuleTest::Album.association_reflection(:tags).associated_class.should == ::AssociationModuleTest::Tag
      ::AssociationModuleTest::Artist.association_reflection(:albums).associated_class.should == ::AssociationModuleTest::Album
      ::AssociationModuleTest::Tag.association_reflection(:albums).associated_class.should == ::AssociationModuleTest::Album
    ensure
      Object.send(:remove_const, :AssociationModuleTest)
    end
  end

  it "should add a model_object and association_reflection accessors to the dataset, and return it with the current model object" do
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
    begin
      class ::ParParent < Sequel::Model; end
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
    ensure
      Object.send(:remove_const, :ParParent)
    end
  end

  it "should raise an error if attempting to clone an association of differing type" do
    c = Class.new(Sequel::Model(:c))
    c.many_to_one :c
    proc{c.one_to_many :cs, :clone=>:c}.should raise_error(Sequel::Error)
  end

  it "should allow cloning of one_to_many to one_to_one associations and vice-versa" do
    c = Class.new(Sequel::Model(:c))
    c.one_to_one :c
    proc{c.one_to_many :cs, :clone=>:c}.should_not raise_error(Sequel::Error)
    proc{c.one_to_one :c2, :clone=>:cs}.should_not raise_error(Sequel::Error)
  end

  it "should clear associations cache when using set_values" do
    c = Class.new(Sequel::Model(:c))
    c.many_to_one :c
    o = c.new
    o.associations[:c] = 1
    o.set_values(:id=>1)
    o.associations.should == {}
  end

  it "should clear associations cache when refreshing object manually" do
    c = Class.new(Sequel::Model(:c))
    c.many_to_one :c
    o = c.new
    o.associations[:c] = 1
    o.refresh
    o.associations.should == {}
  end

  it "should clear associations cache when refreshing object after save" do
    c = Class.new(Sequel::Model(:c))
    c.many_to_one :c
    o = c.new
    o.associations[:c] = 1
    o.save
    o.associations.should == {}
  end

  it "should clear associations cache when saving with insert_select" do
    ds = Sequel::Model.db[:c]
    def ds.supports_insert_select?() true end
    def ds.insert_select(*) {:id=>1} end
    c = Class.new(Sequel::Model(ds))
    c.many_to_one :c
    o = c.new
    o.associations[:c] = 1
    o.save
    o.associations.should == {}
  end

end

describe Sequel::Model, "many_to_one" do
  before do
    @c2 = Class.new(Sequel::Model(:nodes)) do
      unrestrict_primary_key
      columns :id, :parent_id, :par_parent_id, :blah
    end
    @dataset = @c2.dataset
    MODEL_DB.reset
  end

  it "should use implicit key if omitted" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.new(:id => 1, :parent_id => 234)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 234) LIMIT 1"]
  end
  
  it "should allow association with the same name as the key if :key_alias is given" do
    @c2.def_column_alias(:parent_id_id, :parent_id)
    @c2.many_to_one :parent_id, :key_column=>:parent_id, :class => @c2
    d = @c2.load(:id => 1, :parent_id => 234)
    d.parent_id_dataset.sql.should == "SELECT * FROM nodes WHERE (nodes.id = 234) LIMIT 1"
    d.parent_id.should == @c2.load(:x => 1, :id => 1)
    d.parent_id_id.should == 234
    d[:parent_id].should == 234
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 234) LIMIT 1"]

    d.parent_id_id = 3
    d.parent_id_id.should == 3
    d[:parent_id].should == 3
  end
  
  it "should use implicit class if omitted" do
    begin
      class ::ParParent < Sequel::Model; end
      @c2.many_to_one :par_parent
      @c2.new(:id => 1, :par_parent_id => 234).par_parent.class.should == ParParent
      MODEL_DB.sqls.should == ["SELECT * FROM par_parents WHERE (par_parents.id = 234) LIMIT 1"]
    ensure
      Object.send(:remove_const, :ParParent)
    end
  end

  it "should use class inside module if given as a string" do
    begin
      module ::Par 
        class Parent < Sequel::Model; end
      end
      @c2.many_to_one :par_parent, :class=>"Par::Parent"
      @c2.new(:id => 1, :par_parent_id => 234).par_parent.class.should == Par::Parent
      MODEL_DB.sqls.should == ["SELECT * FROM parents WHERE (parents.id = 234) LIMIT 1"]
    ensure
      Object.send(:remove_const, :Par)
    end
  end

  it "should use explicit key if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah

    d = @c2.new(:id => 1, :blah => 567)
    p = d.parent
    p.class.should == @c2
    p.values.should == {:x => 1, :id => 1}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 567) LIMIT 1"]
  end

  it "should respect :qualify => false option" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :qualify=>false
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (id = 567) LIMIT 1"]
  end
  
  it "should use :primary_key option if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :primary_key => :pk
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.pk = 567) LIMIT 1"]
  end
  
  it "should support composite keys" do
    @c2.many_to_one :parent, :class => @c2, :key=>[:id, :parent_id], :primary_key=>[:parent_id, :id]
    @c2.new(:id => 1, :parent_id => 234).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((nodes.parent_id = 1) AND (nodes.id = 234)) LIMIT 1"]
  end
  
  it "should not issue query if not all keys have values" do
    @c2.many_to_one :parent, :class => @c2, :key=>[:id, :parent_id], :primary_key=>[:parent_id, :id]
    @c2.new(:id => 1, :parent_id => nil).parent.should == nil
    MODEL_DB.sqls.should == []
  end
  
  it "should raise an Error unless same number of composite keys used" do
    proc{@c2.many_to_one :parent, :class => @c2, :primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.many_to_one :parent, :class => @c2, :key=>[:id, :parent_id], :primary_key=>:id}.should raise_error(Sequel::Error)
    proc{@c2.many_to_one :parent, :class => @c2, :key=>:id, :primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.many_to_one :parent, :class => @c2, :key=>[:id, :parent_id, :blah], :primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
  end

  it "should use :select option if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :select=>[:id, :name]
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT id, name FROM nodes WHERE (nodes.id = 567) LIMIT 1"]
  end

  it "should use :conditions option if given" do
    @c2.many_to_one :parent, :class => @c2, :key => :blah, :conditions=>{:a=>32}
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((a = 32) AND (nodes.id = 567)) LIMIT 1"]

    @c2.many_to_one :parent, :class => @c2, :key => :blah, :conditions=>:a
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (a AND (nodes.id = 567)) LIMIT 1"]
  end

  it "should support :order, :limit (only for offset), and :dataset options, as well as a block" do
    @c2.many_to_one :child_20, :class => @c2, :key=>:id, :dataset=>proc{model.filter(:parent_id=>pk)}, :limit=>[10,20], :order=>:name do |ds|
      ds.filter{x > 1}
    end
    @c2.load(:id => 100).child_20
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((parent_id = 100) AND (x > 1)) ORDER BY name LIMIT 1 OFFSET 20"]
  end

  it "should return nil if key value is nil" do
    @c2.many_to_one :parent, :class => @c2
    @c2.new(:id => 1).parent.should == nil
    MODEL_DB.sqls.should == []
  end

  it "should cache negative lookup" do
    @c2.many_to_one :parent, :class => @c2
    @c2.dataset._fetch = []
    d = @c2.new(:id => 1, :parent_id=>555)
    MODEL_DB.sqls.should == []
    d.parent.should == nil
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.id = 555) LIMIT 1']
    d.parent.should == nil
    MODEL_DB.sqls.should == []
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
  
  it "should have the setter method respect composite keys" do
    @c2.many_to_one :parent, :class => @c2, :key=>[:id, :parent_id], :primary_key=>[:parent_id, :id]

    d = @c2.new(:id => 1, :parent_id=> 234)
    d.parent = @c2.new(:id => 4, :parent_id=>52)
    d.values.should == {:id => 52, :parent_id => 4}

    d.parent = nil
    d.values.should == {:id => nil, :parent_id => nil}

    e = @c2.new(:id => 6677, :parent_id=>8)
    d.parent = e
    d.values.should == {:id => 8, :parent_id => 6677}
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
    d.parent_id = 234
    d.associations[:parent].should == nil
    @c2.dataset._fetch = {:id=>234}
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
  
  it "should use a callback if given one as the argument" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.associations[:parent] = 42
    d.parent(proc{|ds| ds.filter{name > 'M'}}).should_not == 42 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((nodes.id = 234) AND (name > 'M')) LIMIT 1"]
  end
  
  it "should use a block given to the association method as a callback" do
    @c2.many_to_one :parent, :class => @c2

    d = @c2.create(:id => 1)
    MODEL_DB.reset
    d.parent_id = 234
    d.associations[:parent] = 42
    d.parent{|ds| ds.filter{name > 'M'}}.should_not == 42 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((nodes.id = 234) AND (name > 'M')) LIMIT 1"]
  end
  
  it "should have the setter add to the reciprocal one_to_many cached association list if it exists" do
    @c2.many_to_one :parent, :class => @c2
    @c2.one_to_many :children, :class => @c2, :key=>:parent_id
    @c2.dataset._fetch = []

    d = @c2.new(:id => 1)
    e = @c2.new(:id => 2)
    MODEL_DB.sqls.should == []
    d.parent = e
    e.children.should_not(include(d))
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.parent_id = 2)']

    d = @c2.new(:id => 1)
    e = @c2.new(:id => 2)
    e.children.should_not(include(d))
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.parent_id = 2)']
    d.parent = e
    e.children.should(include(d))
    MODEL_DB.sqls.should == []
  end

  it "should have many_to_one setter deal with a one_to_one reciprocal" do
    @c2.many_to_one :parent, :class => @c2, :key=>:parent_id
    @c2.one_to_one :child, :class => @c2, :key=>:parent_id

    d = @c2.new(:id => 1)
    e = @c2.new(:id => 2)
    e.associations[:child] = nil
    d.parent = e
    e.child.should == d
    d.parent = nil
    e.child.should == nil
    d.parent = e
    e.child.should == d

    f = @c2.new(:id => 3)
    d.parent = nil
    e.child.should == nil
    e.associations[:child] = f
    d.parent = e
    e.child.should == d
  end

  it "should have the setter remove the object from the previous associated object's reciprocal one_to_many cached association list if it exists" do
    @c2.many_to_one :parent, :class => @c2
    @c2.one_to_many :children, :class => @c2, :key=>:parent_id
    @c2.dataset._fetch = []

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

  it "should have the setter not modify the reciprocal if set to same value as current" do
    @c2.many_to_one :parent, :class => @c2
    @c2.one_to_many :children, :class => @c2, :key=>:parent_id

    c1 = @c2.load(:id => 1, :parent_id=>nil)
    c2 = @c2.load(:id => 2, :parent_id=>1)
    c3 = @c2.load(:id => 3, :parent_id=>1)
    c1.associations[:children] = [c2, c3]
    c2.associations[:parent] = c1
    c2.parent = c1
    c1.children.should == [c2, c3]
    MODEL_DB.sqls.should == []
  end

  it "should get all matching records and only return the first if :key option is set to nil" do
    @c2.one_to_many :children, :class => @c2, :key=>:parent_id
    @c2.many_to_one :first_grand_parent, :class => @c2, :key=>nil, :eager_graph=>:children, :dataset=>proc{model.filter(:children_id=>parent_id)}
    @c2.dataset.columns(:id, :parent_id, :par_parent_id, :blah)._fetch = [{:id=>1, :parent_id=>0, :par_parent_id=>3, :blah=>4, :children_id=>2, :children_parent_id=>1, :children_par_parent_id=>5, :children_blah=>6}, {}]
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

  it "should add associations methods to the :methods_module option" do
    m = Module.new
    @c2.many_to_one :parent, :class => @c2, :methods_module=>m
    m.instance_methods.collect{|x| x.to_s}.should(include('parent'))
    m.instance_methods.collect{|x| x.to_s}.should(include('parent='))
    @c2.instance_methods.collect{|x| x.to_s}.should_not(include('parent'))
    @c2.instance_methods.collect{|x| x.to_s}.should_not(include('parent='))
  end

  it "should add associations methods directly to class if :methods_module is the class itself" do
    @c2.many_to_one :parent, :class => @c2, :methods_module=>@c2
    @c2.instance_methods(false).collect{|x| x.to_s}.should(include('parent'))
    @c2.instance_methods(false).collect{|x| x.to_s}.should(include('parent='))
  end

  it "should raise an error if trying to set a model object that doesn't have a valid primary key" do
    @c2.many_to_one :parent, :class => @c2
    p = @c2.new
    c = @c2.load(:id=>123)
    proc{c.parent = p}.should raise_error(Sequel::Error)
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

  it "should support (before|after)_set callbacks" do
    h = []
    @c2.many_to_one :parent, :class => @c2, :before_set=>[proc{|x,y| h << x.pk; h << (y ? -y.pk : :y)}, :blah], :after_set=>proc{h << 3}
    @c2.class_eval do
      self::Foo = h
      def []=(a, v)
        a == :parent_id ? (model::Foo << (v ? 4 : 5)) : super
      end
      def blah(x)
        model::Foo << (x ? x.pk : :x)
      end
      def blahr(x)
        model::Foo << 6
      end
    end
    p = @c2.load(:id=>10)
    c = @c2.load(:id=>123)
    h.should == []
    p.parent = c
    h.should == [10, -123, 123, 4, 3]
    p.parent = nil
    h.should == [10, -123, 123, 4, 3, 10, :y, :x, 5, 3]
  end

  it "should support after_load association callback" do
    h = []
    @c2.many_to_one :parent, :class => @c2, :after_load=>[proc{|x,y| h << [x.pk, y.pk]}, :al]
    @c2.class_eval do
      self::Foo = h
      def al(v)
        model::Foo << v.pk
      end
      dataset._fetch = {:id=>20}
    end
    p = @c2.load(:id=>10, :parent_id=>20)
    parent = p.parent
    h.should == [[10, 20], 20]
    parent.pk.should == 20
  end

  it "should support after_load association callback that changes the cached object" do
    @c2.many_to_one :parent, :class => @c2, :after_load=>:al
    @c2.class_eval do
      def al(v)
        associations[:parent] = :foo
      end
    end
    p = @c2.load(:id=>10, :parent_id=>20)
    p.parent.should == :foo
    p.associations[:parent].should == :foo
  end

  it "should raise error and not call internal add or remove method if before callback returns false, even if raise_on_save_failure is false" do
    # The reason for this is that assignment in ruby always returns the argument instead of the result
    # of the method, so we can't return nil to signal that the association callback prevented the modification
    p = @c2.new
    c = @c2.load(:id=>123)
    p.raise_on_save_failure = false
    @c2.many_to_one :parent, :class => @c2, :before_set=>:bs
    def p.bs(x) false end
    p.should_not_receive(:_parent=)
    proc{p.parent = c}.should raise_error(Sequel::Error)
    
    p.parent.should == nil
    p.associations[:parent] = c
    p.parent.should == c
    proc{p.parent = nil}.should raise_error(Sequel::Error)
  end

  it "should raise an error if a callback is not a proc or symbol" do
    @c2.many_to_one :parent, :class => @c2, :before_set=>Object.new
    proc{@c2.new.parent = @c2.load(:id=>1)}.should raise_error(Sequel::Error)
  end

  it "should call the remove callbacks for the previous object and the add callbacks for the new object" do
    c = @c2.load(:id=>123)
    d = @c2.load(:id=>321)
    p = @c2.new
    p.associations[:parent] = d
    @c2.many_to_one :parent, :class => @c2, :before_set=>:bs, :after_set=>:as
    @c2.class_eval do
      self::Foo = []
      def []=(a, v)
        a == :parent_id ? (model::Foo << 5) : super
      end
      def bs(x)
        model::Foo << x.pk
      end
      def as(x)
        model::Foo << x.pk * 2
      end
    end
    p.parent = c
    @c2::Foo.should == [123, 5, 246]
  end
end

describe Sequel::Model, "one_to_one" do
  before do
    @c1 = Class.new(Sequel::Model(:attributes)) do
      unrestrict_primary_key
      columns :id, :node_id, :y
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      unrestrict_primary_key
      attr_accessor :xxx
      
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
      columns :id, :x, :parent_id, :par_parent_id, :blah, :node_id
    end
    @dataset = @c2.dataset
    @dataset._fetch = {}
    @c1.dataset._fetch = {}
    MODEL_DB.reset
  end
  
  it "should have the getter method return a single object if the :one_to_one option is true" do
    @c2.one_to_one :attribute, :class => @c1
    att = @c2.new(:id => 1234).attribute
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234) LIMIT 1']
    att.should be_a_kind_of(@c1)
    att.values.should == {}
  end

  it "should not add a setter method if the :read_only option is true" do
    @c2.one_to_one :attribute, :class => @c1, :read_only=>true
    im = @c2.instance_methods.collect{|x| x.to_s}
    im.should(include('attribute'))
    im.should_not(include('attribute='))
  end

  it "should add a setter method" do
    @c2.one_to_one :attribute, :class => @c1
    attrib = @c1.new(:id=>3)
    @c1.dataset._fetch = @c1.instance_dataset._fetch = {:id=>3}
    @c2.new(:id => 1234).attribute = attrib
    sqls = MODEL_DB.sqls
    ['INSERT INTO attributes (node_id, id) VALUES (1234, 3)',
      'INSERT INTO attributes (id, node_id) VALUES (3, 1234)'].should(include(sqls.slice! 1))
    sqls.should == ['UPDATE attributes SET node_id = NULL WHERE (node_id = 1234)', "SELECT * FROM attributes WHERE (id = 3) LIMIT 1"]

    @c2.new(:id => 1234).attribute.should == attrib
    attrib = @c1.load(:id=>3)
    @c2.new(:id => 1234).attribute = attrib
    MODEL_DB.sqls.should == ["SELECT * FROM attributes WHERE (attributes.node_id = 1234) LIMIT 1",
      'UPDATE attributes SET node_id = NULL WHERE ((node_id = 1234) AND (id != 3))',
      "UPDATE attributes SET node_id = 1234 WHERE (id = 3)"]
  end

  it "should use a transaction in the setter method" do
    @c2.one_to_one :attribute, :class => @c1
    @c2.use_transactions = true
    attrib = @c1.load(:id=>3)
    @c2.new(:id => 1234).attribute = attrib
    MODEL_DB.sqls.should == ['BEGIN',
      'UPDATE attributes SET node_id = NULL WHERE ((node_id = 1234) AND (id != 3))',
      "UPDATE attributes SET node_id = 1234 WHERE (id = 3)",
      'COMMIT']
  end
    
  it "should have setter method respect association filters" do
    @c2.one_to_one :attribute, :class => @c1, :conditions=>{:a=>1} do |ds|
      ds.filter(:b=>2)
    end
    attrib = @c1.load(:id=>3)
    @c2.new(:id => 1234).attribute = attrib
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = NULL WHERE ((a = 1) AND (node_id = 1234) AND (b = 2) AND (id != 3))',
      "UPDATE attributes SET node_id = 1234 WHERE (id = 3)"]
  end

  it "should have the setter method respect the :primary_key option" do
    @c2.one_to_one :attribute, :class => @c1, :primary_key=>:xxx
    attrib = @c1.new(:id=>3)
    @c1.dataset._fetch = @c1.instance_dataset._fetch = {:id=>3}
    @c2.new(:id => 1234, :xxx=>5).attribute = attrib
    sqls = MODEL_DB.sqls
    ['INSERT INTO attributes (node_id, id) VALUES (5, 3)',
      'INSERT INTO attributes (id, node_id) VALUES (3, 5)'].should(include(sqls.slice! 1))
    sqls.should == ['UPDATE attributes SET node_id = NULL WHERE (node_id = 5)', "SELECT * FROM attributes WHERE (id = 3) LIMIT 1"]

    @c2.new(:id => 321, :xxx=>5).attribute.should == attrib
    attrib = @c1.load(:id=>3)
    @c2.new(:id => 621, :xxx=>5).attribute = attrib
    MODEL_DB.sqls.should == ["SELECT * FROM attributes WHERE (attributes.node_id = 5) LIMIT 1",
      'UPDATE attributes SET node_id = NULL WHERE ((node_id = 5) AND (id != 3))',
      'UPDATE attributes SET node_id = 5 WHERE (id = 3)']
    end
    
  it "should have the setter method respect composite keys" do
    @c2.one_to_one :attribute, :class => @c1, :key=>[:node_id, :y], :primary_key=>[:id, :x]
    attrib = @c1.load(:id=>3, :y=>6)
    @c1.dataset._fetch = {:id=>3, :y=>6}
    @c2.load(:id => 1234, :x=>5).attribute = attrib
    sqls = MODEL_DB.sqls
    sqls.last.should =~ /UPDATE attributes SET (node_id = 1234|y = 5), (node_id = 1234|y = 5) WHERE \(id = 3\)/
    sqls.first.should =~ /UPDATE attributes SET (node_id|y) = NULL, (node_id|y) = NULL WHERE \(\(node_id = 1234\) AND \(y = 5\) AND \(id != 3\)\)/
    sqls.length.should == 2
  end

  it "should use implicit key if omitted" do
    @c2.one_to_one :parent, :class => @c2

    d = @c2.new(:id => 234)
    p = d.parent
    p.class.should == @c2
    p.values.should == {}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.node_id = 234) LIMIT 1"]
  end
  
  it "should use implicit class if omitted" do
    begin
      class ::ParParent < Sequel::Model; end
      @c2.one_to_one :par_parent
      @c2.new(:id => 234).par_parent.class.should == ParParent
      MODEL_DB.sqls.should == ["SELECT * FROM par_parents WHERE (par_parents.node_id = 234) LIMIT 1"]
    ensure
      Object.send(:remove_const, :ParParent)
    end
  end

  it "should use class inside module if given as a string" do
    begin
      module ::Par 
        class Parent < Sequel::Model; end
      end
      @c2.one_to_one :par_parent, :class=>"Par::Parent"
      @c2.new(:id => 234).par_parent.class.should == Par::Parent
      MODEL_DB.sqls.should == ["SELECT * FROM parents WHERE (parents.node_id = 234) LIMIT 1"]
    ensure
      Object.send(:remove_const, :Par)
    end
  end

  it "should use explicit key if given" do
    @c2.one_to_one :parent, :class => @c2, :key => :blah

    d = @c2.new(:id => 234)
    p = d.parent
    p.class.should == @c2
    p.values.should == {}

    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.blah = 234) LIMIT 1"]
  end

  it "should use :primary_key option if given" do
    @c2.one_to_one :parent, :class => @c2, :key => :pk, :primary_key => :blah
    @c2.new(:id => 1, :blah => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.pk = 567) LIMIT 1"]
  end
  
  it "should support composite keys" do
    @c2.one_to_one :parent, :class => @c2, :primary_key=>[:id, :parent_id], :key=>[:parent_id, :id]
    @c2.new(:id => 1, :parent_id => 234).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((nodes.parent_id = 1) AND (nodes.id = 234)) LIMIT 1"]
  end
  
  it "should not issue query if not all keys have values" do
    @c2.one_to_one :parent, :class => @c2, :key=>[:id, :parent_id], :primary_key=>[:parent_id, :id]
    @c2.new(:id => 1, :parent_id => nil).parent.should == nil
    MODEL_DB.sqls.should == []
  end
  
  it "should raise an Error unless same number of composite keys used" do
    proc{@c2.one_to_one :parent, :class => @c2, :primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.one_to_one :parent, :class => @c2, :key=>[:id, :parent_id], :primary_key=>:id}.should raise_error(Sequel::Error)
    proc{@c2.one_to_one :parent, :class => @c2, :key=>:id, :primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.one_to_one :parent, :class => @c2, :key=>[:id, :parent_id, :blah], :primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
  end

  it "should use :select option if given" do
    @c2.one_to_one :parent, :class => @c2, :select=>[:id, :name]
    @c2.new(:id => 567).parent
    MODEL_DB.sqls.should == ["SELECT id, name FROM nodes WHERE (nodes.node_id = 567) LIMIT 1"]
  end

  it "should use :conditions option if given" do
    @c2.one_to_one :parent, :class => @c2, :conditions=>{:a=>32}
    @c2.new(:id => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((a = 32) AND (nodes.node_id = 567)) LIMIT 1"]

    @c2.one_to_one :parent, :class => @c2, :conditions=>:a
    @c2.new(:id => 567).parent
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (a AND (nodes.node_id = 567)) LIMIT 1"]
  end

  it "should support :order, :limit (only for offset), and :dataset options, as well as a block" do
    @c2.one_to_one :child_20, :class => @c2, :key=>:id, :dataset=>proc{model.filter(:parent_id=>pk)}, :limit=>[10,20], :order=>:name do |ds|
      ds.filter{x > 1}
    end
    @c2.load(:id => 100).child_20
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE ((parent_id = 100) AND (x > 1)) ORDER BY name LIMIT 1 OFFSET 20"]
  end

  it "should return nil if primary_key value is nil" do
    @c2.one_to_one :parent, :class => @c2, :primary_key=>:node_id

    @c2.new(:id => 1).parent.should be_nil
    MODEL_DB.sqls.should == []
  end

  it "should cache negative lookup" do
    @c2.one_to_one :parent, :class => @c2
    @c2.dataset._fetch = []
    d = @c2.new(:id => 555)
    MODEL_DB.sqls.should == []
    d.parent.should == nil
    MODEL_DB.sqls.should == ['SELECT * FROM nodes WHERE (nodes.node_id = 555) LIMIT 1']
    d.parent.should == nil
    MODEL_DB.sqls.should == []
  end

  it "should have the setter method respect the :key option" do
    @c2.one_to_one :parent, :class => @c2, :key=>:blah
    d = @c2.new(:id => 3)
    e = @c2.new(:id => 4321, :blah=>444)
    @c2.dataset._fetch = @c2.instance_dataset._fetch = {:id => 4321, :blah => 3}
    d.parent = e
    e.values.should == {:id => 4321, :blah => 3}
    sqls = MODEL_DB.sqls
    ["INSERT INTO nodes (blah, id) VALUES (3, 4321)",
     "INSERT INTO nodes (id, blah) VALUES (4321, 3)"].should include(sqls.slice! 1)
    sqls.should == ["UPDATE nodes SET blah = NULL WHERE (blah = 3)", "SELECT * FROM nodes WHERE (id = 4321) LIMIT 1"]
  end
  
  it "should persist changes to associated object when the setter is called" do
    @c2.one_to_one :parent, :class => @c2
    d = @c2.load(:id => 1)
    d.parent = @c2.load(:id => 3, :node_id=>345)
    MODEL_DB.sqls.should == ["UPDATE nodes SET node_id = NULL WHERE ((node_id = 1) AND (id != 3))",
      "UPDATE nodes SET node_id = 1 WHERE (id = 3)"] 
  end

  it "should set cached instance variable when accessed" do
    @c2.one_to_one :parent, :class => @c2

    d = @c2.load(:id => 1)
    d.associations[:parent].should == nil
    @c2.dataset._fetch = {:id=>234}
    e = d.parent 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.node_id = 1) LIMIT 1"]
    d.parent
    MODEL_DB.sqls.should == []
    d.associations[:parent].should == e
  end

  it "should set cached instance variable when assigned" do
    @c2.one_to_one :parent, :class => @c2

    d = @c2.load(:id => 1)
    d.associations[:parent].should == nil
    e = @c2.load(:id => 234)
    d.parent = e
    f = d.parent 
    d.associations[:parent].should == e
    e.should == f
  end

  it "should use cached instance variable if available" do
    @c2.one_to_one :parent, :class => @c2
    d = @c2.load(:id => 1, :parent_id => 234)
    d.associations[:parent] = 42
    d.parent.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.one_to_one :parent, :class => @c2
    d = @c2.load(:id => 1)
    d.associations[:parent] = [42]
    d.parent(true).should_not == 42 
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.node_id = 1) LIMIT 1"]
  end
  
  it "should have the setter set the reciprocal many_to_one cached association" do
    @c2.one_to_one :parent, :class => @c2, :key=>:parent_id
    @c2.many_to_one :child, :class => @c2, :key=>:parent_id
    
    d = @c2.load(:id => 1)
    e = @c2.load(:id => 2)
    d.parent = e
    e.child.should == d
    MODEL_DB.sqls.should == ["UPDATE nodes SET parent_id = NULL WHERE ((parent_id = 1) AND (id != 2))",
      "UPDATE nodes SET parent_id = 1 WHERE (id = 2)"]
    d.parent = nil
    e.child.should == nil
    MODEL_DB.sqls.should == ["UPDATE nodes SET parent_id = NULL WHERE (parent_id = 1)"]
  end

  it "should have the setter remove the object from the previous associated object's reciprocal many_to_one cached association list if it exists" do
    @c2.one_to_one :parent, :class => @c2, :key=>:parent_id
    @c2.many_to_one :child, :class => @c2, :key=>:parent_id
    @c2.dataset._fetch = []

    d = @c2.load(:id => 1)
    e = @c2.load(:id => 2)
    f = @c2.load(:id => 3)
    e.child.should == nil
    f.child.should == nil
    d.parent = e
    e.child.should == d
    d.parent = f
    f.child.should == d
    e.child.should == nil
    d.parent = nil
    f.child.should == nil
  end

  it "should have the setter not modify the reciprocal if set to same value as current" do
    @c2.one_to_one :parent, :class => @c2, :key=>:parent_id
    @c2.many_to_one :child, :class => @c2, :key=>:parent_id

    c1 = @c2.load(:id => 1, :parent_id=>nil)
    c2 = @c2.load(:id => 2, :parent_id=>1)
    c1.associations[:child] = c2
    c2.associations[:parent] = c1
    c2.parent = c1
    c1.child.should == c2
    MODEL_DB.sqls.should == []
  end

  it "should not add associations methods directly to class" do
    @c2.one_to_one :parent, :class => @c2
    @c2.instance_methods.collect{|x| x.to_s}.should(include('parent'))
    @c2.instance_methods.collect{|x| x.to_s}.should(include('parent='))
    @c2.instance_methods(false).collect{|x| x.to_s}.should_not(include('parent'))
    @c2.instance_methods(false).collect{|x| x.to_s}.should_not(include('parent='))
  end

  it "should raise an error if the current model object that doesn't have a valid primary key" do
    @c2.one_to_one :parent, :class => @c2
    p = @c2.new
    c = @c2.load(:id=>123)
    proc{p.parent = c}.should raise_error(Sequel::Error)
  end

  it "should make the change to the foreign_key value inside a _association= method" do
    @c2.one_to_one :parent, :class => @c2
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_parent="))
    c = @c2.new
    p = @c2.load(:id=>123)
    def p._parent=(x)
      @x = x
    end
    p.should_not_receive(:parent_id=)
    p.parent = c
    p.instance_variable_get(:@x).should == c
  end

  it "should support (before|after)_set callbacks" do
    h = []
    @c2.one_to_one :parent, :class => @c2, :before_set=>[proc{|x,y| h << x.pk; h << (y ? -y.pk : :y)}, :blah], :after_set=>proc{h << 3}
    @c2.class_eval do
      self::Foo = h
      def blah(x)
        model::Foo << (x ? x.pk : :x)
      end
      def blahr(x)
        model::Foo << 6
      end
    end
    p = @c2.load(:id=>10)
    c = @c2.load(:id=>123)
    h.should == []
    p.parent = c
    h.should == [10, -123, 123, 3]
    p.parent = nil
    h.should == [10, -123, 123, 3, 10, :y, :x, 3]
  end

  it "should support after_load association callback" do
    h = []
    @c2.one_to_one :parent, :class => @c2, :after_load=>[proc{|x,y| h << [x.pk, y.pk]}, :al]
    @c2.class_eval do
      self::Foo = h
      def al(v)
        model::Foo << v.pk
      end
      @dataset._fetch = {:id=>20}
    end
    p = @c2.load(:id=>10)
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
    @c2.one_to_one :parent, :class => @c2, :before_set=>:bs
    def p.bs(x) false end
    p.should_not_receive(:_parent=)
    proc{p.parent = c}.should raise_error(Sequel::Error)
    
    p.parent.should == nil
    p.associations[:parent] = c
    p.parent.should == c
    proc{p.parent = nil}.should raise_error(Sequel::Error)
  end

  it "should raise an error if a callback is not a proc or symbol" do
    @c2.one_to_one :parent, :class => @c2, :before_set=>Object.new
    proc{@c2.new.parent = @c2.load(:id=>1)}.should raise_error(Sequel::Error)
  end

  it "should call the set callbacks" do
    c = @c2.load(:id=>123)
    d = @c2.load(:id=>321)
    p = @c2.load(:id=>32)
    p.associations[:parent] = [d]
    h = []
    @c2.one_to_one :parent, :class => @c2, :before_set=>:bs, :after_set=>:as
    @c2.class_eval do
      self::Foo = h
      def []=(a, v)
        a == :node_id ? (model::Foo << 5) : super
      end
      def bs(x)
        model::Foo << x.pk
      end
      def as(x)
        model::Foo << x.pk * 2
      end
    end
    p.parent = c
    h.should == [123, 5, 246]
  end
  
  it "should work_correctly when used with associate" do
    @c2.associate :one_to_one, :parent, :class => @c2
    @c2.load(:id => 567).parent.should == @c2.load({})
    MODEL_DB.sqls.should == ["SELECT * FROM nodes WHERE (nodes.node_id = 567) LIMIT 1"]
  end
end

describe Sequel::Model, "one_to_many" do
  before do
    @c1 = Class.new(Sequel::Model(:attributes)) do
      unrestrict_primary_key
      columns :id, :node_id, :y, :z
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      def _refresh(ds); end
      unrestrict_primary_key
      attr_accessor :xxx
      
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
      columns :id, :x
    end
    @dataset = @c2.dataset
    @dataset._fetch = {}
    @c1.dataset._fetch = proc{|sql| sql =~ /SELECT 1/ ? {:a=>1} : {}}
    MODEL_DB.reset
  end

  it "should use implicit key if omitted" do
    @c2.one_to_many :attributes, :class => @c1 
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE (attributes.node_id = 1234)'
  end
  
  it "should use implicit class if omitted" do
    begin
      class ::HistoricalValue < Sequel::Model; end
      @c2.one_to_many :historical_values
      
      v = @c2.new(:id => 1234).historical_values_dataset
      v.should be_a_kind_of(Sequel::Dataset)
      v.sql.should == 'SELECT * FROM historical_values WHERE (historical_values.node_id = 1234)'
      v.model.should == HistoricalValue
    ensure
      Object.send(:remove_const, :HistoricalValue)
    end
  end
  
  it "should use class inside a module if given as a string" do
    begin
      module ::Historical
        class Value < Sequel::Model; end
      end
      @c2.one_to_many :historical_values, :class=>'Historical::Value'
      
      v = @c2.new(:id => 1234).historical_values_dataset
      v.should be_a_kind_of(Sequel::Dataset)
      v.sql.should == 'SELECT * FROM values WHERE (values.node_id = 1234)'
      v.model.should == Historical::Value
    ensure
      Object.send(:remove_const, :Historical)
    end
  end

  it "should use a callback if given one as the argument" do
    @c2.one_to_many :attributes, :class => @c1, :key => :nodeid
    
    d = @c2.load(:id => 1234)
    d.associations[:attributes] = []
    d.attributes(proc{|ds| ds.filter{name > 'M'}}).should_not == []
    MODEL_DB.sqls.should == ["SELECT * FROM attributes WHERE ((attributes.nodeid = 1234) AND (name > 'M'))"]
  end
  
  it "should use explicit key if given" do
    @c2.one_to_many :attributes, :class => @c1, :key => :nodeid
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE (attributes.nodeid = 1234)'
  end
  
  it "should support_composite keys" do
    @c2.one_to_many :attributes, :class => @c1, :key =>[:node_id, :id], :primary_key=>[:id, :x]
    @c2.load(:id => 1234, :x=>234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (attributes.id = 234))'
  end
  
  it "should not issue query if not all keys have values" do
    @c2.one_to_many :attributes, :class => @c1, :key =>[:node_id, :id], :primary_key=>[:id, :x]
    @c2.load(:id => 1234, :x=>nil).attributes.should == []
    MODEL_DB.sqls.should == []
  end
  
  it "should raise an Error unless same number of composite keys used" do
    proc{@c2.one_to_many :attributes, :class => @c1, :key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.one_to_many :attributes, :class => @c1, :primary_key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.one_to_many :attributes, :class => @c1, :key=>[:node_id, :id], :primary_key=>:id}.should raise_error(Sequel::Error)
    proc{@c2.one_to_many :attributes, :class => @c1, :key=>:id, :primary_key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.one_to_many :attributes, :class => @c1, :key=>[:node_id, :id, :x], :primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
  end

  it "should define an add_ method that works on existing records" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.load(:id => 1234)
    a = @c1.load(:id => 2345)
    a.should == n.add_attribute(a)
    a.values.should == {:node_id => 1234, :id => 2345}
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = 1234 WHERE (id = 2345)']
  end

  it "should define an add_ method that works on new records" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.load(:id => 1234)
    a = @c1.new(:id => 234)
    @c1.dataset._fetch = @c1.instance_dataset._fetch = {:node_id => 1234, :id => 234}
    a.should == n.add_attribute(a)
    sqls = MODEL_DB.sqls
    sqls.shift.should =~ /INSERT INTO attributes \((node_)?id, (node_)?id\) VALUES \(1?234, 1?234\)/
    sqls.should == ["SELECT * FROM attributes WHERE (id = 234) LIMIT 1"]
    a.values.should == {:node_id => 1234, :id => 234}
  end

  it "should define a remove_ method that works on existing records" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.load(:id => 1234)
    a = @c1.load(:id => 2345, :node_id => 1234)
    a.should == n.remove_attribute(a)
    a.values.should == {:node_id => nil, :id => 2345}
    MODEL_DB.sqls.should == ["SELECT 1 AS one FROM attributes WHERE ((attributes.node_id = 1234) AND (id = 2345)) LIMIT 1", 'UPDATE attributes SET node_id = NULL WHERE (id = 2345)']
  end

  it "should have the remove_ method raise an error if the passed object is not already associated" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    a = @c1.load(:id => 2345, :node_id => 1234)
    @c1.dataset._fetch = []
    proc{n.remove_attribute(a)}.should raise_error(Sequel::Error)
    MODEL_DB.sqls.should == ["SELECT 1 AS one FROM attributes WHERE ((attributes.node_id = 1234) AND (id = 2345)) LIMIT 1"]
  end

  it "should accept a hash for the add_ method and create a new record" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    MODEL_DB.reset
    @c1.dataset._fetch = @c1.instance_dataset._fetch = {:node_id => 1234, :id => 234}
    n.add_attribute(:id => 234).should == @c1.load(:node_id => 1234, :id => 234)
    sqls = MODEL_DB.sqls
    sqls.shift.should =~ /INSERT INTO attributes \((node_)?id, (node_)?id\) VALUES \(1?234, 1?234\)/
    sqls.should == ["SELECT * FROM attributes WHERE (id = 234) LIMIT 1"]
  end

  it "should accept a primary key for the add_ method" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    @c1.dataset._fetch = {:id=>234, :node_id=>nil}
    n.add_attribute(234).should == @c1.load(:node_id => 1234, :id => 234)
    MODEL_DB.sqls.should == ["SELECT * FROM attributes WHERE id = 234", "UPDATE attributes SET node_id = 1234 WHERE (id = 234)"]
  end

  it "should raise an error in the add_ method if the passed associated object is not of the correct type" do
    @c2.one_to_many :attributes, :class => @c1
    proc{@c2.new(:id => 1234).add_attribute(@c2.new)}.should raise_error(Sequel::Error)
  end

  it "should accept a primary key for the remove_ method and remove an existing record" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    @c1.dataset._fetch = {:id=>234, :node_id=>1234}
    n.remove_attribute(234).should == @c1.load(:node_id => nil, :id => 234)
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (attributes.id = 234)) LIMIT 1',
      'UPDATE attributes SET node_id = NULL WHERE (id = 234)']
  end
  
  it "should raise an error in the remove_ method if the passed associated object is not of the correct type" do
    @c2.one_to_many :attributes, :class => @c1
    proc{@c2.new(:id => 1234).remove_attribute(@c2.new)}.should raise_error(Sequel::Error)
  end

  it "should have add_ method respect the :primary_key option" do
    @c2.one_to_many :attributes, :class => @c1, :primary_key=>:xxx
    
    n = @c2.new(:id => 1234, :xxx=>5)
    a = @c1.load(:id => 2345)
    n.add_attribute(a).should == a
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = 5 WHERE (id = 2345)']
  end
  
  it "should have add_ method not add the same object to the cached association array if the object is already in the array" do
    @c2.one_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    a = @c1.load(:id => 2345)
    n.associations[:attributes] = []
    a.should == n.add_attribute(a)
    a.should == n.add_attribute(a)
    a.values.should == {:node_id => 1234, :id => 2345}
    n.attributes.should == [a]
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = 1234 WHERE (id = 2345)'] * 2
  end

  it "should have add_ method respect composite keys" do
    @c2.one_to_many :attributes, :class => @c1, :key =>[:node_id, :y], :primary_key=>[:id, :x]
    
    n = @c2.load(:id => 1234, :x=>5)
    a = @c1.load(:id => 2345)
    n.add_attribute(a).should == a
    sqls = MODEL_DB.sqls
    sqls.shift.should =~ /UPDATE attributes SET (node_id = 1234|y = 5), (node_id = 1234|y = 5) WHERE \(id = 2345\)/
    sqls.should == []
  end

  it "should have add_ method accept a composite key" do
    @c1.set_primary_key :id, :z
    @c2.one_to_many :attributes, :class => @c1, :key =>[:node_id, :y], :primary_key=>[:id, :x]
    @c1.dataset._fetch = {:id => 2345, :z => 8, :node_id => 1234, :y=>5}
    
    n = @c2.load(:id => 1234, :x=>5)
    a = @c1.load(:id => 2345, :z => 8, :node_id => 1234, :y=>5)
    n.add_attribute([2345, 8]).should == a
    sqls = MODEL_DB.sqls
    sqls.shift.should =~ /SELECT \* FROM attributes WHERE \(\((id|z) = (2345|8)\) AND \((id|z) = (2345|8)\)\) LIMIT 1/
    sqls.shift.should =~ /UPDATE attributes SET (node_id|y) = (1234|5), (node_id|y) = (1234|5) WHERE \(\((id|z) = (2345|8)\) AND \((id|z) = (2345|8)\)\)/
    sqls.should == []
  end
  
  it "should have remove_ method respect composite keys" do
    @c2.one_to_many :attributes, :class => @c1, :key =>[:node_id, :y], :primary_key=>[:id, :x]
    
    n = @c2.load(:id => 1234, :x=>5)
    a = @c1.load(:id => 2345, :node_id=>1234, :y=>5)
    n.remove_attribute(a).should == a
    sqls = MODEL_DB.sqls
    sqls.pop.should =~ /UPDATE attributes SET (node_id|y) = NULL, (node_id|y) = NULL WHERE \(id = 2345\)/
    sqls.should == ["SELECT 1 AS one FROM attributes WHERE ((attributes.node_id = 1234) AND (attributes.y = 5) AND (id = 2345)) LIMIT 1"]
  end
  
  it "should accept a array of composite primary key values for the remove_ method and remove an existing record" do
    @c1.set_primary_key :id, :y
    @c2.one_to_many :attributes, :class => @c1, :key=>:node_id, :primary_key=>:id
    n = @c2.new(:id => 123)
    @c1.dataset._fetch = {:id=>234, :node_id=>123, :y=>5}
    n.remove_attribute([234, 5]).should == @c1.load(:node_id => nil, :y => 5, :id => 234)
    sqls = MODEL_DB.sqls
    sqls.length.should == 2
    sqls.first.should =~ /SELECT \* FROM attributes WHERE \(\(attributes.node_id = 123\) AND \(attributes\.(id|y) = (234|5)\) AND \(attributes\.(id|y) = (234|5)\)\) LIMIT 1/
    sqls.last.should =~ /UPDATE attributes SET node_id = NULL WHERE \(\((id|y) = (234|5)\) AND \((id|y) = (234|5)\)\)/
  end
  
  it "should raise an error in add_ and remove_ if the passed object returns false to save (is not valid)" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    def a.validate() errors.add(:id, 'foo') end
    proc{n.add_attribute(a)}.should raise_error(Sequel::Error)
    proc{n.remove_attribute(a)}.should raise_error(Sequel::Error)
  end

  it "should not validate the associated object in add_ and remove_ if the :validate=>false option is used" do
    @c2.one_to_many :attributes, :class => @c1, :validate=>false
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    def a.validate() errors.add(:id, 'foo') end
    n.add_attribute(a).should == a
    n.remove_attribute(a).should == a
  end

  it "should raise an error if the model object doesn't have a valid primary key" do
    @c2.one_to_many :attributes, :class => @c1 
    a = @c2.new
    n = @c1.load(:id=>123)
    proc{a.attributes_dataset}.should raise_error(Sequel::Error)
    proc{a.add_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_all_attributes}.should raise_error(Sequel::Error)
  end
  
  it "should use :primary_key option if given" do
    @c1.one_to_many :nodes, :class => @c2, :primary_key => :node_id, :key=>:id
    @c1.load(:id => 1234, :node_id=>4321).nodes_dataset.sql.should == "SELECT * FROM nodes WHERE (nodes.id = 4321)"
  end

  it "should support a select option" do
    @c2.one_to_many :attributes, :class => @c1, :select => [:id, :name]
    @c2.new(:id => 1234).attributes_dataset.sql.should == "SELECT id, name FROM attributes WHERE (attributes.node_id = 1234)"
  end
  
  it "should support a conditions option" do
    @c2.one_to_many :attributes, :class => @c1, :conditions => {:a=>32}
    @c2.new(:id => 1234).attributes_dataset.sql.should == "SELECT * FROM attributes WHERE ((a = 32) AND (attributes.node_id = 1234))"
    @c2.one_to_many :attributes, :class => @c1, :conditions => Sequel.~(:a)
    @c2.new(:id => 1234).attributes_dataset.sql.should == "SELECT * FROM attributes WHERE (NOT a AND (attributes.node_id = 1234))"
  end
  
  it "should support an order option" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind
    @c2.new(:id => 1234).attributes_dataset.sql.should == "SELECT * FROM attributes WHERE (attributes.node_id = 1234) ORDER BY kind"
  end
  
  it "should support an array for the order option" do
    @c2.one_to_many :attributes, :class => @c1, :order => [:kind1, :kind2]
    @c2.new(:id => 1234).attributes_dataset.sql.should == "SELECT * FROM attributes WHERE (attributes.node_id = 1234) ORDER BY kind1, kind2"
  end
  
  it "should return array with all members of the association" do
    @c2.one_to_many :attributes, :class => @c1
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE (attributes.node_id = 1234)'
  end
  
  it "should accept a block" do
    @c2.one_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => nil)
    end
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (xxx IS NULL))'
  end
  
  it "should support :order option with block" do
    @c2.one_to_many :attributes, :class => @c1, :order => :kind do |ds|
      ds.filter(:xxx => nil)
    end
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (xxx IS NULL)) ORDER BY kind'
  end
  
  it "should have the block argument affect the _dataset method" do
    @c2.one_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => 456)
    end
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE ((attributes.node_id = 1234) AND (xxx = 456))'
  end
  
  it "should support a :dataset option that is used instead of the default" do
    c1 = @c1
    @c2.one_to_many :all_other_attributes, :class => @c1, :dataset=>proc{c1.exclude(:nodeid=>pk)}, :order=>:a, :limit=>10 do |ds|
      ds.filter(:xxx => 5)
    end
    @c2.new(:id => 1234).all_other_attributes_dataset.sql.should == 'SELECT * FROM attributes WHERE ((nodeid != 1234) AND (xxx = 5)) ORDER BY a LIMIT 10'
    @c2.new(:id => 1234).all_other_attributes.should == [@c1.load({})]
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
    n.associations.include?(:attributes).should == false
    atts = n.attributes
    atts.should == n.associations[:attributes]
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
  end

  it "should use cached instance variable if available" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    n.associations[:attributes] = 42
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    n.associations[:attributes] = 42
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.one_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    att = @c1.load(:id => 345)
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

  it "should populate the reciprocal many_to_one instance variable when loading the one_to_many association" do
    @c2.one_to_many :attributes, :class => @c1, :key => :node_id
    @c1.many_to_one :node, :class => @c2, :key => :node_id
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
    atts.should == [@c1.load({})]
    atts.map{|a| a.node}.should == [n]
    MODEL_DB.sqls.should == []
  end
  
  it "should use an explicit reciprocal instance variable if given" do
    @c2.one_to_many :attributes, :class => @c1, :key => :node_id, :reciprocal=>:wxyz
    
    n = @c2.new(:id => 1234)
    atts = n.attributes
    MODEL_DB.sqls.should == ['SELECT * FROM attributes WHERE (attributes.node_id = 1234)']
    atts.should == [@c1.load({})]
    atts.map{|a| a.associations[:wxyz]}.should == [n]
    MODEL_DB.sqls.should == []
  end
  
  it "should have an remove_all_ method that removes all associations" do
    @c2.one_to_many :attributes, :class => @c1
    @c2.new(:id => 1234).remove_all_attributes
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = NULL WHERE (node_id = 1234)']
  end

  it "should have remove_all method respect association filters" do
    @c2.one_to_many :attributes, :class => @c1, :conditions=>{:a=>1} do |ds|
      ds.filter(:b=>2)
    end
    @c2.new(:id => 1234).remove_all_attributes
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = NULL WHERE ((a = 1) AND (node_id = 1234) AND (b = 2))']
  end

  it "should have the remove_all_ method respect the :primary_key option" do
    @c2.one_to_many :attributes, :class => @c1, :primary_key=>:xxx
    @c2.new(:id => 1234, :xxx=>5).remove_all_attributes
    MODEL_DB.sqls.should == ['UPDATE attributes SET node_id = NULL WHERE (node_id = 5)']
  end
  
  it "should have the remove_all_ method respect composite keys" do
    @c2.one_to_many :attributes, :class => @c1, :key=>[:node_id, :y], :primary_key=>[:id, :x]
    @c2.new(:id => 1234, :x=>5).remove_all_attributes
    sqls = MODEL_DB.sqls
    sqls.pop.should =~ /UPDATE attributes SET (node_id|y) = NULL, (node_id|y) = NULL WHERE \(\(node_id = 1234\) AND \(y = 5\)\)/
    sqls.should == []
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
    @c1.dataset._fetch = [[], [{:id=>3, :node_id=>1234}]]
    node.attributes.should == []
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
    @c2.dataset._fetch = []
    @c1.dataset._fetch = [[], [{:id=>3, :node_id=>1234}]]
    attrib = @c1.new(:id=>3)
    node = @c2.load(:id => 1234)
    node.attributes.should == []
    attrib.node.should == nil
    node.add_attribute(attrib)
    attrib.associations[:node].should == node 
    node.remove_all_attributes
    attrib.associations.fetch(:node, 2).should == nil
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

  it "should allow additional arguments given to the add_ method and pass them onwards to the _add_ method" do
    @c2.one_to_many :attributes, :class => @c1
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._add_attribute(x,*y)
      @x = x
      @y = y
    end
    c.should_not_receive(:node_id=)
    p.add_attribute(c,:foo,:bar=>:baz)
    p.instance_variable_get(:@x).should == c
    p.instance_variable_get(:@y).should == [:foo,{:bar=>:baz}]
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

  it "should allow additional arguments given to the remove_ method and pass them onwards to the _remove_ method" do
    @c2.one_to_many :attributes, :class => @c1
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._remove_attribute(x,*y)
      @x = x
      @y = y
    end
    c.should_not_receive(:node_id=)
    p.remove_attribute(c,:foo,:bar=>:baz)
    p.instance_variable_get(:@x).should == c
    p.instance_variable_get(:@y).should == [:foo,{:bar=>:baz}]
  end

  it "should allow additional arguments given to the remove_all_ method and pass them onwards to the _remove_all_ method" do
    @c2.one_to_many :attributes, :class => @c1
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._remove_all_attributes(*y)
      @y = y
    end
    c.should_not_receive(:node_id=)
    p.remove_all_attributes(:foo,:bar=>:baz)
    p.instance_variable_get(:@y).should == [:foo,{:bar=>:baz}]
  end

  it "should call a _remove_all_ method internally to remove attributes" do
    @c2.one_to_many :attributes, :class => @c1
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_remove_all_attributes"))
    p = @c2.load(:id=>10)
    def p._remove_all_attributes
      @x = :foo
    end
    p.remove_all_attributes
    p.instance_variable_get(:@x).should == :foo
  end

  it "should support (before|after)_(add|remove) callbacks" do
    h = []
    @c2.one_to_many :attributes, :class => @c1, :before_add=>[proc{|x,y| h << x.pk; h << -y.pk}, :blah], :after_add=>proc{h << 3}, :before_remove=>:blah, :after_remove=>[:blahr]
    @c2.class_eval do
      self::Foo = h
      def _add_attribute(v)
        model::Foo << 4
      end
      def _remove_attribute(v)
        model::Foo << 5
      end
      def blah(x)
        model::Foo << x.pk
      end
      def blahr(x)
        model::Foo << 6
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
      self::Foo = h
      def al(v)
        v.each{|x| model::Foo << x.pk}
      end
    end
    @c1.dataset._fetch = [{:id=>20}, {:id=>30}]
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
  
  it "should raise an error if trying to use the :one_to_one option" do
    proc{@c2.one_to_many :attribute, :class => @c1, :one_to_one=>true}.should raise_error(Sequel::Error)
    proc{@c2.associate :one_to_many, :attribute, :class => @c1, :one_to_one=>true}.should raise_error(Sequel::Error)
  end
end

describe Sequel::Model, "many_to_many" do
  before do
    @c1 = Class.new(Sequel::Model(:attributes)) do
      unrestrict_primary_key
      attr_accessor :yyy
      def self.name; 'Attribute'; end
      def self.to_s; 'Attribute'; end
      columns :id, :y, :z
    end

    @c2 = Class.new(Sequel::Model(:nodes)) do
      unrestrict_primary_key
      attr_accessor :xxx
      
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
      columns :id, :x
    end
    @dataset = @c2.dataset
    @c1.dataset.autoid = 1

    [@c1, @c2].each{|c| c.dataset._fetch = {}}
    MODEL_DB.reset
  end

  it "should use implicit key values and join table if omitted" do
    @c2.many_to_many :attributes, :class => @c1 
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
  it "should use implicit class if omitted" do
    begin
      class ::Tag < Sequel::Model; end
      @c2.many_to_many :tags
      @c2.new(:id => 1234).tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN nodes_tags ON ((nodes_tags.tag_id = tags.id) AND (nodes_tags.node_id = 1234))'
    ensure
      Object.send(:remove_const, :Tag)
    end
  end
  
  it "should use class inside module if given as a string" do
    begin
      module ::Historical
        class Tag < Sequel::Model; end
      end
      @c2.many_to_many :tags, :class=>'::Historical::Tag'
      @c2.new(:id => 1234).tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN nodes_tags ON ((nodes_tags.tag_id = tags.id) AND (nodes_tags.node_id = 1234))'
    ensure
      Object.send(:remove_const, :Historical)
    end
  end
  
  it "should respect :eager_loader_predicate_key when lazily loading" do
    @c2.many_to_many :attributes, :class => @c1, :eager_loading_predicate_key=>Sequel.subscript(:attributes_nodes__node_id, 0)
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id[0] = 1234))'
  end
  
  it "should use explicit key values and join table if given" do
    @c2.many_to_many :attributes, :class => @c1, :left_key => :nodeid, :right_key => :attributeid, :join_table => :attribute2node
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attribute2node ON ((attribute2node.attributeid = attributes.id) AND (attribute2node.nodeid = 1234))'
  end
  
  it "should support a conditions option" do
    @c2.many_to_many :attributes, :class => @c1, :conditions => {:a=>32}
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (a = 32)'

    @c2.many_to_many :attributes, :class => @c1, :conditions => ['a = ?', 32]
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (a = 32)'
    @c2.new(:id => 1234).attributes.should == [@c1.load({})]
  end
  
  it "should support an order option" do
    @c2.many_to_many :attributes, :class => @c1, :order => :blah
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) ORDER BY blah'
  end
  
  it "should support an array for the order option" do
    @c2.many_to_many :attributes, :class => @c1, :order => [:blah1, :blah2]
    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) ORDER BY blah1, blah2'
  end
  
  it "should support :left_primary_key and :right_primary_key options" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx, :right_primary_key=>:yyy
    @c2.new(:id => 1234, :xxx=>5).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.yyy) AND (attributes_nodes.node_id = 5))'
  end
  
  it "should support composite keys" do
    @c2.many_to_many :attributes, :class => @c1, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :left_primary_key=>[:id, :x], :right_primary_key=>[:id, :y]
    @c2.load(:id => 1234, :x=>5).attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.r1 = attributes.id) AND (attributes_nodes.r2 = attributes.y) AND (attributes_nodes.l1 = 1234) AND (attributes_nodes.l2 = 5))'
  end
  
  it "should not issue query if not all keys have values" do
    @c2.many_to_many :attributes, :class => @c1, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :left_primary_key=>[:id, :x], :right_primary_key=>[:id, :y]
    @c2.load(:id => 1234, :x=>nil).attributes.should == []
    MODEL_DB.sqls.should == []
  end
  
  it "should raise an Error unless same number of composite keys used" do
    proc{@c2.many_to_many :attributes, :class => @c1, :left_key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.many_to_many :attributes, :class => @c1, :left_primary_key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.many_to_many :attributes, :class => @c1, :left_key=>[:node_id, :id], :left_primary_key=>:id}.should raise_error(Sequel::Error)
    proc{@c2.many_to_many :attributes, :class => @c1, :left_key=>:id, :left_primary_key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.many_to_many :attributes, :class => @c1, :left_key=>[:node_id, :id, :x], :left_primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
    
    proc{@c2.many_to_many :attributes, :class => @c1, :right_primary_key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.many_to_many :attributes, :class => @c1, :right_key=>[:node_id, :id], :right_primary_key=>:id}.should raise_error(Sequel::Error)
    proc{@c2.many_to_many :attributes, :class => @c1, :right_key=>:id, :left_primary_key=>[:node_id, :id]}.should raise_error(Sequel::Error)
    proc{@c2.many_to_many :attributes, :class => @c1, :right_key=>[:node_id, :id, :x], :right_primary_key=>[:parent_id, :id]}.should raise_error(Sequel::Error)
  end
  
  it "should support a select option" do
    @c2.many_to_many :attributes, :class => @c1, :select => :blah

    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT blah FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
  it "should support an array for the select option" do
    @c2.many_to_many :attributes, :class => @c1, :select => [Sequel::SQL::ColumnAll.new(:attributes), :attribute_nodes__blah2]

    @c2.new(:id => 1234).attributes_dataset.sql.should == 'SELECT attributes.*, attribute_nodes.blah2 FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))'
  end
  
  it "should accept a block" do
    @c2.many_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    n.attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (xxx = 555)'
  end

  it "should allow the :order option while accepting a block" do
    @c2.many_to_many :attributes, :class => @c1, :order=>[:blah1, :blah2] do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    n.attributes_dataset.sql.should == 'SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (xxx = 555) ORDER BY blah1, blah2'
  end

  it "should support a :dataset option that is used instead of the default" do
    c1 = @c1
    @c2.many_to_many :attributes, :class => @c1, :dataset=>proc{c1.join_table(:natural, :an).filter(:an__nodeid=>pk)}, :order=> :a, :limit=>10, :select=>nil do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    n.attributes_dataset.sql.should == 'SELECT * FROM attributes NATURAL JOIN an WHERE ((an.nodeid = 1234) AND (xxx = 555)) ORDER BY a LIMIT 10'
    n.attributes.should == [@c1.load({})]
    MODEL_DB.sqls.should == ['SELECT * FROM attributes NATURAL JOIN an WHERE ((an.nodeid = 1234) AND (xxx = 555)) ORDER BY a LIMIT 10']
  end

  it "should support a :dataset option that accepts the reflection as an argument" do
    @c2.many_to_many :attributes, :class => @c1, :dataset=>lambda{|opts| opts.associated_dataset.join_table(:natural, :an).filter(:an__nodeid=>pk)}, :order=> :a, :limit=>10, :select=>nil do |ds|
      ds.filter(:xxx => @xxx)
    end

    n = @c2.new(:id => 1234)
    n.xxx = 555
    n.attributes_dataset.sql.should == 'SELECT * FROM attributes NATURAL JOIN an WHERE ((an.nodeid = 1234) AND (xxx = 555)) ORDER BY a LIMIT 10'
    n.attributes.should == [@c1.load({})]
    MODEL_DB.sqls.should == ['SELECT * FROM attributes NATURAL JOIN an WHERE ((an.nodeid = 1234) AND (xxx = 555)) ORDER BY a LIMIT 10']
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
  
  it "should handle an aliased join table" do
    @c2.many_to_many :attributes, :class => @c1, :join_table => :attribute2node___attributes_nodes
    n = @c2.load(:id => 1234)
    a = @c1.load(:id => 2345)
    n.attributes_dataset.sql.should == "SELECT attributes.* FROM attributes INNER JOIN attribute2node AS attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))"
    a.should == n.add_attribute(a)
    a.should == n.remove_attribute(a)
    n.remove_all_attributes
    sqls = MODEL_DB.sqls
    ['INSERT INTO attribute2node (node_id, attribute_id) VALUES (1234, 2345)',
     'INSERT INTO attribute2node (attribute_id, node_id) VALUES (2345, 1234)'].should(include(sqls.shift))
    ["DELETE FROM attribute2node WHERE ((node_id = 1234) AND (attribute_id = 2345))", 
     "DELETE FROM attribute2node WHERE ((attribute_id = 2345) AND (node_id = 1234))"].should(include(sqls.shift))
    sqls.should == ["DELETE FROM attribute2node WHERE (node_id = 1234)"]
  end
  
  it "should define an add_ method that works on existing records" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.load(:id => 1234)
    a = @c1.load(:id => 2345)
    n.add_attribute(a).should == a
    sqls = MODEL_DB.sqls
    ['INSERT INTO attributes_nodes (node_id, attribute_id) VALUES (1234, 2345)',
     'INSERT INTO attributes_nodes (attribute_id, node_id) VALUES (2345, 1234)'].should(include(sqls.shift))
    sqls.should == []
  end

  it "should define an add_ method that works with a primary key" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.load(:id => 1234)
    a = @c1.load(:id => 2345)
    @c1.dataset._fetch = {:id=>2345}
    n.add_attribute(2345).should == a
    sqls = MODEL_DB.sqls
    ['INSERT INTO attributes_nodes (node_id, attribute_id) VALUES (1234, 2345)',
     'INSERT INTO attributes_nodes (attribute_id, node_id) VALUES (2345, 1234)'].should(include(sqls.pop))
    sqls.should == ["SELECT * FROM attributes WHERE id = 2345"]
  end

  it "should allow passing a hash to the add_ method which creates a new record" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.load(:id => 1234)
    @c1.dataset._fetch = @c1.instance_dataset._fetch = {:id=>1}
    n.add_attribute(:id => 1).should == @c1.load(:id => 1)
    sqls = MODEL_DB.sqls
    ['INSERT INTO attributes_nodes (node_id, attribute_id) VALUES (1234, 1)',
     'INSERT INTO attributes_nodes (attribute_id, node_id) VALUES (1, 1234)'
    ].should(include(sqls.pop))
    sqls.should == ['INSERT INTO attributes (id) VALUES (1)', "SELECT * FROM attributes WHERE (id = 1) LIMIT 1"]
  end

  it "should define a remove_ method that works on existing records" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.new(:id => 1234)
    a = @c1.new(:id => 2345)
    n.remove_attribute(a).should == a
    MODEL_DB.sqls.should == ['DELETE FROM attributes_nodes WHERE ((node_id = 1234) AND (attribute_id = 2345))']
  end

  it "should raise an error in the add_ method if the passed associated object is not of the correct type" do
    @c2.many_to_many :attributes, :class => @c1
    proc{@c2.new(:id => 1234).add_attribute(@c2.new)}.should raise_error(Sequel::Error)
  end

  it "should accept a primary key for the remove_ method and remove an existing record" do
    @c2.many_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    @c1.dataset._fetch = {:id=>234}
    n.remove_attribute(234).should == @c1.load(:id => 234)
    MODEL_DB.sqls.should == ["SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE (attributes.id = 234) LIMIT 1",
      "DELETE FROM attributes_nodes WHERE ((node_id = 1234) AND (attribute_id = 234))"]
  end
    
  it "should raise an error in the remove_ method if the passed associated object is not of the correct type" do
    @c2.many_to_many :attributes, :class => @c1
    proc{@c2.new(:id => 1234).remove_attribute(@c2.new)}.should raise_error(Sequel::Error)
  end

  it "should have the add_ method respect the :left_primary_key and :right_primary_key options" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx, :right_primary_key=>:yyy
    
    n = @c2.load(:id => 1234).set(:xxx=>5)
    a = @c1.load(:id => 2345).set(:yyy=>8)
    n.add_attribute(a).should == a
    sqls = MODEL_DB.sqls
    ['INSERT INTO attributes_nodes (node_id, attribute_id) VALUES (5, 8)',
     'INSERT INTO attributes_nodes (attribute_id, node_id) VALUES (8, 5)'
    ].should(include(sqls.pop))
    sqls.should == []
  end
  
  it "should have add_ method not add the same object to the cached association array if the object is already in the array" do
    @c2.many_to_many :attributes, :class => @c1
    
    n = @c2.load(:id => 1234).set(:xxx=>5)
    a = @c1.load(:id => 2345).set(:yyy=>8)
    n.associations[:attributes] = []
    a.should == n.add_attribute(a)
    a.should == n.add_attribute(a)
    n.attributes.should == [a]
  end
  
  it "should have the add_ method respect composite keys" do
    @c2.many_to_many :attributes, :class => @c1, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :left_primary_key=>[:id, :x], :right_primary_key=>[:id, :z]
    n = @c2.load(:id => 1234, :x=>5)
    a = @c1.load(:id => 2345, :z=>8)
    a.should == n.add_attribute(a)
    sqls = MODEL_DB.sqls
    m = /INSERT INTO attributes_nodes \((\w+), (\w+), (\w+), (\w+)\) VALUES \((\d+), (\d+), (\d+), (\d+)\)/.match(sqls.pop)
    sqls.should == []
    m.should_not == nil
    map = {'l1'=>1234, 'l2'=>5, 'r1'=>2345, 'r2'=>8}
    %w[l1 l2 r1 r2].each do |x|
      v = false
      4.times do |i| i += 1
        if m[i] == x
          m[i+4].should == map[x].to_s
          v = true
        end
      end
      v.should == true
    end
  end
  
  it "should have the add_ method respect composite keys" do
    @c2.many_to_many :attributes, :class => @c1, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :left_primary_key=>[:id, :x], :right_primary_key=>[:id, :z]
    @c1.set_primary_key [:id, :z]
    n = @c2.load(:id => 1234, :x=>5)
    a = @c1.load(:id => 2345, :z=>8)
    @c1.dataset._fetch = {:id => 2345, :z=>8}
    n.add_attribute([2345, 8]).should == a
    sqls = MODEL_DB.sqls
    sqls.shift.should =~ /SELECT \* FROM attributes WHERE \(\((id|z) = (8|2345)\) AND \((id|z) = (8|2345)\)\) LIMIT 1/
    sqls.pop.should =~ /INSERT INTO attributes_nodes \([lr][12], [lr][12], [lr][12], [lr][12]\) VALUES \((1234|5|2345|8), (1234|5|2345|8), (1234|5|2345|8), (1234|5|2345|8)\)/
    sqls.should == []
  end

  it "should have the remove_ method respect the :left_primary_key and :right_primary_key options" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx, :right_primary_key=>:yyy
    
    n = @c2.new(:id => 1234, :xxx=>5)
    a = @c1.new(:id => 2345, :yyy=>8)
    n.remove_attribute(a).should == a
    MODEL_DB.sqls.should == ['DELETE FROM attributes_nodes WHERE ((node_id = 5) AND (attribute_id = 8))']
  end
  
  it "should have the remove_ method respect composite keys" do
    @c2.many_to_many :attributes, :class => @c1, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :left_primary_key=>[:id, :x], :right_primary_key=>[:id, :z]
    n = @c2.load(:id => 1234, :x=>5)
    a = @c1.load(:id => 2345, :z=>8)
    a.should == n.remove_attribute(a)
    MODEL_DB.sqls.should == ["DELETE FROM attributes_nodes WHERE ((l1 = 1234) AND (l2 = 5) AND (r1 = 2345) AND (r2 = 8))"]
  end

  it "should accept a array of composite primary key values for the remove_ method and remove an existing record" do
    @c1.set_primary_key [:id, :y]
    @c2.many_to_many :attributes, :class => @c1
    n = @c2.new(:id => 1234)
    @c1.dataset._fetch = {:id=>234, :y=>8}
    @c1.load(:id => 234, :y=>8).should == n.remove_attribute([234, 8])
    sqls = MODEL_DB.sqls
    ["SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE ((attributes.id = 234) AND (attributes.y = 8)) LIMIT 1",
      "SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234)) WHERE ((attributes.y = 8) AND (attributes.id = 234)) LIMIT 1"].should include(sqls.shift)
    sqls.should == ["DELETE FROM attributes_nodes WHERE ((node_id = 1234) AND (attribute_id = 234))"]
  end
    
  it "should raise an error if the model object doesn't have a valid primary key" do
    @c2.many_to_many :attributes, :class => @c1 
    a = @c2.new
    n = @c1.load(:id=>123)
    proc{a.attributes_dataset}.should raise_error(Sequel::Error)
    proc{a.add_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_attribute(n)}.should raise_error(Sequel::Error)
    proc{a.remove_all_attributes}.should raise_error(Sequel::Error)
  end
  
  it "should save the associated object first in add_ if passed a new model object" do
    @c2.many_to_many :attributes, :class => @c1 
    n = @c1.new
    a = @c2.load(:id=>123)
    n.new?.should == true
    @c1.dataset._fetch = {:id=>1}
    a.add_attribute(n)
    n.new?.should == false
  end

  it "should raise a ValidationFailed in add_ if the associated object is new and invalid" do
    @c2.many_to_many :attributes, :class => @c1 
    n = @c1.new
    a = @c2.load(:id=>123)
    def n.validate() errors.add(:id, 'foo') end
    proc{a.add_attribute(n)}.should raise_error(Sequel::ValidationFailed)
  end

  it "should raise an Error in add_ if the associated object is new and invalid and raise_on_save_failure is false" do
    @c2.many_to_many :attributes, :class => @c1 
    n = @c1.new
    n.raise_on_save_failure = false
    a = @c2.load(:id=>123)
    def n.validate() errors.add(:id, 'foo') end
    proc{a.add_attribute(n)}.should raise_error(Sequel::Error)
  end

  it "should not attempt to validate the associated object in add_ if the :validate=>false option is used" do
    @c2.many_to_many :attributes, :class => @c1, :validate=>false
    n = @c1.new
    a = @c2.load(:id=>123)
    def n.validate() errors.add(:id, 'foo') end
    @c1.dataset._fetch = {:id=>1}
    a.add_attribute(n)
    n.new?.should == false
  end

  it "should raise an error if trying to remove a model object that doesn't have a valid primary key" do
    @c2.many_to_many :attributes, :class => @c1 
    n = @c1.new
    a = @c2.load(:id=>123)
    proc{a.remove_attribute(n)}.should raise_error(Sequel::Error)
  end

  it "should provide an array with all members of the association" do
    @c2.many_to_many :attributes, :class => @c1
    
    @c2.new(:id => 1234).attributes.should == [@c1.load({})]
    MODEL_DB.sqls.should == ['SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))']
  end

  it "should set cached instance variable when accessed" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    n.associations.include?(:attributes).should == false
    atts = n.attributes
    atts.should == n.associations[:attributes]
  end

  it "should use cached instance variable if available" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    n.associations[:attributes] = 42
    n.attributes.should == 42
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    n.associations[:attributes] = 42
    n.attributes(true).should_not == 42
    MODEL_DB.sqls.should == ["SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1234))"]
  end

  it "should add item to cached instance variable if it exists when calling add_" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.load(:id => 345)
    a = []
    n.associations[:attributes] = a
    n.add_attribute(att)
    a.should == [att]
  end

  it "should add item to reciprocal cached instance variable if it exists when calling add_" do
    @c2.many_to_many :attributes, :class => @c1
    @c1.many_to_many :nodes, :class => @c2

    n = @c2.new(:id => 1234)
    att = @c1.load(:id => 345)
    att.associations[:nodes] = []
    n.add_attribute(att)
    att.nodes.should == [n]
  end

  it "should remove item from cached instance variable if it exists when calling remove_" do
    @c2.many_to_many :attributes, :class => @c1

    n = @c2.new(:id => 1234)
    att = @c1.load(:id => 345)
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

  it "should have an remove_all_ method that removes all associations" do
    @c2.many_to_many :attributes, :class => @c1
    @c2.new(:id => 1234).remove_all_attributes
    MODEL_DB.sqls.should == ['DELETE FROM attributes_nodes WHERE (node_id = 1234)']
  end

  it "should have the remove_all_ method respect the :left_primary_key option" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>:xxx
    @c2.new(:id => 1234, :xxx=>5).remove_all_attributes
    MODEL_DB.sqls.should == ['DELETE FROM attributes_nodes WHERE (node_id = 5)']
  end
  
  it "should have the remove_all_ method respect composite keys" do
    @c2.many_to_many :attributes, :class => @c1, :left_primary_key=>[:id, :x], :left_key=>[:l1, :l2]
    @c2.load(:id => 1234, :x=>5).remove_all_attributes
    MODEL_DB.sqls.should == ['DELETE FROM attributes_nodes WHERE ((l1 = 1234) AND (l2 = 5))']
  end

  it "remove_all should set the cached instance variable to []" do
    @c2.many_to_many :attributes, :class => @c1
    node = @c2.new(:id => 1234)
    node.remove_all_attributes
    node.associations[:attributes].should == []
  end

  it "remove_all should return the array of previously associated items if the cached instance variable exists" do
    @c2.many_to_many :attributes, :class => @c1
    attrib = @c1.load(:id=>3)
    node = @c2.load(:id => 1234)
    @c1.dataset._fetch = []
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
    @c1.dataset._fetch = []
    @c2.dataset._fetch = []
    attrib = @c1.load(:id=>3)
    node = @c2.new(:id => 1234)
    node.attributes.should == []
    attrib.nodes.should == []
    node.add_attribute(attrib)
    attrib.associations[:nodes].should == [node]
    node.remove_all_attributes
    attrib.associations[:nodes].should == []
  end

  it "add, remove, and remove_all methods should respect :join_table_block option" do
    @c2.many_to_many :attributes, :class => @c1, :join_table_block=>proc{|ds| ds.filter(:x=>123).set_overrides(:x=>123)}
    o = @c2.load(:id => 1234)
    o.add_attribute(@c1.load(:id=>44))
    o.remove_attribute(@c1.load(:id=>45))
    o.remove_all_attributes
    sqls = MODEL_DB.sqls
    sqls.shift =~ /INSERT INTO attributes_nodes \((node_id|attribute_id|x), (node_id|attribute_id|x), (node_id|attribute_id|x)\) VALUES \((1234|123|44), (1234|123|44), (1234|123|44)\)/
    sqls.should == ["DELETE FROM attributes_nodes WHERE ((x = 123) AND (node_id = 1234) AND (attribute_id = 45))",
      "DELETE FROM attributes_nodes WHERE ((x = 123) AND (node_id = 1234))"]
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

  it "should allow additional arguments given to the add_ method and pass them onwards to the _add_ method" do
    @c2.many_to_many :attributes, :class => @c1
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._add_attribute(x,*y)
      @x = x
      @y = y
    end
    p.add_attribute(c,:foo,:bar=>:baz)
    p.instance_variable_get(:@x).should == c
    p.instance_variable_get(:@y).should == [:foo,{:bar=>:baz}]
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

  it "should allow additional arguments given to the remove_ method and pass them onwards to the _remove_ method" do
    @c2.many_to_many :attributes, :class => @c1
    p = @c2.load(:id=>10)
    c = @c1.load(:id=>123)
    def p._remove_attribute(x,*y)
      @x = x
      @y = y
    end
    p.remove_attribute(c,:foo,:bar=>:baz)
    p.instance_variable_get(:@x).should == c
    p.instance_variable_get(:@y).should == [:foo,{:bar=>:baz}]
  end

  it "should allow additional arguments given to the remove_all_ method and pass them onwards to the _remove_all_ method" do
    @c2.many_to_many :attributes, :class => @c1
    p = @c2.load(:id=>10)
    def p._remove_all_attributes(*y)
      @y = y
    end
    p.remove_all_attributes(:foo,:bar=>:baz)
    p.instance_variable_get(:@y).should == [:foo,{:bar=>:baz}]
  end

  it "should call a _remove_all_ method internally to remove attributes" do
    @c2.many_to_many :attributes, :class => @c1
    @c2.private_instance_methods.collect{|x| x.to_s}.sort.should(include("_remove_all_attributes"))
    p = @c2.load(:id=>10)
    def p._remove_all_attributes
      @x = :foo
    end
    p.remove_all_attributes
    p.instance_variable_get(:@x).should == :foo
    MODEL_DB.sqls.should == []
  end

  it "should support (before|after)_(add|remove) callbacks" do
    h = []
    @c2.many_to_many :attributes, :class => @c1, :before_add=>[proc{|x,y| h << x.pk; h << -y.pk}, :blah], :after_add=>proc{h << 3}, :before_remove=>:blah, :after_remove=>[:blahr]
    @c2.class_eval do
      self::Foo = h
      def _add_attribute(v)
        model::Foo << 4
      end
      def _remove_attribute(v)
        model::Foo << 5
      end
      def blah(x)
        model::Foo << x.pk
      end
      def blahr(x)
        model::Foo << 6
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
      self::Foo = h
      def al(v)
        v.each{|x| model::Foo << x.pk}
      end
    end
    @c1.dataset._fetch = [{:id=>20}, {:id=>30}]
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
    @c2.many_to_many :attributes, :class => @c1, :uniq=>true
    @c1.dataset._fetch = [{:id=>20}, {:id=>30}, {:id=>20}, {:id=>30}]
    @c2.load(:id=>10, :parent_id=>20).attributes.should == [@c1.load(:id=>20), @c1.load(:id=>30)]
  end
  
  it "should support a :distinct option that uses the DISTINCT clause" do
    @c2.many_to_many :attributes, :class => @c1, :distinct=>true
    @c2.load(:id=>10).attributes_dataset.sql.should == "SELECT DISTINCT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 10))"
  end

  it "should not apply association options when removing all associated records" do
    @c2.many_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:name=>'John')
    end
    @c2.load(:id=>1).remove_all_attributes
    MODEL_DB.sqls.should == ["DELETE FROM attributes_nodes WHERE (node_id = 1)"]
  end

  it "should use assocation's dataset when grabbing a record to remove from the assocation by primary key" do
    @c2.many_to_many :attributes, :class => @c1 do |ds|
      ds.filter(:join_table_att=>3)
    end
    @c1.dataset._fetch = {:id=>2}
    @c2.load(:id=>1).remove_attribute(2)
    MODEL_DB.sqls.should == ["SELECT attributes.* FROM attributes INNER JOIN attributes_nodes ON ((attributes_nodes.attribute_id = attributes.id) AND (attributes_nodes.node_id = 1)) WHERE ((join_table_att = 3) AND (attributes.id = 2)) LIMIT 1",
      "DELETE FROM attributes_nodes WHERE ((node_id = 1) AND (attribute_id = 2))"] 
  end
end

describe "Filtering by associations" do
  before do
    @Album = Class.new(Sequel::Model(:albums))
    artist = @Artist = Class.new(Sequel::Model(:artists))
    tag = @Tag = Class.new(Sequel::Model(:tags))
    track = @Track = Class.new(Sequel::Model(:tracks))
    album_info = @AlbumInfo = Class.new(Sequel::Model(:album_infos))
    @Artist.columns :id, :id1, :id2
    @Tag.columns :id, :tid1, :tid2
    @Track.columns :id, :album_id, :album_id1, :album_id2
    @AlbumInfo.columns :id, :album_id, :album_id1, :album_id2
    @Album.class_eval do
      columns :id, :id1, :id2, :artist_id, :artist_id1, :artist_id2
      many_to_one :artist, :class=>artist
      one_to_many :tracks, :class=>track, :key=>:album_id
      one_to_one :album_info, :class=>album_info, :key=>:album_id
      many_to_many :tags, :class=>tag, :left_key=>:album_id, :join_table=>:albums_tags

      many_to_one :cartist, :class=>artist, :key=>[:artist_id1, :artist_id2], :primary_key=>[:id1, :id2]
      one_to_many :ctracks, :class=>track, :key=>[:album_id1, :album_id2], :primary_key=>[:id1, :id2]
      one_to_one :calbum_info, :class=>album_info, :key=>[:album_id1, :album_id2], :primary_key=>[:id1, :id2]
      many_to_many :ctags, :class=>tag, :left_key=>[:album_id1, :album_id2], :left_primary_key=>[:id1, :id2], :right_key=>[:tag_id1, :tag_id2], :right_primary_key=>[:tid1, :tid2], :join_table=>:albums_tags
    end
  end

  it "should be able to filter on many_to_one associations" do
    @Album.filter(:artist=>@Artist.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE (albums.artist_id = 3)'
  end

  it "should be able to filter on one_to_many associations" do
    @Album.filter(:tracks=>@Track.load(:album_id=>3)).sql.should == 'SELECT * FROM albums WHERE (albums.id = 3)'
  end

  it "should be able to filter on one_to_one associations" do
    @Album.filter(:album_info=>@AlbumInfo.load(:album_id=>3)).sql.should == 'SELECT * FROM albums WHERE (albums.id = 3)'
  end

  it "should be able to filter on many_to_many associations" do
    @Album.filter(:tags=>@Tag.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id = 3) AND (albums_tags.album_id IS NOT NULL))))'
  end

  it "should be able to filter on many_to_one associations with composite keys" do
    @Album.filter(:cartist=>@Artist.load(:id1=>3, :id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id1 = 3) AND (albums.artist_id2 = 4))'
  end

  it "should be able to filter on one_to_many associations with composite keys" do
    @Album.filter(:ctracks=>@Track.load(:album_id1=>3, :album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1 = 3) AND (albums.id2 = 4))'
  end

  it "should be able to filter on one_to_one associations with composite keys" do
    @Album.filter(:calbum_info=>@AlbumInfo.load(:album_id1=>3, :album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1 = 3) AND (albums.id2 = 4))' 
  end

  it "should be able to filter on many_to_many associations with composite keys" do
    @Album.filter(:ctags=>@Tag.load(:tid1=>3, :tid2=>4)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE ((albums_tags.tag_id1 = 3) AND (albums_tags.tag_id2 = 4) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL))))'
  end

  it "should work inside a complex filter" do
    artist = @Artist.load(:id=>3)
    @Album.filter{foo & {:artist=>artist}}.sql.should == 'SELECT * FROM albums WHERE (foo AND (albums.artist_id = 3))'
    track = @Track.load(:album_id=>4)
    @Album.filter{foo & [[:artist, artist], [:tracks, track]]}.sql.should == 'SELECT * FROM albums WHERE (foo AND (albums.artist_id = 3) AND (albums.id = 4))'
  end

  it "should raise for an invalid association name" do
    proc{@Album.filter(:foo=>@Artist.load(:id=>3)).sql}.should raise_error(Sequel::Error)
  end

  it "should raise for an invalid association type" do
    @Album.many_to_many :iatags, :clone=>:tags
    @Album.association_reflection(:iatags)[:type] = :foo
    proc{@Album.filter(:iatags=>@Tag.load(:id=>3)).sql}.should raise_error(Sequel::Error)
  end

  it "should raise for an invalid associated object class " do
    proc{@Album.filter(:tags=>@Artist.load(:id=>3)).sql}.should raise_error(Sequel::Error)
  end

  it "should raise for an invalid associated object class when multiple objects are used" do
    proc{@Album.filter(:tags=>[@Tag.load(:id=>3), @Artist.load(:id=>3)]).sql}.should raise_error(Sequel::Error)
  end

  it "should correctly handle case when a multiple value association is used" do
    proc{@Album.filter(:tags=>[@Tag.load(:id=>3), @Artist.load(:id=>3)]).sql}.should raise_error(Sequel::Error)
  end

  it "should not affect non-association IN/NOT IN filtering with an empty array" do
    @Album.filter(:tag_id=>[]).sql.should == 'SELECT * FROM albums WHERE (tag_id != tag_id)'
    @Album.exclude(:tag_id=>[]).sql.should == 'SELECT * FROM albums WHERE (tag_id = tag_id)'
  end

  it "should work correctly in subclasses" do
    c = Class.new(@Album)
    c.many_to_one :sartist, :class=>@Artist
    c.filter(:sartist=>@Artist.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE (albums.sartist_id = 3)'
  end

  it "should be able to exclude on many_to_one associations" do
    @Album.exclude(:artist=>@Artist.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id != 3) OR (albums.artist_id IS NULL))'
  end

  it "should be able to exclude on one_to_many associations" do
    @Album.exclude(:tracks=>@Track.load(:album_id=>3)).sql.should == 'SELECT * FROM albums WHERE ((albums.id != 3) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on one_to_one associations" do
    @Album.exclude(:album_info=>@AlbumInfo.load(:album_id=>3)).sql.should == 'SELECT * FROM albums WHERE ((albums.id != 3) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on many_to_many associations" do
    @Album.exclude(:tags=>@Tag.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id = 3) AND (albums_tags.album_id IS NOT NULL)))) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on many_to_one associations with composite keys" do
    @Album.exclude(:cartist=>@Artist.load(:id1=>3, :id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id1 != 3) OR (albums.artist_id2 != 4) OR (albums.artist_id1 IS NULL) OR (albums.artist_id2 IS NULL))'
  end

  it "should be able to exclude on one_to_many associations with composite keys" do
    @Album.exclude(:ctracks=>@Track.load(:album_id1=>3, :album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1 != 3) OR (albums.id2 != 4) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to exclude on one_to_one associations with composite keys" do
    @Album.exclude(:calbum_info=>@AlbumInfo.load(:album_id1=>3, :album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1 != 3) OR (albums.id2 != 4) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))' 
  end

  it "should be able to exclude on many_to_many associations with composite keys" do
    @Album.exclude(:ctags=>@Tag.load(:tid1=>3, :tid2=>4)).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE ((albums_tags.tag_id1 = 3) AND (albums_tags.tag_id2 = 4) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL)))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to filter on multiple many_to_one associations" do
    @Album.filter(:artist=>[@Artist.load(:id=>3), @Artist.load(:id=>4)]).sql.should == 'SELECT * FROM albums WHERE (albums.artist_id IN (3, 4))'
  end

  it "should be able to filter on multiple one_to_many associations" do
    @Album.filter(:tracks=>[@Track.load(:album_id=>3), @Track.load(:album_id=>4)]).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (3, 4))'
  end

  it "should be able to filter on multiple one_to_one associations" do
    @Album.filter(:album_info=>[@AlbumInfo.load(:album_id=>3), @AlbumInfo.load(:album_id=>4)]).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (3, 4))'
  end

  it "should be able to filter on multiple many_to_many associations" do
    @Album.filter(:tags=>[@Tag.load(:id=>3), @Tag.load(:id=>4)]).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id IN (3, 4)) AND (albums_tags.album_id IS NOT NULL))))'
  end

  it "should be able to filter on multiple many_to_one associations with composite keys" do
    @Album.filter(:cartist=>[@Artist.load(:id1=>3, :id2=>4), @Artist.load(:id1=>5, :id2=>6)]).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id1, albums.artist_id2) IN ((3, 4), (5, 6)))'
  end

  it "should be able to filter on multiple one_to_many associations with composite keys" do
    @Album.filter(:ctracks=>[@Track.load(:album_id1=>3, :album_id2=>4), @Track.load(:album_id1=>5, :album_id2=>6)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN ((3, 4), (5, 6)))'
  end

  it "should be able to filter on multiple one_to_one associations with composite keys" do
    @Album.filter(:calbum_info=>[@AlbumInfo.load(:album_id1=>3, :album_id2=>4), @AlbumInfo.load(:album_id1=>5, :album_id2=>6)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN ((3, 4), (5, 6)))' 
  end

  it "should be able to filter on multiple many_to_many associations with composite keys" do
    @Album.filter(:ctags=>[@Tag.load(:tid1=>3, :tid2=>4), @Tag.load(:tid1=>5, :tid2=>6)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN ((3, 4), (5, 6))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL))))'
  end

  it "should be able to exclude on multiple many_to_one associations" do
    @Album.exclude(:artist=>[@Artist.load(:id=>3), @Artist.load(:id=>4)]).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id NOT IN (3, 4)) OR (albums.artist_id IS NULL))'
  end

  it "should be able to exclude on multiple one_to_many associations" do
    @Album.exclude(:tracks=>[@Track.load(:album_id=>3), @Track.load(:album_id=>4)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (3, 4)) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on multiple one_to_one associations" do
    @Album.exclude(:album_info=>[@AlbumInfo.load(:album_id=>3), @AlbumInfo.load(:album_id=>4)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (3, 4)) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on multiple many_to_many associations" do
    @Album.exclude(:tags=>[@Tag.load(:id=>3), @Tag.load(:id=>4)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id IN (3, 4)) AND (albums_tags.album_id IS NOT NULL)))) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on multiple many_to_one associations with composite keys" do
    @Album.exclude(:cartist=>[@Artist.load(:id1=>3, :id2=>4), @Artist.load(:id1=>5, :id2=>6)]).sql.should == 'SELECT * FROM albums WHERE (((albums.artist_id1, albums.artist_id2) NOT IN ((3, 4), (5, 6))) OR (albums.artist_id1 IS NULL) OR (albums.artist_id2 IS NULL))'
  end

  it "should be able to exclude on multiple one_to_many associations with composite keys" do
    @Album.exclude(:ctracks=>[@Track.load(:album_id1=>3, :album_id2=>4), @Track.load(:album_id1=>5, :album_id2=>6)]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN ((3, 4), (5, 6))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to exclude on multiple one_to_one associations with composite keys" do
    @Album.exclude(:calbum_info=>[@AlbumInfo.load(:album_id1=>3, :album_id2=>4), @AlbumInfo.load(:album_id1=>5, :album_id2=>6)]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN ((3, 4), (5, 6))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))' 
  end

  it "should be able to exclude on multiple many_to_many associations with composite keys" do
    @Album.exclude(:ctags=>[@Tag.load(:tid1=>3, :tid2=>4), @Tag.load(:tid1=>5, :tid2=>6)]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN ((3, 4), (5, 6))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL)))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to handle NULL values when filtering many_to_one associations" do
    @Album.filter(:artist=>@Artist.new).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when filtering one_to_many associations" do
    @Album.filter(:tracks=>@Track.new).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when filtering one_to_one associations" do
    @Album.filter(:album_info=>@AlbumInfo.new).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when filtering many_to_many associations" do
    @Album.filter(:tags=>@Tag.new).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle filtering with NULL values for many_to_one associations with composite keys" do
    @Album.filter(:cartist=>@Artist.load(:id2=>4)).sql.should == 'SELECT * FROM albums WHERE \'f\''
    @Album.filter(:cartist=>@Artist.load(:id1=>3)).sql.should == 'SELECT * FROM albums WHERE \'f\''
    @Album.filter(:cartist=>@Artist.new).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to filter with NULL values for one_to_many associations with composite keys" do
    @Album.filter(:ctracks=>@Track.load(:album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE \'f\''
    @Album.filter(:ctracks=>@Track.load(:album_id1=>3)).sql.should == 'SELECT * FROM albums WHERE \'f\''
    @Album.filter(:ctracks=>@Track.new).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to filter with NULL values for one_to_one associations with composite keys" do
    @Album.filter(:calbum_info=>@AlbumInfo.load(:album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE \'f\'' 
    @Album.filter(:calbum_info=>@AlbumInfo.load(:album_id1=>3)).sql.should == 'SELECT * FROM albums WHERE \'f\'' 
    @Album.filter(:calbum_info=>@AlbumInfo.new).sql.should == 'SELECT * FROM albums WHERE \'f\'' 
  end

  it "should be able to filter with NULL values for many_to_many associations with composite keys" do
    @Album.filter(:ctags=>@Tag.load(:tid1=>3)).sql.should == 'SELECT * FROM albums WHERE \'f\''
    @Album.filter(:ctags=>@Tag.load(:tid2=>4)).sql.should == 'SELECT * FROM albums WHERE \'f\''
    @Album.filter(:ctags=>@Tag.new).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when excluding many_to_one associations" do
    @Album.exclude(:artist=>@Artist.new).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when excluding one_to_many associations" do
    @Album.exclude(:tracks=>@Track.new).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when excluding one_to_one associations" do
    @Album.exclude(:album_info=>@AlbumInfo.new).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when excluding many_to_many associations" do
    @Album.exclude(:tags=>@Tag.new).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle excluding with NULL values for many_to_one associations with composite keys" do
    @Album.exclude(:cartist=>@Artist.load(:id2=>4)).sql.should == 'SELECT * FROM albums WHERE \'t\''
    @Album.exclude(:cartist=>@Artist.load(:id1=>3)).sql.should == 'SELECT * FROM albums WHERE \'t\''
    @Album.exclude(:cartist=>@Artist.new).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to excluding with NULL values for one_to_many associations with composite keys" do
    @Album.exclude(:ctracks=>@Track.load(:album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE \'t\''
    @Album.exclude(:ctracks=>@Track.load(:album_id1=>3)).sql.should == 'SELECT * FROM albums WHERE \'t\''
    @Album.exclude(:ctracks=>@Track.new).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to excluding with NULL values for one_to_one associations with composite keys" do
    @Album.exclude(:calbum_info=>@AlbumInfo.load(:album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE \'t\'' 
    @Album.exclude(:calbum_info=>@AlbumInfo.load(:album_id1=>3)).sql.should == 'SELECT * FROM albums WHERE \'t\'' 
    @Album.exclude(:calbum_info=>@AlbumInfo.new).sql.should == 'SELECT * FROM albums WHERE \'t\'' 
  end

  it "should be able to excluding with NULL values for many_to_many associations with composite keys" do
    @Album.exclude(:ctags=>@Tag.load(:tid1=>3)).sql.should == 'SELECT * FROM albums WHERE \'t\''
    @Album.exclude(:ctags=>@Tag.load(:tid2=>4)).sql.should == 'SELECT * FROM albums WHERE \'t\''
    @Album.exclude(:ctags=>@Tag.new).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when filtering multiple many_to_one associations" do
    @Album.filter(:artist=>[@Artist.load(:id=>3), @Artist.new]).sql.should == 'SELECT * FROM albums WHERE (albums.artist_id IN (3))'
    @Album.filter(:artist=>[@Artist.new, @Artist.new]).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when filtering multiple one_to_many associations" do
    @Album.filter(:tracks=>[@Track.load(:album_id=>3), @Track.new]).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (3))'
    @Album.filter(:tracks=>[@Track.new, @Track.new]).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when filtering multiple one_to_one associations" do
    @Album.filter(:album_info=>[@AlbumInfo.load(:album_id=>3), @AlbumInfo.new]).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (3))'
    @Album.filter(:album_info=>[@AlbumInfo.new, @AlbumInfo.new]).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when filtering multiple many_to_many associations" do
    @Album.filter(:tags=>[@Tag.load(:id=>3), @Tag.new]).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id IN (3)) AND (albums_tags.album_id IS NOT NULL))))'
    @Album.filter(:tags=>[@Tag.new, @Tag.new]).sql.should == 'SELECT * FROM albums WHERE \'f\''
  end

  it "should be able to handle NULL values when filtering multiple many_to_one associations with composite keys" do
    @Album.filter(:cartist=>[@Artist.load(:id1=>3, :id2=>4), @Artist.load(:id1=>3)]).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id1, albums.artist_id2) IN ((3, 4)))'
    @Album.filter(:cartist=>[@Artist.load(:id1=>3, :id2=>4), @Artist.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id1, albums.artist_id2) IN ((3, 4)))'
  end

  it "should be able handle NULL values when filtering multiple one_to_many associations with composite keys" do
    @Album.filter(:ctracks=>[@Track.load(:album_id1=>3, :album_id2=>4), @Track.load(:album_id1=>3)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN ((3, 4)))'
    @Album.filter(:ctracks=>[@Track.load(:album_id1=>3, :album_id2=>4), @Track.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN ((3, 4)))'
  end

  it "should be able to handle NULL values when filtering multiple one_to_one associations with composite keys" do
    @Album.filter(:calbum_info=>[@AlbumInfo.load(:album_id1=>3, :album_id2=>4), @AlbumInfo.load(:album_id1=>5)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN ((3, 4)))' 
    @Album.filter(:calbum_info=>[@AlbumInfo.load(:album_id1=>3, :album_id2=>4), @AlbumInfo.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN ((3, 4)))' 
  end

  it "should be able to handle NULL values when filtering multiple many_to_many associations with composite keys" do
    @Album.filter(:ctags=>[@Tag.load(:tid1=>3, :tid2=>4), @Tag.load(:tid1=>5)]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN ((3, 4))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL))))'
    @Album.filter(:ctags=>[@Tag.load(:tid1=>3, :tid2=>4), @Tag.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN ((3, 4))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL))))'
  end

  it "should be able to handle NULL values when excluding multiple many_to_one associations" do
    @Album.exclude(:artist=>[@Artist.load(:id=>3), @Artist.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id NOT IN (3)) OR (albums.artist_id IS NULL))'
    @Album.exclude(:artist=>[@Artist.new, @Artist.new]).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when excluding multiple one_to_many associations" do
    @Album.exclude(:tracks=>[@Track.load(:album_id=>3), @Track.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (3)) OR (albums.id IS NULL))'
    @Album.exclude(:tracks=>[@Track.new, @Track.new]).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when excluding multiple one_to_one associations" do
    @Album.exclude(:album_info=>[@AlbumInfo.load(:album_id=>3), @AlbumInfo.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (3)) OR (albums.id IS NULL))'
    @Album.exclude(:album_info=>[@AlbumInfo.new, @AlbumInfo.new]).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when excluding multiple many_to_many associations" do
    @Album.exclude(:tags=>[@Tag.load(:id=>3), @Tag.new]).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id IN (3)) AND (albums_tags.album_id IS NOT NULL)))) OR (albums.id IS NULL))'
    @Album.exclude(:tags=>[@Tag.new, @Tag.new]).sql.should == 'SELECT * FROM albums WHERE \'t\''
  end

  it "should be able to handle NULL values when excluding multiple many_to_one associations with composite keys" do
    @Album.exclude(:cartist=>[@Artist.load(:id1=>3, :id2=>4), @Artist.load(:id1=>3)]).sql.should == 'SELECT * FROM albums WHERE (((albums.artist_id1, albums.artist_id2) NOT IN ((3, 4))) OR (albums.artist_id1 IS NULL) OR (albums.artist_id2 IS NULL))'
    @Album.exclude(:cartist=>[@Artist.load(:id1=>3, :id2=>4), @Artist.new]).sql.should == 'SELECT * FROM albums WHERE (((albums.artist_id1, albums.artist_id2) NOT IN ((3, 4))) OR (albums.artist_id1 IS NULL) OR (albums.artist_id2 IS NULL))'
  end

  it "should be able handle NULL values when excluding multiple one_to_many associations with composite keys" do
    @Album.exclude(:ctracks=>[@Track.load(:album_id1=>3, :album_id2=>4), @Track.load(:album_id1=>3)]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN ((3, 4))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
    @Album.exclude(:ctracks=>[@Track.load(:album_id1=>3, :album_id2=>4), @Track.new]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN ((3, 4))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to handle NULL values when excluding multiple one_to_one associations with composite keys" do
    @Album.exclude(:calbum_info=>[@AlbumInfo.load(:album_id1=>3, :album_id2=>4), @AlbumInfo.load(:album_id1=>5)]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN ((3, 4))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))' 
    @Album.exclude(:calbum_info=>[@AlbumInfo.load(:album_id1=>3, :album_id2=>4), @AlbumInfo.new]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN ((3, 4))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))' 
  end

  it "should be able to handle NULL values when excluding multiple many_to_many associations with composite keys" do
    @Album.exclude(:ctags=>[@Tag.load(:tid1=>3, :tid2=>4), @Tag.load(:tid1=>5)]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN ((3, 4))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL)))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
    @Album.exclude(:ctags=>[@Tag.load(:tid1=>3, :tid2=>4), @Tag.new]).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN ((3, 4))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL)))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to filter on many_to_one association datasets" do
    @Album.filter(:artist=>@Artist.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (albums.artist_id IN (SELECT artists.id FROM artists WHERE ((x = 1) AND (artists.id IS NOT NULL))))'
  end

  it "should be able to filter on one_to_many association datasets" do
    @Album.filter(:tracks=>@Track.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (SELECT tracks.album_id FROM tracks WHERE ((x = 1) AND (tracks.album_id IS NOT NULL))))'
  end

  it "should be able to filter on one_to_one association datasets" do
    @Album.filter(:album_info=>@AlbumInfo.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (SELECT album_infos.album_id FROM album_infos WHERE ((x = 1) AND (album_infos.album_id IS NOT NULL))))'
  end

  it "should be able to filter on many_to_many association datasets" do
    @Album.filter(:tags=>@Tag.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (albums.id IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id IN (SELECT tags.id FROM tags WHERE ((x = 1) AND (tags.id IS NOT NULL)))) AND (albums_tags.album_id IS NOT NULL))))'
  end

  it "should be able to filter on many_to_one association datasets with composite keys" do
    @Album.filter(:cartist=>@Artist.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id1, albums.artist_id2) IN (SELECT artists.id1, artists.id2 FROM artists WHERE ((x = 1) AND (artists.id1 IS NOT NULL) AND (artists.id2 IS NOT NULL))))'
  end

  it "should be able to filter on one_to_many association datasets with composite keys" do
    @Album.filter(:ctracks=>@Track.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN (SELECT tracks.album_id1, tracks.album_id2 FROM tracks WHERE ((x = 1) AND (tracks.album_id1 IS NOT NULL) AND (tracks.album_id2 IS NOT NULL))))'
  end

  it "should be able to filter on one_to_one association datasets with composite keys" do
    @Album.filter(:calbum_info=>@AlbumInfo.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN (SELECT album_infos.album_id1, album_infos.album_id2 FROM album_infos WHERE ((x = 1) AND (album_infos.album_id1 IS NOT NULL) AND (album_infos.album_id2 IS NOT NULL))))'
  end

  it "should be able to filter on many_to_many association datasets with composite keys" do
    @Album.filter(:ctags=>@Tag.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.id1, albums.id2) IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN (SELECT tags.tid1, tags.tid2 FROM tags WHERE ((x = 1) AND (tags.tid1 IS NOT NULL) AND (tags.tid2 IS NOT NULL)))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL))))'
  end

  it "should be able to exclude on many_to_one association datasets" do
    @Album.exclude(:artist=>@Artist.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.artist_id NOT IN (SELECT artists.id FROM artists WHERE ((x = 1) AND (artists.id IS NOT NULL)))) OR (albums.artist_id IS NULL))'
  end

  it "should be able to exclude on one_to_many association datasets" do
    @Album.exclude(:tracks=>@Track.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (SELECT tracks.album_id FROM tracks WHERE ((x = 1) AND (tracks.album_id IS NOT NULL)))) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on one_to_one association datasets" do
    @Album.exclude(:album_info=>@AlbumInfo.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (SELECT album_infos.album_id FROM album_infos WHERE ((x = 1) AND (album_infos.album_id IS NOT NULL)))) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on many_to_many association datasets" do
    @Album.exclude(:tags=>@Tag.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE ((albums.id NOT IN (SELECT albums_tags.album_id FROM albums_tags WHERE ((albums_tags.tag_id IN (SELECT tags.id FROM tags WHERE ((x = 1) AND (tags.id IS NOT NULL)))) AND (albums_tags.album_id IS NOT NULL)))) OR (albums.id IS NULL))'
  end

  it "should be able to exclude on many_to_one association datasets with composite keys" do
    @Album.exclude(:cartist=>@Artist.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (((albums.artist_id1, albums.artist_id2) NOT IN (SELECT artists.id1, artists.id2 FROM artists WHERE ((x = 1) AND (artists.id1 IS NOT NULL) AND (artists.id2 IS NOT NULL)))) OR (albums.artist_id1 IS NULL) OR (albums.artist_id2 IS NULL))'
  end

  it "should be able to exclude on one_to_many association datasets with composite keys" do
    @Album.exclude(:ctracks=>@Track.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN (SELECT tracks.album_id1, tracks.album_id2 FROM tracks WHERE ((x = 1) AND (tracks.album_id1 IS NOT NULL) AND (tracks.album_id2 IS NOT NULL)))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to exclude on one_to_one association datasets with composite keys" do
    @Album.exclude(:calbum_info=>@AlbumInfo.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN (SELECT album_infos.album_id1, album_infos.album_id2 FROM album_infos WHERE ((x = 1) AND (album_infos.album_id1 IS NOT NULL) AND (album_infos.album_id2 IS NOT NULL)))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should be able to exclude on many_to_many association datasets with composite keys" do
    @Album.exclude(:ctags=>@Tag.filter(:x=>1)).sql.should == 'SELECT * FROM albums WHERE (((albums.id1, albums.id2) NOT IN (SELECT albums_tags.album_id1, albums_tags.album_id2 FROM albums_tags WHERE (((albums_tags.tag_id1, albums_tags.tag_id2) IN (SELECT tags.tid1, tags.tid2 FROM tags WHERE ((x = 1) AND (tags.tid1 IS NOT NULL) AND (tags.tid2 IS NOT NULL)))) AND (albums_tags.album_id1 IS NOT NULL) AND (albums_tags.album_id2 IS NOT NULL)))) OR (albums.id1 IS NULL) OR (albums.id2 IS NULL))'
  end

  it "should do a regular IN query if the dataset for a different model is used" do
    @Album.filter(:artist=>@Album.select(:x)).sql.should == 'SELECT * FROM albums WHERE (artist IN (SELECT x FROM albums))'
  end

  it "should do a regular IN query if a non-model dataset is used" do
    @Album.filter(:artist=>@Album.db.from(:albums).select(:x)).sql.should == 'SELECT * FROM albums WHERE (artist IN (SELECT x FROM albums))'
  end
end

describe "Sequel::Model Associations with clashing column names" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :object_id=>2})
    @Foo = Class.new(Sequel::Model(@db[:foos]))
    @Bar = Class.new(Sequel::Model(@db[:bars]))
    @Foo.columns :id, :object_id
    @Bar.columns :id, :object_id
    @Foo.def_column_alias(:obj_id, :object_id)
    @Bar.def_column_alias(:obj_id, :object_id)
    @Foo.one_to_many :bars, :primary_key=>:obj_id, :primary_key_column=>:object_id, :key=>:object_id, :key_method=>:obj_id,  :class=>@Bar
    @Foo.one_to_one :bar, :primary_key=>:obj_id, :primary_key_column=>:object_id, :key=>:object_id, :key_method=>:obj_id, :class=>@Bar
    @Bar.many_to_one :foo, :key=>:obj_id, :key_column=>:object_id, :primary_key=>:object_id, :primary_key_method=>:obj_id, :class=>@Foo
    @Foo.many_to_many :mtmbars, :join_table=>:bars_foos, :left_primary_key=>:obj_id, :left_primary_key_column=>:object_id, :right_primary_key=>:object_id, :right_primary_key_method=>:obj_id, :left_key=>:foo_id, :right_key=>:object_id, :class=>@Bar
    @Bar.many_to_many :mtmfoos, :join_table=>:bars_foos, :left_primary_key=>:obj_id, :left_primary_key_column=>:object_id, :right_primary_key=>:object_id, :right_primary_key_method=>:obj_id, :left_key=>:object_id, :right_key=>:foo_id, :class=>@Foo
    @foo = @Foo.load(:id=>1, :object_id=>2)
    @bar = @Bar.load(:id=>1, :object_id=>2)
    @db.sqls
  end

  it "should have working regular association methods" do
    @Bar.first.foo.should == @foo
    @db.sqls.should == ["SELECT * FROM bars LIMIT 1", "SELECT * FROM foos WHERE (foos.object_id = 2) LIMIT 1"]
    @Foo.first.bars.should == [@bar]
    @db.sqls.should == ["SELECT * FROM foos LIMIT 1", "SELECT * FROM bars WHERE (bars.object_id = 2)"]
    @Foo.first.bar.should == @bar
    @db.sqls.should == ["SELECT * FROM foos LIMIT 1", "SELECT * FROM bars WHERE (bars.object_id = 2) LIMIT 1"]
    @Foo.first.mtmbars.should == [@bar]
    @db.sqls.should == ["SELECT * FROM foos LIMIT 1", "SELECT bars.* FROM bars INNER JOIN bars_foos ON ((bars_foos.object_id = bars.object_id) AND (bars_foos.foo_id = 2))"]
    @Bar.first.mtmfoos.should == [@foo]
    @db.sqls.should == ["SELECT * FROM bars LIMIT 1", "SELECT foos.* FROM foos INNER JOIN bars_foos ON ((bars_foos.foo_id = foos.object_id) AND (bars_foos.object_id = 2))"]
  end

  it "should have working eager loading methods" do
    @Bar.eager(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @db.sqls.should == ["SELECT * FROM bars", "SELECT * FROM foos WHERE (foos.object_id IN (2))"]
    @Foo.eager(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT * FROM foos", "SELECT * FROM bars WHERE (bars.object_id IN (2))"]
    @Foo.eager(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @db.sqls.should == ["SELECT * FROM foos", "SELECT * FROM bars WHERE (bars.object_id IN (2))"]
    @db.fetch = [[{:id=>1, :object_id=>2}], [{:id=>1, :object_id=>2, :x_foreign_key_x=>2}]]
    @Foo.eager(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT * FROM foos", "SELECT bars.*, bars_foos.foo_id AS x_foreign_key_x FROM bars INNER JOIN bars_foos ON ((bars_foos.object_id = bars.object_id) AND (bars_foos.foo_id IN (2)))"]
    @db.fetch = [[{:id=>1, :object_id=>2}], [{:id=>1, :object_id=>2, :x_foreign_key_x=>2}]]
    @Bar.eager(:mtmfoos).all.map{|o| [o, o.mtmfoos]}.should == [[@bar, [@foo]]]
    @db.sqls.should == ["SELECT * FROM bars", "SELECT foos.*, bars_foos.object_id AS x_foreign_key_x FROM foos INNER JOIN bars_foos ON ((bars_foos.foo_id = foos.object_id) AND (bars_foos.object_id IN (2)))"]
  end

  it "should have working eager graphing methods" do
    @db.fetch = {:id=>1, :object_id=>2, :foo_id=>1, :foo_object_id=>2}
    @Bar.eager_graph(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @db.sqls.should == ["SELECT bars.id, bars.object_id, foo.id AS foo_id, foo.object_id AS foo_object_id FROM bars LEFT OUTER JOIN foos AS foo ON (foo.object_id = bars.object_id)"]
    @db.fetch = {:id=>1, :object_id=>2, :bars_id=>1, :bars_object_id=>2}
    @Foo.eager_graph(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT foos.id, foos.object_id, bars.id AS bars_id, bars.object_id AS bars_object_id FROM foos LEFT OUTER JOIN bars ON (bars.object_id = foos.object_id)"]
    @db.fetch = {:id=>1, :object_id=>2, :bar_id=>1, :bar_object_id=>2}
    @Foo.eager_graph(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @db.sqls.should == ["SELECT foos.id, foos.object_id, bar.id AS bar_id, bar.object_id AS bar_object_id FROM foos LEFT OUTER JOIN bars AS bar ON (bar.object_id = foos.object_id)"]
    @db.fetch = {:id=>1, :object_id=>2, :mtmfoos_id=>1, :mtmfoos_object_id=>2}
    @Bar.eager_graph(:mtmfoos).all.map{|o| [o, o.mtmfoos]}.should == [[@bar, [@foo]]]
    @db.sqls.should == ["SELECT bars.id, bars.object_id, mtmfoos.id AS mtmfoos_id, mtmfoos.object_id AS mtmfoos_object_id FROM bars LEFT OUTER JOIN bars_foos ON (bars_foos.object_id = bars.object_id) LEFT OUTER JOIN foos AS mtmfoos ON (mtmfoos.object_id = bars_foos.foo_id)"]
    @db.fetch = {:id=>1, :object_id=>2, :mtmbars_id=>1, :mtmbars_object_id=>2}
    @Foo.eager_graph(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT foos.id, foos.object_id, mtmbars.id AS mtmbars_id, mtmbars.object_id AS mtmbars_object_id FROM foos LEFT OUTER JOIN bars_foos ON (bars_foos.foo_id = foos.object_id) LEFT OUTER JOIN bars AS mtmbars ON (mtmbars.object_id = bars_foos.object_id)"]
  end

  it "should have working filter by associations with model instances" do
    @Bar.first(:foo=>@foo).should == @bar
    @db.sqls.should == ["SELECT * FROM bars WHERE (bars.object_id = 2) LIMIT 1"]
    @Foo.first(:bars=>@bar).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_id = 2) LIMIT 1"]
    @Foo.first(:bar=>@bar).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_id = 2) LIMIT 1"]
    @Foo.first(:mtmbars=>@bar).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_id IN (SELECT bars_foos.foo_id FROM bars_foos WHERE ((bars_foos.object_id = 2) AND (bars_foos.foo_id IS NOT NULL)))) LIMIT 1"]
    @Bar.first(:mtmfoos=>@foo).should == @bar
    @db.sqls.should == ["SELECT * FROM bars WHERE (bars.object_id IN (SELECT bars_foos.object_id FROM bars_foos WHERE ((bars_foos.foo_id = 2) AND (bars_foos.object_id IS NOT NULL)))) LIMIT 1"]
  end

  it "should have working modification methods" do
    b = @Bar.load(:id=>2, :object_id=>3)
    f = @Foo.load(:id=>2, :object_id=>3)
    @db.numrows = 1

    @bar.foo = f
    @bar.obj_id.should == 3
    @foo.bar = @bar
    @bar.obj_id.should == 2

    @foo.add_bar(b)
    @db.fetch = [[{:id=>1, :object_id=>2}, {:id=>2, :object_id=>2}], [{:id=>1, :object_id=>2}]]
    @foo.bars.should == [@bar, b]
    @foo.remove_bar(b)
    @foo.bars.should == [@bar]
    @foo.remove_all_bars
    @foo.bars.should == []

    @db.fetch = [[{:id=>1, :object_id=>2}], [], [{:id=>2, :object_id=>2}]]
    @bar = @Bar.load(:id=>1, :object_id=>2)
    @foo.mtmbars.should == [@bar]
    @foo.remove_all_mtmbars
    @foo.mtmbars.should == []
    @foo.add_mtmbar(b)
    @foo.mtmbars.should == [b]
    @foo.remove_mtmbar(b)
    @foo.mtmbars.should == []

    @db.fetch = [[{:id=>2, :object_id=>3}], [], [{:id=>2, :object_id=>3}]]
    @bar.add_mtmfoo(f)
    @bar.mtmfoos.should == [f]
    @bar.remove_all_mtmfoos
    @bar.mtmfoos.should == []
    @bar.add_mtmfoo(f)
    @bar.mtmfoos.should == [f]
    @bar.remove_mtmfoo(f)
    @bar.mtmfoos.should == []
  end
end 

describe "Sequel::Model Associations with non-column expression keys" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :object_ids=>[2]})
    @Foo = Class.new(Sequel::Model(@db[:foos]))
    @Bar = Class.new(Sequel::Model(@db[:bars]))
    @Foo.columns :id, :object_ids
    @Bar.columns :id, :object_ids
    m = Module.new{def obj_id; object_ids[0]; end}
    @Foo.include m
    @Bar.include m

    @Foo.one_to_many :bars, :primary_key=>:obj_id, :primary_key_column=>Sequel.subscript(:object_ids, 0), :key=>Sequel.subscript(:object_ids, 0), :key_method=>:obj_id,  :class=>@Bar
    @Foo.one_to_one :bar, :primary_key=>:obj_id, :primary_key_column=>Sequel.subscript(:object_ids, 0), :key=>Sequel.subscript(:object_ids, 0), :key_method=>:obj_id, :class=>@Bar
    @Bar.many_to_one :foo, :key=>:obj_id, :key_column=>Sequel.subscript(:object_ids, 0), :primary_key=>Sequel.subscript(:object_ids, 0), :primary_key_method=>:obj_id, :class=>@Foo
    @Foo.many_to_many :mtmbars, :join_table=>:bars_foos, :left_primary_key=>:obj_id, :left_primary_key_column=>Sequel.subscript(:object_ids, 0), :right_primary_key=>Sequel.subscript(:object_ids, 0), :right_primary_key_method=>:obj_id, :left_key=>Sequel.subscript(:foo_ids, 0), :right_key=>Sequel.subscript(:bar_ids, 0), :class=>@Bar
    @Bar.many_to_many :mtmfoos, :join_table=>:bars_foos, :left_primary_key=>:obj_id, :left_primary_key_column=>Sequel.subscript(:object_ids, 0), :right_primary_key=>Sequel.subscript(:object_ids, 0), :right_primary_key_method=>:obj_id, :left_key=>Sequel.subscript(:bar_ids, 0), :right_key=>Sequel.subscript(:foo_ids, 0), :class=>@Foo
    @foo = @Foo.load(:id=>1, :object_ids=>[2])
    @bar = @Bar.load(:id=>1, :object_ids=>[2])
    @db.sqls
  end

  it "should have working regular association methods" do
    @Bar.first.foo.should == @foo
    @db.sqls.should == ["SELECT * FROM bars LIMIT 1", "SELECT * FROM foos WHERE (foos.object_ids[0] = 2) LIMIT 1"]
    @Foo.first.bars.should == [@bar]
    @db.sqls.should == ["SELECT * FROM foos LIMIT 1", "SELECT * FROM bars WHERE (bars.object_ids[0] = 2)"]
    @Foo.first.bar.should == @bar
    @db.sqls.should == ["SELECT * FROM foos LIMIT 1", "SELECT * FROM bars WHERE (bars.object_ids[0] = 2) LIMIT 1"]
    @Foo.first.mtmbars.should == [@bar]
    @db.sqls.should == ["SELECT * FROM foos LIMIT 1", "SELECT bars.* FROM bars INNER JOIN bars_foos ON ((bars_foos.bar_ids[0] = bars.object_ids[0]) AND (bars_foos.foo_ids[0] = 2))"]
    @Bar.first.mtmfoos.should == [@foo]
    @db.sqls.should == ["SELECT * FROM bars LIMIT 1", "SELECT foos.* FROM foos INNER JOIN bars_foos ON ((bars_foos.foo_ids[0] = foos.object_ids[0]) AND (bars_foos.bar_ids[0] = 2))"]
  end

  it "should have working eager loading methods" do
    @Bar.eager(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @db.sqls.should == ["SELECT * FROM bars", "SELECT * FROM foos WHERE (foos.object_ids[0] IN (2))"]
    @Foo.eager(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT * FROM foos", "SELECT * FROM bars WHERE (bars.object_ids[0] IN (2))"]
    @Foo.eager(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @db.sqls.should == ["SELECT * FROM foos", "SELECT * FROM bars WHERE (bars.object_ids[0] IN (2))"]
    @db.fetch = [[{:id=>1, :object_ids=>[2]}], [{:id=>1, :object_ids=>[2], :x_foreign_key_x=>2}]]
    @Foo.eager(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT * FROM foos", "SELECT bars.*, bars_foos.foo_ids[0] AS x_foreign_key_x FROM bars INNER JOIN bars_foos ON ((bars_foos.bar_ids[0] = bars.object_ids[0]) AND (bars_foos.foo_ids[0] IN (2)))"]
    @db.fetch = [[{:id=>1, :object_ids=>[2]}], [{:id=>1, :object_ids=>[2], :x_foreign_key_x=>2}]]
    @Bar.eager(:mtmfoos).all.map{|o| [o, o.mtmfoos]}.should == [[@bar, [@foo]]]
    @db.sqls.should == ["SELECT * FROM bars", "SELECT foos.*, bars_foos.bar_ids[0] AS x_foreign_key_x FROM foos INNER JOIN bars_foos ON ((bars_foos.foo_ids[0] = foos.object_ids[0]) AND (bars_foos.bar_ids[0] IN (2)))"]
  end

  it "should have working eager graphing methods" do
    @db.fetch = {:id=>1, :object_ids=>[2], :foo_id=>1, :foo_object_ids=>[2]}
    @Bar.eager_graph(:foo).all.map{|o| [o, o.foo]}.should == [[@bar, @foo]]
    @db.sqls.should == ["SELECT bars.id, bars.object_ids, foo.id AS foo_id, foo.object_ids AS foo_object_ids FROM bars LEFT OUTER JOIN foos AS foo ON (foo.object_ids[0] = bars.object_ids[0])"]
    @db.fetch = {:id=>1, :object_ids=>[2], :bars_id=>1, :bars_object_ids=>[2]}
    @Foo.eager_graph(:bars).all.map{|o| [o, o.bars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT foos.id, foos.object_ids, bars.id AS bars_id, bars.object_ids AS bars_object_ids FROM foos LEFT OUTER JOIN bars ON (bars.object_ids[0] = foos.object_ids[0])"]
    @db.fetch = {:id=>1, :object_ids=>[2], :bar_id=>1, :bar_object_ids=>[2]}
    @Foo.eager_graph(:bar).all.map{|o| [o, o.bar]}.should == [[@foo, @bar]]
    @db.sqls.should == ["SELECT foos.id, foos.object_ids, bar.id AS bar_id, bar.object_ids AS bar_object_ids FROM foos LEFT OUTER JOIN bars AS bar ON (bar.object_ids[0] = foos.object_ids[0])"]
    @db.fetch = {:id=>1, :object_ids=>[2], :mtmfoos_id=>1, :mtmfoos_object_ids=>[2]}
    @Bar.eager_graph(:mtmfoos).all.map{|o| [o, o.mtmfoos]}.should == [[@bar, [@foo]]]
    @db.sqls.should == ["SELECT bars.id, bars.object_ids, mtmfoos.id AS mtmfoos_id, mtmfoos.object_ids AS mtmfoos_object_ids FROM bars LEFT OUTER JOIN bars_foos ON (bars_foos.bar_ids[0] = bars.object_ids[0]) LEFT OUTER JOIN foos AS mtmfoos ON (mtmfoos.object_ids[0] = bars_foos.foo_ids[0])"]
    @db.fetch = {:id=>1, :object_ids=>[2], :mtmbars_id=>1, :mtmbars_object_ids=>[2]}
    @Foo.eager_graph(:mtmbars).all.map{|o| [o, o.mtmbars]}.should == [[@foo, [@bar]]]
    @db.sqls.should == ["SELECT foos.id, foos.object_ids, mtmbars.id AS mtmbars_id, mtmbars.object_ids AS mtmbars_object_ids FROM foos LEFT OUTER JOIN bars_foos ON (bars_foos.foo_ids[0] = foos.object_ids[0]) LEFT OUTER JOIN bars AS mtmbars ON (mtmbars.object_ids[0] = bars_foos.bar_ids[0])"]
  end

  it "should have working filter by associations with model instances" do
    @Bar.first(:foo=>@foo).should == @bar
    @db.sqls.should == ["SELECT * FROM bars WHERE (bars.object_ids[0] = 2) LIMIT 1"]
    @Foo.first(:bars=>@bar).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] = 2) LIMIT 1"]
    @Foo.first(:bar=>@bar).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] = 2) LIMIT 1"]
    @Foo.first(:mtmbars=>@bar).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] IN (SELECT bars_foos.foo_ids[0] FROM bars_foos WHERE ((bars_foos.bar_ids[0] = 2) AND (bars_foos.foo_ids[0] IS NOT NULL)))) LIMIT 1"]
    @Bar.first(:mtmfoos=>@foo).should == @bar
    @db.sqls.should == ["SELECT * FROM bars WHERE (bars.object_ids[0] IN (SELECT bars_foos.bar_ids[0] FROM bars_foos WHERE ((bars_foos.foo_ids[0] = 2) AND (bars_foos.bar_ids[0] IS NOT NULL)))) LIMIT 1"]
  end

  it "should have working filter by associations with model datasets" do
    @Bar.first(:foo=>@Foo.where(:id=>@foo.id)).should == @bar
    @db.sqls.should == ["SELECT * FROM bars WHERE (bars.object_ids[0] IN (SELECT foos.object_ids[0] FROM foos WHERE ((id = 1) AND (foos.object_ids[0] IS NOT NULL)))) LIMIT 1"]
    @Foo.first(:bars=>@Bar.where(:id=>@bar.id)).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] IN (SELECT bars.object_ids[0] FROM bars WHERE ((id = 1) AND (bars.object_ids[0] IS NOT NULL)))) LIMIT 1"]
    @Foo.first(:bar=>@Bar.where(:id=>@bar.id)).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] IN (SELECT bars.object_ids[0] FROM bars WHERE ((id = 1) AND (bars.object_ids[0] IS NOT NULL)))) LIMIT 1"]
    @Foo.first(:mtmbars=>@Bar.where(:id=>@bar.id)).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] IN (SELECT bars_foos.foo_ids[0] FROM bars_foos WHERE ((bars_foos.bar_ids[0] IN (SELECT bars.object_ids[0] FROM bars WHERE ((id = 1) AND (bars.object_ids[0] IS NOT NULL)))) AND (bars_foos.foo_ids[0] IS NOT NULL)))) LIMIT 1"]
    @Bar.first(:mtmfoos=>@Foo.where(:id=>@foo.id)).should == @bar
    @db.sqls.should == ["SELECT * FROM bars WHERE (bars.object_ids[0] IN (SELECT bars_foos.bar_ids[0] FROM bars_foos WHERE ((bars_foos.foo_ids[0] IN (SELECT foos.object_ids[0] FROM foos WHERE ((id = 1) AND (foos.object_ids[0] IS NOT NULL)))) AND (bars_foos.bar_ids[0] IS NOT NULL)))) LIMIT 1"]
  end
end

describe "Model#pk_or_nil" do
  before do
    @m = Class.new(Sequel::Model)
    @m.columns :id, :x, :y
  end
  
  it "should be default return the value of the :id column" do
    m = @m.load(:id => 111, :x => 2, :y => 3)
    m.pk_or_nil.should == 111
  end

  it "should be return the primary key value for custom primary key" do
    @m.set_primary_key :x
    m = @m.load(:id => 111, :x => 2, :y => 3)
    m.pk_or_nil.should == 2
  end

  it "should be return the primary key value for composite primary key" do
    @m.set_primary_key [:y, :x]
    m = @m.load(:id => 111, :x => 2, :y => 3)
    m.pk_or_nil.should == [3, 2]
  end

  it "should raise if no primary key" do
    @m.set_primary_key nil
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_or_nil.should be_nil

    @m.no_primary_key
    m = @m.new(:id => 111, :x => 2, :y => 3)
    m.pk_or_nil.should be_nil
  end
end

describe Sequel::Model, "#refresh" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      unrestrict_primary_key
      columns :id, :x
    end
    MODEL_DB.reset
  end

  specify "should remove cached associations" do
    @c.many_to_one :node, :class=>@c
    @m = @c.new(:id => 555)
    @m.associations[:node] = 15
    @m.reload
    @m.associations.should == {}
  end
end

describe "Model#freeze" do
  before do
    class ::Album < Sequel::Model
      columns :id
      class B < Sequel::Model
        columns :id, :album_id
        many_to_one :album, :class=>Album
      end
      one_to_one :b, :key=>:album_id, :class=>B
    end
    @o = Album.load(:id=>1).freeze
    MODEL_DB.sqls
  end
  after do
    Object.send(:remove_const, :Album)
  end

  it "should freeze the object's associations" do
    @o.associations.frozen?.should be_true
  end

  it "should not break associations getters" do
    Album::B.dataset._fetch = {:album_id=>1, :id=>2}
    @o.b.should == Album::B.load(:id=>2, :album_id=>1)
    @o.associations[:b].should be_nil
  end

  it "should not break reciprocal associations" do
    b = Album::B.load(:id=>2, :album_id=>nil)
    b.album = @o
    @o.associations[:b].should be_nil
  end
end
