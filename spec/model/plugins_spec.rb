require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, ".plugin" do
  before do
    module Sequel::Plugins
      module Timestamped
        module InstanceMethods
          def get_stamp(*args); @values[:stamp] end
          def abc; 123; end
        end
        
        module ClassMethods
          def def; 234; end
        end

        module DatasetMethods
          def ghi; 345; end
        end
      end
    end
    @c = Class.new(Sequel::Model(:items))
    @t = Sequel::Plugins::Timestamped
  end
  after do
    Sequel::Plugins.send(:remove_const, :Timestamped)
  end
  
  it "should raise LoadError if the plugin is not found" do
    proc{@c.plugin :something_or_other}.should raise_error(LoadError)
  end
  
  it "should store the plugin in .plugins" do
    @c.plugins.should_not include(@t)
    @c.plugin @t
    @c.plugins.should include(@t)
  end
  
  it "should be inherited in subclasses" do
    @c.plugins.should_not include(@t)
    c1 = Class.new(@c)
    @c.plugin @t
    c2 = Class.new(@c)
    @c.plugins.should include(@t)
    c1.plugins.should_not include(@t)
    c2.plugins.should include(@t)
  end
  
  it "should accept a symbol and load the module from the Sequel::Plugins namespace" do
    @c.plugin :timestamped
    @c.plugins.should include(@t)
  end

  it "should accept a module" do
    m = Module.new
    @c.plugin m
    @c.plugins.should include(m)
  end

  it "should not attempt to load a plugin twice" do
    @c.plugins.should_not include(@t)
    @c.plugin @t
    @c.plugins.reject{|m| m != @t}.length.should == 1
    @c.plugin @t
    @c.plugins.reject{|m| m != @t}.length.should == 1
  end

  it "should call apply and configure if the plugin responds to it, with the args and block used" do
    m = Module.new do
      def self.args; @args; end
      def self.block; @block; end
      def self.block_call; @block.call; end
      def self.args2; @args2; end
      def self.block2; @block2; end
      def self.block2_call; @block2.call; end
      def self.apply(model, *args, &block)
        @args = args
        @block = block
        model.send(:define_method, :blah){43}
      end
      def self.configure(model, *args, &block)
        @args2 = args
        @block2 = block
        model.send(:define_method, :blag){44}
      end
    end
    b = lambda{42}
    @c.plugin(m, 123, 1=>2, &b)
    
    m.args.should == [123, {1=>2}]
    m.block.should == b
    m.block_call.should == 42
    @c.new.blah.should == 43
    
    m.args2.should == [123, {1=>2}]
    m.block2.should == b
    m.block2_call.should == 42
    @c.new.blag.should == 44
  end
  
  it "should call configure even if the plugin has already been loaded" do
    m = Module.new do
      @args = []
      def self.args; @args; end
      def self.configure(model, *args, &block)
        @args << [block, *args]
      end
    end
    
    b = lambda{42}
    @c.plugin(m, 123, 1=>2, &b)
    m.args.should == [[b, 123, {1=>2}]]
    
    b2 = lambda{44}
    @c.plugin(m, 234, 2=>3, &b2)
    m.args.should == [[b, 123, {1=>2}], [b2, 234, {2=>3}]]
  end
  
  it "should call things in the following order: apply, InstanceMethods, ClassMethods, DatasetMethods, configure" do
    m = Module.new do
      @args = []
      def self.args; @args; end
      def self.apply(model, *args, &block)
        @args << :apply
      end
      def self.configure(model, *args, &block)
        @args << :configure
      end
      im = Module.new do
        def self.included(model)
          model.plugins.last.args << :im
        end
      end
      cm = Module.new do
        def self.extended(model)
          model.plugins.last.args << :cm
        end
      end
      dm = Module.new do
        def self.extended(dataset)
          dataset.model.plugins.last.args << :dm
        end
      end
      const_set(:InstanceMethods, im)
      const_set(:ClassMethods, cm)
      const_set(:DatasetMethods, dm)
    end
    
    b = lambda{44}
    @c.plugin(m, 123, 1=>2, &b)
    m.args.should == [:apply, :im, :cm, :dm, :configure]
    @c.plugin(m, 234, 2=>3, &b)
    m.args.should == [:apply, :im, :cm, :dm, :configure, :configure]
  end

  it "should include an InstanceMethods module in the class if the plugin includes it" do
    @c.plugin @t
    m = @c.new
    m.should respond_to(:get_stamp)
    m.should respond_to(:abc)
    m.abc.should == 123
    t = Time.now
    m[:stamp] = t
    m.get_stamp.should == t
  end

  it "should extend the class with a ClassMethods module if the plugin includes it" do
    @c.plugin @t
    @c.def.should == 234
  end

  it "should extend the class's dataset with a DatasetMethods module if the plugin includes it" do
    @c.plugin @t
    @c.dataset.ghi.should == 345
    @c.ghi.should == 345
  end

  it "should save the DatasetMethods module and apply it later if the class doesn't have a dataset" do
    c = Class.new(Sequel::Model)
    c.plugin @t
    proc{c.ghi}.should raise_error(Sequel::Error)
    c.dataset = MODEL_DB[:i]
    c.dataset.ghi.should == 345
    c.ghi.should == 345
  end
  
  it "should save the DatasetMethods module and apply it later if the class has a dataset" do
    @c.plugin @t
    @c.dataset = MODEL_DB[:i]
    @c.dataset.ghi.should == 345
    @c.ghi.should == 345
  end

  it "should define class methods for all public instance methods in DatasetMethod" do
    m = Module.new do
      dm = Module.new do
        def a; 1; end
        def b; 2; end
      end
      const_set(:DatasetMethods, dm)
    end
    @c.plugin m
    @c.dataset.a.should == 1
    @c.dataset.b.should == 2
    @c.a.should == 1
    @c.b.should == 2
  end
  
  it "should define class methods for all public instance methods in DatasetMethod" do
    m = Module.new do
      dm = Module.new do
        def b; 2; end
        private
        def a; 1; end
      end
      const_set(:DatasetMethods, dm)
    end
    @c.plugin m
    @c.dataset.b.should == 2
    lambda{@c.dataset.a}.should raise_error(NoMethodError)
    @c.dataset.send(:a).should == 1
    @c.b.should == 2
    lambda{@c.a}.should raise_error(NoMethodError)
    lambda{@c.send(:a)}.should raise_error(NoMethodError)
  end

  it "should not raise an error if the DatasetMethod module has no public instance methods" do
    m = Module.new do
      dm = Module.new do
        private
        def a; 1; end
      end
      const_set(:DatasetMethods, dm)
    end
    lambda{@c.plugin m}.should_not raise_error
  end

  it "should not raise an error if plugin submodule names exist higher up in the namespace hierarchy" do
    class ::ClassMethods; end
    @c.plugin(m = Module.new)
    Object.send(:remove_const, :ClassMethods)
    @c.plugins.should include(m)

    class ::InstanceMethods; end
    @c.plugin(m = Module.new)
    Object.send(:remove_const, :InstanceMethods)
    @c.plugins.should include(m)

    class ::DatasetMethods; end
    @c.plugin(m = Module.new)
    Object.send(:remove_const, :DatasetMethods)
    @c.plugins.should include(m)
  end
end
