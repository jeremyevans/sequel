require_relative "spec_helper"

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
    proc{@c.plugin :something_or_other}.must_raise(LoadError)
  end
  
  it "should store the plugin in .plugins" do
    @c.plugins.wont_include(@t)
    @c.plugin @t
    @c.plugins.must_include(@t)
  end
  
  it "should be inherited in subclasses" do
    @c.plugins.wont_include(@t)
    c1 = Class.new(@c)
    @c.plugin @t
    c2 = Class.new(@c)
    @c.plugins.must_include(@t)
    c1.plugins.wont_include(@t)
    c2.plugins.must_include(@t)
  end
  
  it "should accept a symbol and load the module from the Sequel::Plugins namespace" do
    @c.plugin :timestamped
    @c.plugins.must_include(@t)
  end

  it "should accept a module" do
    m = Module.new
    @c.plugin m
    @c.plugins.must_include(m)
  end

  it "should not attempt to load a plugin twice" do
    @c.plugins.wont_include(@t)
    @c.plugin @t
    @c.plugins.reject{|m| m != @t}.length.must_equal 1
    @c.plugin @t
    @c.plugins.reject{|m| m != @t}.length.must_equal 1
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
    
    m.args.must_equal [123, {1=>2}]
    m.block.must_equal b
    m.block_call.must_equal 42
    @c.new.blah.must_equal 43
    
    m.args2.must_equal [123, {1=>2}]
    m.block2.must_equal b
    m.block2_call.must_equal 42
    @c.new.blag.must_equal 44
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
    m.args.must_equal [[b, 123, {1=>2}]]
    
    b2 = lambda{44}
    @c.plugin(m, 234, 2=>3, &b2)
    m.args.must_equal [[b, 123, {1=>2}], [b2, 234, {2=>3}]]
  end
  
  it "should call things in the following order: apply, ClassMethods, InstanceMethods, DatasetMethods, configure" do
    m = Module.new do
      @args = []
      def self.args; @args; end
      def self.apply(model, *args, &block)
        @args << :apply
      end
      def self.configure(model, *args, &block)
        @args << :configure
      end
      self::InstanceMethods = Module.new do
        def self.included(model)
          model.plugins.last.args << :im
        end
      end
      self::ClassMethods = Module.new do
        def self.extended(model)
          model.plugins.last.args << :cm
        end
      end
      self::DatasetMethods = Module.new do
        def self.extended(dataset)
          dataset.model.plugins.last.args << :dm
        end
      end
    end
    
    b = lambda{44}
    @c.plugin(m, 123, 1=>2, &b)
    m.args.must_equal [:apply, :cm, :im, :dm, :configure]
    @c.plugin(m, 234, 2=>3, &b)
    m.args.must_equal [:apply, :cm, :im, :dm, :configure, :configure]
  end

  it "should include an InstanceMethods module in the class if the plugin includes it" do
    @c.plugin @t
    m = @c.new
    m.must_respond_to(:get_stamp)
    m.must_respond_to(:abc)
    m.abc.must_equal 123
    t = Time.now
    m[:stamp] = t
    m.get_stamp.must_equal t
  end

  it "should extend the class with a ClassMethods module if the plugin includes it" do
    @c.plugin @t
    @c.def.must_equal 234
  end

  it "should extend the class's dataset with a DatasetMethods module if the plugin includes it" do
    @c.plugin @t
    @c.dataset.ghi.must_equal 345
  end

  it "should save the DatasetMethods module and apply it later if the class doesn't have a dataset" do
    c = Class.new(Sequel::Model)
    c.plugin @t
    c.dataset = DB[:i]
    c.dataset.ghi.must_equal 345
  end
  
  it "should save the DatasetMethods module and apply it later if the class has a dataset" do
    @c.plugin @t
    @c.dataset = DB[:i]
    @c.dataset.ghi.must_equal 345
  end

  it "should not define class methods for private instance methods in DatasetMethod" do
    m = Module.new do
      self::DatasetMethods = Module.new do
        def b; 2; end
        private
        def a; 1; end
      end
    end
    @c.plugin m
    @c.dataset.b.must_equal 2
    lambda{@c.dataset.a}.must_raise(NoMethodError)
    @c.dataset.send(:a).must_equal 1
    lambda{@c.a}.must_raise(NoMethodError)
    lambda{@c.send(:a)}.must_raise(NoMethodError)
  end

  it "should not raise an error if the DatasetMethod module has no public instance methods" do
    m = Module.new do
      self::DatasetMethods = Module.new do
        private
        def a; 1; end
      end
    end
    @c.plugin m
  end

  it "should not raise an error if plugin submodule names exist higher up in the namespace hierarchy" do
    class ::ClassMethods; end
    @c.plugin(m = Module.new)
    Object.send(:remove_const, :ClassMethods)
    @c.plugins.must_include(m)

    class ::InstanceMethods; end
    @c.plugin(m = Module.new)
    Object.send(:remove_const, :InstanceMethods)
    @c.plugins.must_include(m)

    class ::DatasetMethods; end
    @c.plugin(m = Module.new)
    Object.send(:remove_const, :DatasetMethods)
    @c.plugins.must_include(m)
  end
end

describe Sequel::Plugins do
  before do
    @c = Class.new(Sequel::Model(:items))
  end
  
  it "should have def_dataset_methods define methods that call methods on the dataset" do
    m = Module.new do
      module self::ClassMethods
        Sequel::Plugins.def_dataset_methods(self, :one)
      end
      module self::DatasetMethods
        def one
          1
        end
      end
    end
    @c.plugin m
    @c.one.must_equal 1
  end
  
  it "should have def_dataset_methods accept an array with multiple methods" do
    m = Module.new do
      module self::ClassMethods
        Sequel::Plugins.def_dataset_methods(self, [:one, :two])
      end
      module self::DatasetMethods
        def one
          1
        end
        def two 
          2
        end
      end
    end
    @c.plugin m
    @c.one.must_equal 1
    @c.two.must_equal 2
  end

  it "should have inherited_instance_variables add instance variables to copy into the subclass" do
    m = Module.new do
      def self.apply(model)
        model.instance_variable_set(:@one, 1)
      end
      module self::ClassMethods
        attr_reader :one
        Sequel::Plugins.inherited_instance_variables(self, :@one=>nil)
      end
    end
    @c.plugin m
    Class.new(@c).one.must_equal 1
  end
  
  it "should have after_set_dataset add a method to call after set_dataset" do
    m = Module.new do
      module self::ClassMethods
        Sequel::Plugins.after_set_dataset(self, :one)
        def foo; dataset.send(:cache_get, :foo) end
        private
        def one; dataset.send(:cache_set, :foo, 1) end
      end
    end
    @c.plugin m
    @c.foo.must_be_nil
    @c.set_dataset :blah
    @c.foo.must_equal 1
  end
end

describe "Sequel::Model.plugin" do
  before do
    @c = Class.new(Sequel::Model)
  end
  after do
    Sequel::Plugins.send(:remove_const, :SomethingOrOther)
  end

  it "should try loading plugins from sequel/plugins/:plugin" do
    a = []
    m = Module.new
    @c.define_singleton_method(:require) do |b|
      a << b
      Sequel::Plugins.const_set(:SomethingOrOther, m)
    end
    @c.plugin :something_or_other
    @c.plugins.must_include m
    a.must_equal ['sequel/plugins/something_or_other']
  end
end
