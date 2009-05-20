require File.join(File.dirname(__FILE__), "spec_helper")

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

  it "should exit if the plugin is already loaded" do
    @c.plugins.should_not include(@t)
    @c.plugin @t
    @c.plugins.reject{|m| m != @t}.length.should == 1
    @c.plugin @t
    @c.plugins.reject{|m| m != @t}.length.should == 1
  end

  it "should call apply if the plugin responds to it, with the args and block used" do
    m = Module.new do
      def self.args; @args; end
      def self.block; @block; end
      def self.block_call; @block.call; end
      def self.apply(model, *args, &block)
        @args = args
        @block = block
        model.send(:define_method, :blah){43}
      end
    end
    b = lambda{42}
    @c.plugin(m, 123, 1=>2, &b)
    m.args.should == [123, {1=>2}]
    m.block.should == b
    m.block_call.should == 42
    @c.new.blah.should == 43
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

  it "should define a plugin_opts instance method if the plugin has an InstanceMethods module" do
    @c.plugin :timestamped, 1, 2=>3
    @c.new.timestamped_opts.should == [1, {2=>3}]
  end

  it "should extend the class with a ClassMethods module if the plugin includes it" do
    @c.plugin @t
    @c.def.should == 234
  end

  it "should define a plugin_opts class method if the plugin has a ClassMethods module" do
    @c.plugin :timestamped, 1, 2=>3
    @c.timestamped_opts.should == [1, {2=>3}]
  end

  it "should extend the class's dataset with a DatasetMethods module if the plugin includes it" do
    @c.plugin @t
    @c.dataset.ghi.should == 345
    @c.ghi.should == 345
  end

  it "should define a plugin_opts dataset method if the plugin has a DatasetMethods module" do
    @c.plugin :timestamped, 1, 2=>3
    @c.dataset.timestamped_opts.should == [1, {2=>3}]
  end
  
  it "should use a single arg for the plugin_opts method if only a single arg was given" do
    @c.plugin :timestamped, 1
    @c.new.timestamped_opts.should == 1
    @c.timestamped_opts.should == 1
    @c.dataset.timestamped_opts.should == 1
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
end
