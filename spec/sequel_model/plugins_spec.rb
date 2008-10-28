require File.join(File.dirname(__FILE__), "spec_helper")

module Sequel::Plugins

  module Timestamped
    def self.apply(m, opts)
      m.class_def(:get_stamp) {@values[:stamp]}
      m.meta_def(:stamp_opts) {opts}
      m.before_save {@values[:stamp] = Time.now}
    end
    
    module InstanceMethods
      def abc; timestamped_opts; end
    end
    
    module ClassMethods
      def deff; timestamped_opts; end
    end

    module DatasetMethods
      def ghi; timestamped_opts; end
    end
  end

end

describe Sequel::Model, "using a plugin" do

  it "should fail if the plugin is not found" do
    proc do
      c = Class.new(Sequel::Model)
      c.class_eval do
        is :something_or_other
      end
    end.should raise_error(LoadError)
  end
  
  it "should apply the plugin to the class" do
    c = nil
    proc do
      c = Class.new(Sequel::Model)
      c.class_eval do
        set_dataset MODEL_DB[:items]
        is :timestamped, :a => 1, :b => 2
      end
    end.should_not raise_error(LoadError)
    
    c.should respond_to(:stamp_opts)
    c.stamp_opts.should == {:a => 1, :b => 2}
    
    # instance methods
    m = c.new
    m.should respond_to(:get_stamp)
    m.should respond_to(:abc)
    m.abc.should == {:a => 1, :b => 2}
    t = Time.now
    m[:stamp] = t
    m.get_stamp.should == t
    
    # class methods
    c.should respond_to(:deff)
    c.deff.should == {:a => 1, :b => 2}
    
    # dataset methods
    c.dataset.should respond_to(:ghi)
    c.dataset.ghi.should == {:a => 1, :b => 2}

    # dataset methods called on the class
    c.should respond_to(:ghi)
    c.ghi.should == {:a => 1, :b => 2}
  end
  
  it "should fail to apply if the plugin has DatasetMethod and the model has no datset" do
    proc do
      Class.new(Sequel::Model) do
        is :timestamped, :a => 1, :b => 2
      end
    end.should raise_error(Sequel::Error)
  end
end
