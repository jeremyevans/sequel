require File.join(File.dirname(__FILE__), 'spec_helper')

describe "Sequel::Model()" do
  specify "should raise Sequel::Error if sequel_model lib is not available" do
    module Kernel
      alias_method :orig_sq_require, :require
      def require(*args); raise LoadError; end
    end
    db = Sequel::Database.new
    Sequel::Model.instance_eval {@db = db}
    proc {Sequel::Model(:items)}.should raise_error(Sequel::Error)
    module Kernel
      alias_method :require, :orig_sq_require
    end
  end
  
  specify "should auto-load sequel_model and create a sequel model" do
    db = Sequel::Database.new
    Sequel::Model.instance_eval {@db = db}
    c = Class.new(Sequel::Model(:items))
    c.dataset.sql.should == "SELECT * FROM items"
  end
end