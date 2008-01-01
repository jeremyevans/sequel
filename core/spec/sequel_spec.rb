require File.join(File.dirname(__FILE__), 'spec_helper')

describe "Sequel::Model()" do
  specify "should auto-load sequel_model and create a sequel model" do
    db = Sequel::Database.new
    Sequel::Model.instance_eval {@db = db}
    c = Class.new(Sequel::Model(:items))
    c.dataset.sql.should == "SELECT * FROM items"
  end
end