require File.join(File.dirname(__FILE__), "spec_helper")

DB = Sequel.sqlite

class StateModel < Sequel::Model(:items)
  columns [:id, :name, :state]

  is :state_machine, :field => :pos

end

describe StateModel do

  before(:all) do
    StateModel.create_table!

    StateModel.create :name => "one"
    StateModel.create :name => "two"
    StateModel.create :name => "three"
  end

  it "should start off in the initial state"

  it "should define the event transition methods"

  it "should define the states"
  
  it "should transition from one state to another when the transition method is called"
  
  it "should "
  
end