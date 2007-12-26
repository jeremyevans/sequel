require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Plugins::Versioned do

  it "should introduce a 'version' field into the schema"

  it "should allow for customization of the schema field"
  
  it "should create a new record with an incremented version number on update"

  it "should update an existing record when :update => false is used" do
    pending("Have to figure out the exact interface for this.")
  end
  
end
