require File.join(File.dirname(__FILE__), "../lib/sequel_taggable")

# Assumptions
# DB has a tags table name:string
# DB has a taggings table tagged:string tagged_id:integer

describe Sequel::Plugins::Taggable do

  it "should add a tag method"

  it "should add a tags method"
  
  it "should allow specification of a delimiter string"
  
  it "should allow specification of a delimiter regex :delimiter => /[\\s|,]+/"
  
  it "should have a default delimiter of /\\s|,/"

end

describe Sequel::Plugins::Taggable, "#tag" do
  
  it "should add tags to the element from the given string, split based on the delimiter"
  
end

describe Sequel::Plugins::Taggable, "#tags" do

  it "should return all taggings on the current object"
  
end
