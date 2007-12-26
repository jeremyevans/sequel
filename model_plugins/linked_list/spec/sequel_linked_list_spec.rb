require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Plugins::LinkedList do

  it "should introduce a 'next' field into the schema"

  it "should allow for custom 'next' field specification via :next => 'field_name'"

  it "should introduce a 'previous' field into the schema"

  it "should allow for custom 'previous' field specification via :previous => 'field_name'"

  it "should define a prevous method"

  it "should define an previous?"

  it "should define a next method"

  it "should define an next?"

  it "should define a first method"

  it "should define an first?"

  it "should define a last method"

  it "should define an last?"

  it "should define an insert_before method"

  it "should define an insert_after method"

  it "should define an empty?"
  
end

describe Sequel::Plugins::LinkedList, "#next" do

  it "should return the successor of the given node"
  
  it "should return nil if the given node has no successor"

end

describe Sequel::Plugins::LinkedList, "#next?" do

  it "should return true if the given node has a successor"

  it "should return false if the given node has no successor"
  
end

describe Sequel::Plugins::LinkedList, "#previous" do

  it "should return the predecessor of the given node"

  it "should return nil if the given node has no predecessor"

end

describe Sequel::Plugins::LinkedList, "#previous?" do

  it "should return true if the given node has a predecessor"

  it "should return false if the given node has no predecessor"

end

describe Sequel::Plugins::LinkedList, "#first" do

  it "should return the first node if the list is non-empty"

  it "should return nil if the list is empty"

end

describe Sequel::Plugins::LinkedList, "#first?" do

  it "should return true if the list is non-empty and the given node is the first node"

  it "should return false if the list is empty"

end

describe Sequel::Plugins::LinkedList, "#last" do

  it "should return the last node if the list is non-empty"

  it "should return nil if the list is empty"

end

describe Sequel::Plugins::LinkedList, "#last?" do

  it "should return true if the list is non-empty and the given node is the last node"

  it "should return false if the list is empty"

end

describe Sequel::Plugins::LinkedList, "#empty?" do

  it "should return true if the list has no nodes"

  it "should return false if the list has nodes"

end

describe Sequel::Plugins::LinkedList, "#insert_before" do

  it "should insert the given item into the list before the given node (self)"

end

describe Sequel::Plugins::LinkedList, "#insert_after" do

  it "should insert the given item into the list after the given node (self)"

end
