require File.join(File.dirname(__FILE__), "spec_helper")

# Should we take advantage of STI in databases that are capable?

describe Sequel::Plugins::Tree do

  it "should define a root method"
  
  it "should define a ancestors method"
  
  it "should define a parent method"
  
  it "should define a children method"
  
  it "should define a children? method"

  it "should define a descendants method"
  
  it "should define a descendants? method"

  it "should define a siblings method"

  it "should define a siblings? method"

  it "should define an insert method"

  it "should define an delete method"

  it "should define an leaf? method"

  it "should define an subtree method"

  it "should define an subtree? method"
  
end

describe Sequel::Plugins::Tree, "#root" do

  it "should return the root node"

end

describe Sequel::Plugins::Tree, "#parent" do

  it "should return the parent of the given node"

end

describe Sequel::Plugins::Tree, "#ancestors" do

  it "should return all the ancestors to the root of the given node"
  
end

describe Sequel::Plugins::Tree, "#children" do

  it "should return all the immediate children of the given node"
  
end

describe Sequel::Plugins::Tree, "#children?" do

  it "should return true if the given node has children"
  
  it "should return false if the given node has no children"
  
end

describe Sequel::Plugins::Tree, "#descendants" do
  
  it "should return all descendants of the given node"
  
end

describe Sequel::Plugins::Tree, "#descendants?" do
  
  it "should return true if the given node has descendants"
  
  it "should return false if the given node has no descendants"
  
end

describe Sequel::Plugins::Tree, "#grandchildren" do
  
  it "should return all grandchildren (childrens children) of the given node"
  
end

describe Sequel::Plugins::Tree, "#grandchildren?" do

  it "should return true if the given node has grandchildren"
  
  it "should return false if the given node has no grandchildren"
  
end

describe Sequel::Plugins::Tree, "#siblings" do
  
  it "should return all siblings of the current node"
  
end

describe Sequel::Plugins::Tree, "#siblings?" do

  it "should return true if the given node has siblings"
  
  it "should return false if the given node has no siblings"

end

describe Sequel::Plugins::Tree, "#insert" do
  
  it "should insert the given node into the tree at the specified position and return the node" do
    pending("This needs more thought.")
  end
    
end

describe Sequel::Plugins::Tree, "#delete" do

  it "should delete the given node from the tree"

end

describe Sequel::Plugins::Tree, "#leaf?" do

  it "should return true if the given node has siblings"
  
  it "should return false if the given node has no siblings"

end

describe Sequel::Plugins::Tree, "#subtree" do
  
  it "should return the subtree with the given node as the 'subtree root node'"

end

describe Sequel::Plugins::Tree, "#subtree?" do

  it "should return true if the given node has a subtree"
  
  it "should return false if the given node has no subtree"

end
