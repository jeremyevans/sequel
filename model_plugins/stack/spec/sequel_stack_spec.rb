require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Plugins::Stack do

  it "should do define a push method"
  
  it "should define a pop method"
  
  it "should define a empty? method"

  it "should define a peek method"
  
  it "should define a length method"
  
  it "should order elements via Last In First Out (LIFO)"

  it "should define a swap method"
  
end

describe Sequel::Plugins::Stack, "#empty?" do

  it "should return true if the stack has no elements"
  
  it "should return false if the stack has elements"
  
  it "should not modify the stack"

end

describe Sequel::Plugins::Stack, "#length" do

  it "should return the number of elements in the stack"

  it "should not modify the stack"

end

describe Sequel::Plugins::Stack, "#push" do

 it "should add the given element to the end of the stack"

 it "should returns the element it enstacks"

 it "should increment the length of the stack by 1"

end

describe Sequel::Plugins::Stack, "#pop" do

  it "should return nil if the stack is empty"  

  it "should return the last element from the stack"

  it "should remove the last element from the stack"

  it "should decrement the length of the stack by 1"

end

describe Sequel::Plugins::Stack, "#peek" do

  it "should return the last element"

  it "should not remove the last element"
  
end

describe Sequel::Plugins::Stack, "#swap" do

  it "should exchange the last two items on the stack."

  it "should not alter the length of the stack"
  
end
