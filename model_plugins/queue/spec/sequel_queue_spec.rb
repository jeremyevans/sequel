require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Plugins::Queue do

  it "should do define a push method"
  
  it "should define a pop method"
  
  it "should define a empty? method"

  it "should define a peek method"
  
  it "should define a length method"
  
  it "should order elements via first-in-first-out (FIFO)"
  
end

describe Sequel::Plugins::Queue, "#empty?" do

  it "should return true if the queue has no elements"
  
  it "should return false if the queue has elements"
  
  it "should not modify the queue"

end

describe Sequel::Plugins::Queue, "#length" do

  it "should return the number of elements in the queue"

  it "should not modify the queue"

end

describe Sequel::Plugins::Queue, "#push" do

 it "should add the given element to the end of the queue"

 it "should returns the element it enqueues"

 it "should increment the length of the queue by 1"

end

describe Sequel::Plugins::Queue, "#pop" do

  it "should return nil if the queue is empty"  

  it "should return the first element from the queue"

  it "should remove the first element from the queue"

  it "should decrement the length of the queue by 1"

end

describe Sequel::Plugins::Queue, "#peek" do

  it "should return the first element"

  it "should not remove the first element"
  
end
