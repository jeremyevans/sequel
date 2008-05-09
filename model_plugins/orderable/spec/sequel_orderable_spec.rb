require File.join(File.dirname(__FILE__), "spec_helper")

DB = Sequel.sqlite
 
class Item < Sequel::Model(:items)

  set_schema do
    primary_key :id
    varchar :name
    int :pos
  end

  is :orderable, :field => :pos

end

describe Item do
  before(:all) {
    Item.create_table!
 
    Item.create :name => "one",   :pos => 3
    Item.create :name => "two",   :pos => 2
    Item.create :name => "three", :pos => 1
  }
 
  it "should return rows in order of position" do
    Item.map(&:pos).should == [1,2,3]
    Item.map(&:name).should == %w[ three two one ]
  end
 
  it "should define prev and next" do
    i = Item[:name => "two"]
    i.prev.should == Item[:name => "three"]
    i.next.should == Item[:name => "one"]
  end
 
  it "should define move_to" do
    Item[:name => "two"].move_to(1)
    Item.map(&:name).should == %w[ two three one ]
 
    Item[:name => "two"].move_to(3)
    Item.map(&:name).should == %w[ three one two ]
  end
 
  it "should define move_to_top and move_to_bottom" do
    Item[:name => "two"].move_to_top
    Item.map(&:name).should == %w[ two three one ]
 
    Item[:name => "two"].move_to_bottom
    Item.map(&:name).should == %w[ three one two ]
  end
 
  it "should define move_up and move_down" do
    Item[:name => "one"].move_up
    Item.map(&:name).should == %w[ one three two ]
 
    Item[:name => "three"].move_down
    Item.map(&:name).should == %w[ one two three ]
  end
  
end
 
class ListItem < Sequel::Model(:list_items)

  set_schema do
    primary_key :id
    int :list_id
    varchar :name
    int :position
  end

  is :orderable, :scope => :list_id

end
 
describe ListItem do
  
  before(:all) {
    ListItem.create_table!
 
    ListItem.create :name => "a", :list_id => 1, :position => 3
    ListItem.create :name => "b", :list_id => 1, :position => 2
    ListItem.create :name => "c", :list_id => 1, :position => 1
 
    ListItem.create :name => "d", :list_id => 2, :position => 1
    ListItem.create :name => "e", :list_id => 2, :position => 2
    ListItem.create :name => "f", :list_id => 2, :position => 3
  }
 
  it "should print in order with scope provided" do
    ListItem.map(&:name).should == %w[ c b a d e f ]
  end
 
  it "should fetch prev and next records with scope" do
    b = ListItem[:name => "b"]
    b.next.name.should == "a"
    b.prev.name.should == "c"
    b.next.next.should be_nil
    b.prev.prev.should be_nil
 
    e = ListItem[:name => "e"]
    e.next.name.should == "f"
    e.prev.name.should == "d"
    e.next.next.should be_nil
    e.prev.prev.should be_nil
  end
 
  it "should move only within the scope provided" do
    ListItem[:name => "b"].move_to_top
    ListItem.map(&:name).should == %w[ b c a d e f ]
 
    ListItem[:name => "c"].move_to_bottom
    ListItem.map(&:name).should == %w[ b a c d e f ]
  end
  
end
