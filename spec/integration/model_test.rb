require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe "Sequel::Model basic support" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      String :name
    end
    class ::Item < Sequel::Model(@db)
    end
  end
  after do
    @db.drop_table(:items)
    Object.send(:remove_const, :Item)
  end

  specify ".find should return first matching item" do
    Item.all.should == []
    Item.find(:name=>'J').should == nil
    Item.create(:name=>'J')
    Item.find(:name=>'J').should == Item.load(:id=>1, :name=>'J')
  end
  
  specify ".find_or_create should return first matching item, or create it if it doesn't exist" do
    Item.all.should == []
    Item.find_or_create(:name=>'J').should == Item.load(:id=>1, :name=>'J')
    Item.all.should == [Item.load(:id=>1, :name=>'J')]
    Item.find_or_create(:name=>'J').should == Item.load(:id=>1, :name=>'J')
    Item.all.should == [Item.load(:id=>1, :name=>'J')]
  end

  specify "should not raise an error if the implied database table doesn't exist " do
    class ::Item::Thing < Sequel::Model(@db)
      set_dataset :items
    end
    Item.create(:name=>'J')
    Item::Thing.first.should == Item::Thing.load(:id=>1, :name=>'J')
  end

  specify "should work correctly when a dataset restricts the colums it selects" do
    class ::Item::Thing < Sequel::Model(@db[:items].select(:name))
    end
    Item.create(:name=>'J')
    Item::Thing.first.should == Item::Thing.load(:name=>'J')
  end

  specify "#delete should delete items correctly" do
    i = Item.create(:name=>'J')
    Item.count.should == 1
    i.delete
    Item.count.should == 0
  end

  specify "#exists? should return whether the item is still in the database" do
    i = Item.create(:name=>'J')
    i.exists?.should == true
    Item.delete
    i.exists?.should == false
  end

  specify "#save should only update specified columns when saving" do
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :num
    end
    Item.dataset = Item.dataset
    i = Item.create(:name=>'J', :num=>1)
    Item.all.should == [Item.load(:id=>1, :name=>'J', :num=>1)]
    i.set(:name=>'K', :num=>2)
    i.save(:name)
    Item.all.should == [Item.load(:id=>1, :name=>'K', :num=>1)]
    i.set(:name=>'L')
    i.save(:num)
    Item.all.should == [Item.load(:id=>1, :name=>'K', :num=>2)]
  end

  specify ".to_hash should return a hash keyed on primary key if no argument provided" do
    i = Item.create(:name=>'J')
    Item.to_hash.should == {1=>Item.load(:id=>1, :name=>'J')}
  end
  
  specify ".to_hash should return a hash keyed on argument if one argument provided" do
    i = Item.create(:name=>'J')
    Item.to_hash(:name).should == {'J'=>Item.load(:id=>1, :name=>'J')}
  end
  
  specify "should be marshallable before and after saving if marshallable! is called" do
    i = Item.new(:name=>'J')
    s = nil
    i2 = nil
    i.marshallable!
    proc{s = Marshal.dump(i)}.should_not raise_error
    proc{i2 = Marshal.load(s)}.should_not raise_error
    i2.should == i

    i.save
    i.marshallable!
    proc{s = Marshal.dump(i)}.should_not raise_error
    proc{i2 = Marshal.load(s)}.should_not raise_error
    i2.should == i

    i.save
    i.marshallable!
    proc{s = Marshal.dump(i)}.should_not raise_error
    proc{i2 = Marshal.load(s)}.should_not raise_error
    i2.should == i
  end
end
