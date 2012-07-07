require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Sequel::Model basic support" do
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items, :engine=>:InnoDB) do
      primary_key :id
      String :name
    end
    class ::Item < Sequel::Model(@db)
    end
  end
  after do
    @db.drop_table?(:items)
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

  specify "should create accessors for all table columns even if all dataset columns aren't selected" do
    c = Class.new(Sequel::Model(@db[:items].select(:id)))
    o = c.new
    o.name = 'A'
    o.save.should == c.load(:id=>1)
    c.select_map(:name).should == ['A']
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

  specify "#save should return nil if raise_on_save_failure is false and save isn't successful" do
    i = Item.new(:name=>'J')
    i.use_transactions = true
    def i.after_save
      raise Sequel::Rollback
    end
    i.save.should be_nil
  end

  specify "#should respect after_commit, after_rollback, after_destroy_commit, and after_destroy_rollback hooks" do
    i = Item.create(:name=>'J')
    i.use_transactions = true
    def i.hooks
      @hooks
    end
    def i.rb=(x)
      @hooks = []
      @rb = x
    end
    def i.after_save
      @hooks << :as
      raise Sequel::Rollback if @rb
    end
    def i.after_destroy
      @hooks << :ad
      raise Sequel::Rollback if @rb
    end
    def i.after_commit
      @hooks << :ac
    end
    def i.after_rollback
      @hooks << :ar
    end
    def i.after_destroy_commit
      @hooks << :adc
    end
    def i.after_destroy_rollback
      @hooks << :adr
    end
    i.name = 'K'
    i.rb = true
    i.save.should be_nil
    i.reload.name.should == 'J'
    i.hooks.should == [:as, :ar]

    i.rb = true
    i.destroy.should be_nil
    i.exists?.should be_true
    i.hooks.should == [:ad, :adr]

    i.name = 'K'
    i.rb = false
    i.save.should_not be_nil
    i.reload.name.should == 'K'
    i.hooks.should == [:as, :ac]

    i.rb = false
    i.destroy.should_not be_nil
    i.exists?.should be_false
    i.hooks.should == [:ad, :adc]
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

  specify "#save should check that the only a single row is modified, unless require_modification is false" do
    i = Item.create(:name=>'a')
    i.require_modification = true
    i.delete
    proc{i.save}.should raise_error(Sequel::NoExistingObject)
    proc{i.delete}.should raise_error(Sequel::NoExistingObject)

    i.require_modification = false
    i.save
    i.delete
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

  specify "#lock! should lock records" do
    Item.db.transaction do
      i = Item.create(:name=>'J')
      i.lock!
      i.update(:name=>'K')
    end
  end
end

describe "Sequel::Model with no existing table" do
  specify "should not raise an error when setting the dataset" do
    db = INTEGRATION_DB
    db.drop_table?(:items)
    proc{class ::Item < Sequel::Model(db); end; Object.send(:remove_const, :Item)}.should_not raise_error
    proc{c = Class.new(Sequel::Model); c.set_dataset(db[:items])}.should_not raise_error
  end
end
