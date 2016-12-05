require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Sequel::Model basic support" do 
  before do
    @db = DB
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

  it ".find should return first matching item" do
    Item.all.must_equal []
    Item.find(:name=>'J').must_be_nil
    Item.create(:name=>'J')
    Item.find(:name=>'J').must_equal Item.load(:id=>1, :name=>'J')
  end
  
  it ".finder should create method that returns first matching item" do
    def Item.by_name(name) where(:name=>name) end
    Item.finder :by_name
    Item.first_by_name('J').must_be_nil
    Item.create(:name=>'J')
    Item.first_by_name('J').must_equal Item.load(:id=>1, :name=>'J')
    Item.first_by_name(['J', 'K']).must_equal Item.load(:id=>1, :name=>'J')
  end
  
  it ".prepared_finder should create method that returns first matching item" do
    def Item.by_name(name) where(:name=>name) end
    Item.prepared_finder :by_name
    Item.first_by_name('J').must_be_nil
    Item.create(:name=>'J')
    Item.first_by_name('J').must_equal Item.load(:id=>1, :name=>'J')
  end
  
  it ".find_or_create should return first matching item, or create it if it doesn't exist" do
    Item.all.must_equal []
    Item.find_or_create(:name=>'J').must_equal Item.load(:id=>1, :name=>'J')
    Item.all.must_equal [Item.load(:id=>1, :name=>'J')]
    Item.find_or_create(:name=>'J').must_equal Item.load(:id=>1, :name=>'J')
    Item.all.must_equal [Item.load(:id=>1, :name=>'J')]
  end

  it "should not raise an error if the implied database table doesn't exist " do
    class ::Item::Thing < Sequel::Model(@db)
      set_dataset :items
    end
    Item.create(:name=>'J')
    Item::Thing.first.must_equal Item::Thing.load(:id=>1, :name=>'J')
  end

  it "should create accessors for all table columns even if all dataset columns aren't selected" do
    c = Class.new(Sequel::Model(@db[:items].select(:id)))
    o = c.new
    o.name = 'A'
    o.save.must_equal c.load(:id=>1)
    c.select_map(:name).must_equal ['A']
  end

  it "should work correctly when a dataset restricts the colums it selects" do
    class ::Item::Thing < Sequel::Model(@db[:items].select(:name))
    end
    Item.create(:name=>'J')
    Item::Thing.first.must_equal Item::Thing.load(:name=>'J')
  end

  it "#delete should delete items correctly" do
    i = Item.create(:name=>'J')
    Item.count.must_equal 1
    i.delete
    Item.count.must_equal 0
  end

  it "#save should return nil if raise_on_save_failure is false and save isn't successful" do
    i = Item.new(:name=>'J')
    i.use_transactions = true
    def i.after_save
      raise Sequel::Rollback
    end
    i.save.must_be_nil
  end

  it "#should respect after_commit, after_rollback, after_destroy_commit, and after_destroy_rollback hooks" do
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
    i.save.must_be_nil
    i.reload.name.must_equal 'J'
    i.hooks.must_equal [:as, :ar]

    i.rb = true
    i.destroy.must_be_nil
    i.exists?.must_equal true
    i.hooks.must_equal [:ad, :adr]

    i.name = 'K'
    i.rb = false
    i.save.wont_equal nil
    i.reload.name.must_equal 'K'
    i.hooks.must_equal [:as, :ac]

    i.rb = false
    i.destroy.wont_equal nil
    i.exists?.must_equal false
    i.hooks.must_equal [:ad, :adc]
  end

  it "#exists? should return whether the item is still in the database" do
    i = Item.create(:name=>'J')
    i.exists?.must_equal true
    Item.dataset.delete
    i.exists?.must_equal false
  end

  it "#save should only update specified columns when saving" do
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :num
    end
    Item.dataset = Item.dataset
    i = Item.create(:name=>'J', :num=>1)
    Item.all.must_equal [Item.load(:id=>1, :name=>'J', :num=>1)]
    i.set(:name=>'K', :num=>2)
    i.save(:columns=>:name)
    Item.all.must_equal [Item.load(:id=>1, :name=>'K', :num=>1)]
    i.set(:name=>'L')
    i.save(:columns=>:num)
    Item.all.must_equal [Item.load(:id=>1, :name=>'K', :num=>2)]
  end
  
  it "#save should check that the only a single row is modified, unless require_modification is false" do
    i = Item.create(:name=>'a')
    i.require_modification = true
    i.delete
    proc{i.save}.must_raise(Sequel::NoExistingObject)
    proc{i.delete}.must_raise(Sequel::NoExistingObject)
    
    i.require_modification = false
    i.save
    i.delete
  end

  it ".to_hash should return a hash keyed on primary key if no argument provided" do
    Item.create(:name=>'J')
    Item.to_hash.must_equal(1=>Item.load(:id=>1, :name=>'J'))
  end
  
  it ".to_hash should return a hash keyed on argument if one argument provided" do
    Item.create(:name=>'J')
    Item.to_hash(:name).must_equal('J'=>Item.load(:id=>1, :name=>'J'))
  end
  
  it "should be marshallable before and after saving if marshallable! is called" do
    i = Item.new(:name=>'J')
    s = nil
    i2 = nil
    i.marshallable!
    s = Marshal.dump(i)
    i2 = Marshal.load(s)
    i2.must_equal i

    i.save
    i.marshallable!
    s = Marshal.dump(i)
    i2 = Marshal.load(s)
    i2.must_equal i

    i.save
    i.marshallable!
    s = Marshal.dump(i)
    i2 = Marshal.load(s)
    i2.must_equal i
  end
  
  it "#lock! should lock records" do
    Item.db.transaction do
      i = Item.create(:name=>'J')
      i.lock!
      i.update(:name=>'K')
    end
  end
end

describe "Sequel::Model with no existing table" do 
  it "should not raise an error when setting the dataset" do
    db = DB
    db.drop_table?(:items)
    class ::Item < Sequel::Model(db); end; Object.send(:remove_const, :Item)
    c = Class.new(Sequel::Model); c.set_dataset(db[:items])
    db.transaction do
      c = Class.new(Sequel::Model(db[:items]))
      db.get(Sequel.cast(1, Integer)).must_equal 1
    end
  end
end
