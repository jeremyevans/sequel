require File.join(File.dirname(__FILE__), 'spec_helper')

context "Migration classes" do
  before do
    Sequel::Migration.descendants.clear
  end

  specify "should be registred in Migration.descendants" do
    @class = Class.new(Sequel::Migration)
    
    Sequel::Migration.descendants.should == [@class]
  end
  
  specify "should be registered in the right order" do
    @c1 = Class.new(Sequel::Migration)
    @c2 = Class.new(Sequel::Migration)
    @c3 = Class.new(Sequel::Migration)
    
    Sequel::Migration.descendants.should == [@c1, @c2, @c3]
  end
end

context "Migration#apply" do
  before do
    @c = Class.new do
      define_method(:one) {|x| [1111, x]}
      define_method(:two) {|x| [2222, x]}
    end
    @db = @c.new
    
    @migration = Class.new(Sequel::Migration) do
      define_method(:up) {one(3333)}
      define_method(:down) {two(4444)}
    end
  end
  
  specify "should raise for an invalid direction" do
    proc {@migration.apply(@db, :hahaha)}.should raise_error(ArgumentError)
  end
  
  specify "should apply the up direction correctly" do
    @migration.apply(@db, :up).should == [1111, 3333]
  end

  specify "should apply the down direction correctly" do
    @migration.apply(@db, :down).should == [2222, 4444]
  end
end

class DummyMigrationDataset
  attr_reader :from
  
  def initialize(x); @from = x; end
  
  @@version = nil
  
  def version; @@version; end
  def version=(x); @@version = x; end
  def first; @@version ? {:version => @@version} : nil; end
  def update(h); @@version = h[:version]; end
  def <<(h); @@version = h[:version]; end
end

class DummyMigrationDB
  attr_reader :creates, :drops, :table_created
  
  def initialize
    @creates = []
    @drops = []
  end
  
  def create(x); @creates << x; end
  def drop(x); @drops << x; end
  
  def [](x); DummyMigrationDataset.new(x); end
  
  def create_table(x); raise if @table_created == x; @table_created = x; end
  def table_exists?(x); @table_created == x; end
  
  def transaction; yield; end
end

MIGRATION_001 = %[
  class CreateSessions < Sequel::Migration
    def up
      create(1111)
    end
    
    def down
      drop(1111)
    end
  end
]

MIGRATION_002 = %[
  class CreateNodes < Sequel::Migration
    def up
      create(2222)
    end
    
    def down
      drop(2222)
    end
  end
]

MIGRATION_003 = %[
  class CreateUsers < Sequel::Migration
    def up
      create(3333)
    end
    
    def down
      drop(3333)
    end
  end
]

MIGRATION_005 = %[
  class CreateAttributes < Sequel::Migration
    def up
      create(5555)
    end
    
    def down
      drop(5555)
    end
  end
]

context "Sequel::Migrator" do
  before do
    @db = DummyMigrationDB.new
    
    File.open('001_create_sessions.rb', 'w') {|f| f << MIGRATION_001}
    File.open('002_create_nodes.rb', 'w') {|f| f << MIGRATION_002}
    File.open('003_create_users.rb', 'w') {|f| f << MIGRATION_003}
    File.open('005_5_create_attributes.rb', 'w') {|f| f << MIGRATION_005}
    
    @db[:schema_info].version = nil
  end
  
  after do
    Object.send(:remove_const, "CreateSessions") if Object.const_defined?("CreateSessions")
    Object.send(:remove_const, "CreateNodes") if Object.const_defined?("CreateNodes")
    Object.send(:remove_const, "CreateUsers") if Object.const_defined?("CreateUsers")
    Object.send(:remove_const, "CreateAttributes") if Object.const_defined?("CreateAttributes")

    File.delete('001_create_sessions.rb')
    File.delete('002_create_nodes.rb')
    File.delete('003_create_users.rb')
    File.delete('005_5_create_attributes.rb')
  end
  
  specify "should return the list of files for a specified version range" do
    Sequel::Migrator.migration_files('.', 1..1).should == \
      ['./001_create_sessions.rb']

    Sequel::Migrator.migration_files('.', 1..3).should == \
      ['./001_create_sessions.rb', './002_create_nodes.rb', './003_create_users.rb']

    Sequel::Migrator.migration_files('.', 3..6).should == \
      ['./003_create_users.rb', './005_5_create_attributes.rb']
      
    Sequel::Migrator.migration_files('.', 7..8).should == []
  end
  
  specify "should return the latest version available" do
    Sequel::Migrator.latest_migration_version('.').should == 5
  end
  
  specify "should load the migration classes for the specified range" do
    Sequel::Migrator.migration_classes('.', 3, 0, :up).should == \
      [CreateSessions, CreateNodes, CreateUsers]
  end
  
  specify "should load the migration classes for the specified range" do
    Sequel::Migrator.migration_classes('.', 0, 5, :down).should == \
      [CreateAttributes, CreateUsers, CreateNodes, CreateSessions]
  end
  
  specify "should start from current + 1 for the up direction" do
    Sequel::Migrator.migration_classes('.', 3, 1, :up).should == \
      [CreateNodes, CreateUsers]
  end
  
  specify "should end on current + 1 for the down direction" do
    Sequel::Migrator.migration_classes('.', 2, 5, :down).should == \
      [CreateAttributes, CreateUsers]
  end
  
  specify "should automatically create the schema_info table" do
    @db.table_exists?(:schema_info).should be_false
    Sequel::Migrator.schema_info_dataset(@db)
    @db.table_exists?(:schema_info).should be_true
    
    # should not raise if table already exists
    proc {Sequel::Migrator.schema_info_dataset(@db)}.should_not raise_error
  end
  
  specify "should return a dataset for the schema_info table" do
    d = Sequel::Migrator.schema_info_dataset(@db)
    d.from.should == :schema_info
  end
  
  specify "should get the migration version stored in the database" do
    # default is 0
    Sequel::Migrator.get_current_migration_version(@db).should == 0
    
    Sequel::Migrator.schema_info_dataset(@db) << {:version => 4321}

    Sequel::Migrator.get_current_migration_version(@db).should == 4321
  end
  
  specify "should set the migration version stored in the database" do
    Sequel::Migrator.get_current_migration_version(@db).should == 0
    Sequel::Migrator.set_current_migration_version(@db, 6666)
    Sequel::Migrator.get_current_migration_version(@db).should == 6666
  end
  
  specify "should apply migrations correctly in the up direction" do
    Sequel::Migrator.apply(@db, '.', 3, 2)
    @db.creates.should == [3333]
    
    Sequel::Migrator.get_current_migration_version(@db).should == 3

    Sequel::Migrator.apply(@db, '.', 5)
    @db.creates.should == [3333, 5555]

    Sequel::Migrator.get_current_migration_version(@db).should == 5
  end
  
  specify "should apply migrations correctly in the down direction" do
    Sequel::Migrator.apply(@db, '.', 1, 5)
    @db.drops.should == [5555, 3333, 2222]

    Sequel::Migrator.get_current_migration_version(@db).should == 1
  end

  specify "should apply migrations up to the latest version if no target is given" do
    Sequel::Migrator.apply(@db, '.')
    @db.creates.should == [1111, 2222, 3333, 5555]

    Sequel::Migrator.get_current_migration_version(@db).should == 5
  end

  specify "should apply migrations down to 0 version correctly" do
    Sequel::Migrator.apply(@db, '.', 0, 5)
    @db.drops.should == [5555, 3333, 2222, 1111]

    Sequel::Migrator.get_current_migration_version(@db).should == 0
  end
  
  specify "should return the target version" do
    Sequel::Migrator.apply(@db, '.', 3, 2).should == 3

    Sequel::Migrator.apply(@db, '.', 0).should == 0

    Sequel::Migrator.apply(@db, '.').should == 5
  end
end
