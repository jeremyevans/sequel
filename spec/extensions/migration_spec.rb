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

ALT_MIGRATION_001 = %[
  class CreateAltBasic < Sequel::Migration
    def up
      create(11111)
    end
    
    def down
      drop(11111)
    end
  end
]

ALT_MIGRATION_003 = %[
  class CreateAltAdvanced < Sequel::Migration
    def up
      create(33333)
    end
    
    def down
      drop(33333)
    end
  end
]

context "Sequel::Migrator" do
  before do
    dbc = Class.new(MockDatabase) do
      attr_reader :creates, :drops, :tables_created, :columns_created, :versions
      def initialize(*args)
        super
        @creates = []
        @drops = []
        @tables_created = []
        @columns_created = []
        @versions = {}
      end
  
      def create(x); @creates << x; end
      def drop(x); @drops << x; end

      def create_table(name, opts={}, &block)
        super
        @columns_created << / \(?(\w+) integer\)?\z/.match(sqls.last)[1].to_sym
        @tables_created << name
      end
      
      def dataset(opts={})
        ds = super
        ds.extend(Module.new do
          def columns; db.columns_created end
          def insert(h); db.versions.merge!(h); super(h) end
          def update(h); db.versions.merge!(h); super(h) end
          def fetch_rows(sql); db.execute(sql); yield(db.versions) unless db.versions.empty? end
        end)
        ds
      end

      def table_exists?(name)
        @tables_created.include?(name)
      end
    end
    @db = dbc.new
    
    @dirname = "migrate_#{$$}"
    Dir.mkdir(@dirname)
    File.open("#{@dirname}/001_create_sessions.rb", 'w') {|f| f << MIGRATION_001}
    File.open("#{@dirname}/002_create_nodes.rb", 'w') {|f| f << MIGRATION_002}
    File.open("#{@dirname}/003_create_users.rb", 'w') {|f| f << MIGRATION_003}
    File.open("#{@dirname}/005_5_create_attributes.rb", 'w') {|f| f << MIGRATION_005}
    
    @alt_dirname = "migrate_alt_#{$$}"
    Dir.mkdir(@alt_dirname)
    File.open("#{@alt_dirname}/001_create_alt_basic.rb", 'w') {|f| f << ALT_MIGRATION_001}
    File.open("#{@alt_dirname}/003_create_alt_advanced.rb", 'w') {|f| f << ALT_MIGRATION_003}
  end
  
  after do
    Object.send(:remove_const, "CreateSessions") if Object.const_defined?("CreateSessions")
    Object.send(:remove_const, "CreateNodes") if Object.const_defined?("CreateNodes")
    Object.send(:remove_const, "CreateUsers") if Object.const_defined?("CreateUsers")
    Object.send(:remove_const, "CreateAttributes") if Object.const_defined?("CreateAttributes")
    Object.send(:remove_const, "CreateAltBasic") if Object.const_defined?("CreateAltBasic")
    Object.send(:remove_const, "CreateAltAdvanced") if Object.const_defined?("CreateAltAdvanced")

    File.delete("#{@dirname}/001_create_sessions.rb")
    File.delete("#{@dirname}/002_create_nodes.rb")
    File.delete("#{@dirname}/003_create_users.rb")
    File.delete("#{@dirname}/005_5_create_attributes.rb")
    Dir.rmdir(@dirname)
    File.delete("#{@alt_dirname}/001_create_alt_basic.rb")
    File.delete("#{@alt_dirname}/003_create_alt_advanced.rb")
    Dir.rmdir(@alt_dirname)
  end
  
  specify "#migration_files should return the list of files for a specified version range" do
    Sequel::Migrator.migration_files(@dirname, 1..1).map{|f| File.basename(f)}.should == ['001_create_sessions.rb']
    Sequel::Migrator.migration_files(@dirname, 1..3).map{|f| File.basename(f)}.should == ['001_create_sessions.rb', '002_create_nodes.rb', '003_create_users.rb']
    Sequel::Migrator.migration_files(@dirname, 3..6).map{|f| File.basename(f)}.should == ['003_create_users.rb', '005_5_create_attributes.rb']
    Sequel::Migrator.migration_files(@dirname, 7..8).map{|f| File.basename(f)}.should == []
    Sequel::Migrator.migration_files(@alt_dirname, 1..1).map{|f| File.basename(f)}.should == ['001_create_alt_basic.rb']
    Sequel::Migrator.migration_files(@alt_dirname, 1..3).map{|f| File.basename(f)}.should == ['001_create_alt_basic.rb','003_create_alt_advanced.rb']
  end
  
  specify "#latest_migration_version should return the latest version available" do
    Sequel::Migrator.latest_migration_version(@dirname).should == 5
    Sequel::Migrator.latest_migration_version(@alt_dirname).should == 3
  end
  
  specify "#migration_classes should load the migration classes for the specified range for the up direction" do
    Sequel::Migrator.migration_classes(@dirname, 3, 0, :up).should == [CreateSessions, CreateNodes, CreateUsers]
    Sequel::Migrator.migration_classes(@alt_dirname, 3, 0, :up).should == [CreateAltBasic, CreateAltAdvanced]
  end
  
  specify "#migration_classes should load the migration classes for the specified range for the down direction" do
    Sequel::Migrator.migration_classes(@dirname, 0, 5, :down).should == [CreateAttributes, CreateUsers, CreateNodes, CreateSessions]
    Sequel::Migrator.migration_classes(@alt_dirname, 0, 3, :down).should == [CreateAltAdvanced, CreateAltBasic]
  end
  
  specify "#migration_classes should start from current + 1 for the up direction" do
    Sequel::Migrator.migration_classes(@dirname, 3, 1, :up).should == [CreateNodes, CreateUsers]
    Sequel::Migrator.migration_classes(@alt_dirname, 3, 2, :up).should == [CreateAltAdvanced]
  end
  
  specify "#migration_classes should end on current + 1 for the down direction" do
    Sequel::Migrator.migration_classes(@dirname, 2, 5, :down).should == [CreateAttributes, CreateUsers]
    Sequel::Migrator.migration_classes(@alt_dirname, 2, 4, :down).should == [CreateAltAdvanced]
  end
  
  specify "#schema_info_dataset should automatically create the schema_info table" do
    @db.table_exists?(:schema_info).should be_false
    Sequel::Migrator.schema_info_dataset(@db)
    @db.table_exists?(:schema_info).should be_true
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)"]
  end

  specify "should automatically create new APP_version column in schema_info" do
    @db.table_exists?(:schema_info).should be_false
    Sequel::Migrator.schema_info_dataset(@db, :column => :alt_version)
    @db.table_exists?(:schema_info).should be_true
    @db.sqls.should == ["CREATE TABLE schema_info (alt_version integer)"]
  end
  
  specify "should automatically create new APP_version column in schema_info" do
    @db.table_exists?(:alt_table).should be_false
    Sequel::Migrator.schema_info_dataset(@db, :table => :alt_table)
    @db.table_exists?(:alt_table).should be_true
    Sequel::Migrator.schema_info_dataset(@db, :table => :alt_table, :column=>:alt_version)
    @db.sqls.should == ["CREATE TABLE alt_table (version integer)",
      "ALTER TABLE alt_table ADD COLUMN alt_version integer"]
  end
  
  specify "should return a dataset for the correct table" do
    Sequel::Migrator.schema_info_dataset(@db).first_source_alias.should == :schema_info
    Sequel::Migrator.schema_info_dataset(@db, :table=>:blah).first_source_alias.should == :blah
  end
  
  specify "should assume a migration version of 0 if no migration information exists in the database" do
    Sequel::Migrator.get_current_migration_version(@db).should == 0
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)", "SELECT * FROM schema_info LIMIT 1"]
  end
  specify "should use the migration version stored in the database" do
    Sequel::Migrator.schema_info_dataset(@db).insert(:version => 4321)
    Sequel::Migrator.get_current_migration_version(@db).should == 4321
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)", "INSERT INTO schema_info (version) VALUES (4321)", "SELECT * FROM schema_info LIMIT 1"]
  end
  
  specify "should set the migration version stored in the database" do
    Sequel::Migrator.set_current_migration_version(@db, 6666)
    Sequel::Migrator.get_current_migration_version(@db).should == 6666
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)",
      "SELECT * FROM schema_info LIMIT 1",
      "INSERT INTO schema_info (version) VALUES (6666)",
      "SELECT * FROM schema_info LIMIT 1"]
  end
  
  specify "should apply migrations correctly in the up direction" do
    Sequel::Migrator.apply(@db, @dirname, 3, 2)
    @db.creates.should == [3333]
    
    Sequel::Migrator.get_current_migration_version(@db).should == 3

    Sequel::Migrator.apply(@db, @dirname, 5)
    @db.creates.should == [3333, 5555]

    Sequel::Migrator.get_current_migration_version(@db).should == 5
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)",
      "SELECT * FROM schema_info LIMIT 1",
      "INSERT INTO schema_info (version) VALUES (3)",
      "SELECT * FROM schema_info LIMIT 1",
      "SELECT * FROM schema_info LIMIT 1",
      "SELECT * FROM schema_info LIMIT 1",
      "UPDATE schema_info SET version = 5",
      "SELECT * FROM schema_info LIMIT 1"]
  end
  
  specify "should apply migrations correctly in the down direction" do
    Sequel::Migrator.apply(@db, @dirname, 1, 5)
    @db.drops.should == [5555, 3333, 2222]

    Sequel::Migrator.get_current_migration_version(@db).should == 1
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)",
      "SELECT * FROM schema_info LIMIT 1",
      "INSERT INTO schema_info (version) VALUES (1)",
      "SELECT * FROM schema_info LIMIT 1"]
  end

  specify "should apply migrations up to the latest version if no target is given" do
    Sequel::Migrator.apply(@db, @dirname)
    @db.creates.should == [1111, 2222, 3333, 5555]

    Sequel::Migrator.get_current_migration_version(@db).should == 5
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)",
      "SELECT * FROM schema_info LIMIT 1",
      "SELECT * FROM schema_info LIMIT 1",
      "INSERT INTO schema_info (version) VALUES (5)",
      "SELECT * FROM schema_info LIMIT 1"]
  end

  specify "should apply migrations down to 0 version correctly" do
    Sequel::Migrator.apply(@db, @dirname, 0, 5)
    @db.drops.should == [5555, 3333, 2222, 1111]

    Sequel::Migrator.get_current_migration_version(@db).should == 0
    @db.sqls.should == ["CREATE TABLE schema_info (version integer)",
      "SELECT * FROM schema_info LIMIT 1",
      "INSERT INTO schema_info (version) VALUES (0)",
      "SELECT * FROM schema_info LIMIT 1"]
  end
  
  specify "should return the target version" do
    Sequel::Migrator.apply(@db, @dirname, 3, 2).should == 3
    Sequel::Migrator.apply(@db, @dirname, 0).should == 0
    Sequel::Migrator.apply(@db, @dirname).should == 5
  end
end
