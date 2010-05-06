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
  end
  
  specify "should raise for an invalid direction" do
    proc {Sequel::Migration.apply(@db, :hahaha)}.should raise_error(ArgumentError)
  end
  
  specify "should apply the up and down directions correctly" do
    m = Class.new(Sequel::Migration) do
      define_method(:up) {one(3333)}
      define_method(:down) {two(4444)}
    end
    m.apply(@db, :up).should == [1111, 3333]
    m.apply(@db, :down).should == [2222, 4444]
  end

  specify "should support the simpler DSL" do
    m = Sequel.migration do
      up{one(3333)}
      down{two(4444)}
    end
    m.apply(@db, :up).should == [1111, 3333]
    m.apply(@db, :down).should == [2222, 4444]
  end

  specify "should have default up and down actions that do nothing" do
    m = Class.new(Sequel::Migration)
    m.apply(@db, :up).should == nil
    m.apply(@db, :down).should == nil
  end
end

context "Sequel::Migrator" do
  before do
    dbc = Class.new(MockDatabase) do
      attr_reader :drops, :tables_created, :columns_created, :versions
      def initialize(*args)
        super
        @drops = []
        @tables_created = []
        @columns_created = []
        @versions = {}
      end
  
      def version; versions.values.first || 0; end
      def creates; @tables_created.map{|x| y = x.to_s; y !~ /\Asm(\d+)/; $1.to_i if $1}.compact; end
      def drop_table(*a); super; @drops.concat(a.map{|x| y = x.to_s; y !~ /\Asm(\d+)/; $1.to_i if $1}.compact); end

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
    
    @dirname = "spec/files/integer_migrations"
    @alt_dirname = "spec/files/alt_integer_migrations"
  end
  
  after do
    Object.send(:remove_const, "CreateSessions") if Object.const_defined?("CreateSessions")
    Object.send(:remove_const, "CreateNodes") if Object.const_defined?("CreateNodes")
    Object.send(:remove_const, "CreateUsers") if Object.const_defined?("CreateUsers")
    Object.send(:remove_const, "CreateAltBasic") if Object.const_defined?("CreateAltBasic")
    Object.send(:remove_const, "CreateAltAdvanced") if Object.const_defined?("CreateAltAdvanced")
  end
  
  specify "#migration_files should return the list of files for a specified version range" do
    Sequel::Migrator.migration_files(@dirname, 1..1).map{|f| File.basename(f)}.should == ['001_create_sessions.rb']
    Sequel::Migrator.migration_files(@dirname, 1..3).map{|f| File.basename(f)}.should == ['001_create_sessions.rb', '002_create_nodes.rb', '003_create_users.rb']
    Sequel::Migrator.migration_files(@dirname, 3..6).map{|f| File.basename(f)}.should == ['003_create_users.rb']
    Sequel::Migrator.migration_files(@dirname, 7..8).map{|f| File.basename(f)}.should == []
    Sequel::Migrator.migration_files(@alt_dirname, 1..1).map{|f| File.basename(f)}.should == ['001_create_alt_basic.rb']
    Sequel::Migrator.migration_files(@alt_dirname, 1..3).map{|f| File.basename(f)}.should == ['001_create_alt_basic.rb','003_3_create_alt_advanced.rb']
  end
  
  specify "#latest_migration_version should return the latest version available" do
    Sequel::Migrator.latest_migration_version(@dirname).should == 3
    Sequel::Migrator.latest_migration_version(@alt_dirname).should == 3
  end
  
  specify "#migration_classes should load the migration classes for the specified range for the up direction" do
    Sequel::Migrator.migration_classes(@dirname, 3, 0, :up).should == [CreateSessions, CreateNodes, CreateUsers]
    Sequel::Migrator.migration_classes(@alt_dirname, 3, 0, :up).should == [CreateAltBasic, CreateAltAdvanced]
  end
  
  specify "#migration_classes should load the migration classes for the specified range for the down direction" do
    Sequel::Migrator.migration_classes(@dirname, 0, 5, :down).should == [CreateUsers, CreateNodes, CreateSessions]
    Sequel::Migrator.migration_classes(@alt_dirname, 0, 3, :down).should == [CreateAltAdvanced, CreateAltBasic]
  end
  
  specify "#migration_classes should start from current + 1 for the up direction" do
    Sequel::Migrator.migration_classes(@dirname, 3, 1, :up).should == [CreateNodes, CreateUsers]
    Sequel::Migrator.migration_classes(@alt_dirname, 3, 2, :up).should == [CreateAltAdvanced]
  end
  
  specify "#migration_classes should end on current + 1 for the down direction" do
    Sequel::Migrator.migration_classes(@dirname, 2, 5, :down).should == [CreateUsers]
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
  
  specify "should use the migration version stored in the database" do
    Sequel::Migrator.schema_info_dataset(@db).insert(:version => 4321)
    @db.version.should == 4321
  end
  
  specify "should set the migration version stored in the database" do
    Sequel::Migrator.set_current_migration_version(@db, 6666)
    @db.version.should == 6666
  end
  
  specify "should apply migrations correctly in the up direction" do
    Sequel::Migrator.apply(@db, @dirname, 2, 1)
    @db.creates.should == [2222]
    
    @db.version.should == 2

    Sequel::Migrator.apply(@db, @dirname, 3)
    @db.creates.should == [2222, 3333]

    @db.version.should == 3
  end
  
  specify "should apply migrations correctly in the down direction" do
    Sequel::Migrator.apply(@db, @dirname, 1, 3)
    @db.drops.should == [3333, 2222]

    @db.version.should == 1
  end

  specify "should apply migrations up to the latest version if no target is given" do
    Sequel::Migrator.apply(@db, @dirname)
    @db.creates.should == [1111, 2222, 3333]

    @db.version.should == 3
  end

  specify "should apply migrations down to 0 version correctly" do
    Sequel::Migrator.apply(@db, @dirname, 0, 3)
    @db.drops.should == [3333, 2222, 1111]

    @db.version.should == 0
  end
  
  specify "should return the target version" do
    Sequel::Migrator.apply(@db, @dirname, 3, 2).should == 3
    Sequel::Migrator.apply(@db, @dirname, 0).should == 0
    Sequel::Migrator.apply(@db, @dirname).should == 3
  end
end
