require File.join(File.dirname(__FILE__), 'spec_helper')

context "Migration.descendants" do
  before do
    Sequel::Migration.descendants.clear
  end

  specify "should include Migration subclasses" do
    @class = Class.new(Sequel::Migration)
    
    Sequel::Migration.descendants.should == [@class]
  end
  
  specify "should include Migration subclasses in order of creation" do
    @c1 = Class.new(Sequel::Migration)
    @c2 = Class.new(Sequel::Migration)
    @c3 = Class.new(Sequel::Migration)
    
    Sequel::Migration.descendants.should == [@c1, @c2, @c3]
  end

  specify "should include SimpleMigration instances created by migration DSL" do
    i1 = Sequel.migration{}
    i2 = Sequel.migration{}
    i3 = Sequel.migration{}
    
    Sequel::Migration.descendants.should == [i1, i2, i3]
  end
end

context "Migration.apply" do
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

  specify "should have default up and down actions that do nothing" do
    m = Class.new(Sequel::Migration)
    m.apply(@db, :up).should == nil
    m.apply(@db, :down).should == nil
  end
end

context "SimpleMigration#apply" do
  before do
    @c = Class.new do
      define_method(:one) {|x| [1111, x]}
      define_method(:two) {|x| [2222, x]}
    end
    @db = @c.new
  end
  
  specify "should raise for an invalid direction" do
    proc {Sequel.migration{}.apply(@db, :hahaha)}.should raise_error(ArgumentError)
  end
  
  specify "should apply the up and down directions correctly" do
    m = Sequel.migration do
      up{one(3333)}
      down{two(4444)}
    end
    m.apply(@db, :up).should == [1111, 3333]
    m.apply(@db, :down).should == [2222, 4444]
  end

  specify "should have default up and down actions that do nothing" do
    m = Sequel.migration{}
    m.apply(@db, :up).should == nil
    m.apply(@db, :down).should == nil
  end
end

context "Sequel::IntegerMigrator" do
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
        @columns_created << / \(?(\w+) integer.*\)?\z/.match(sqls.last)[1].to_sym
        @tables_created << name
      end
      
      def dataset(opts={})
        ds = super
        ds.extend(Module.new do
          def count; 1; end
          def columns; db.columns_created end
          def insert(h); db.versions.merge!(h); db.sqls << insert_sql(h) end
          def update(h); db.versions.merge!(h); db.sqls << update_sql(h) end
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
  end
  
  after do
    Object.send(:remove_const, "CreateSessions") if Object.const_defined?("CreateSessions")
  end
  
  specify "should raise and error if there is a missing integer migration version" do
    proc{Sequel::Migrator.apply(@db, "spec/files/missing_integer_migrations")}.should raise_error(Sequel::Migrator::Error)
  end

  specify "should raise and error if there is a duplicate integer migration version" do
    proc{Sequel::Migrator.apply(@db, "spec/files/duplicate_integer_migrations")}.should raise_error(Sequel::Migrator::Error)
  end

  specify "should automatically create the schema_info table with the version column" do
    @db.table_exists?(:schema_info).should be_false
    Sequel::Migrator.run(@db, @dirname, :target=>0)
    @db.table_exists?(:schema_info).should be_true
    @db.dataset.columns.should == [:version]
  end

  specify "should allow specifying the table and columns" do
    @db.table_exists?(:si).should be_false
    Sequel::Migrator.run(@db, @dirname, :target=>0, :table=>:si, :column=>:sic)
    @db.table_exists?(:si).should be_true
    @db.dataset.columns.should == [:sic]
  end
  
  specify "should apply migrations correctly in the up direction if no target is given" do
    Sequel::Migrator.apply(@db, @dirname)
    @db.creates.should == [1111, 2222, 3333]
    @db.version.should == 3
    @db.sqls.map{|x| x =~ /\AUPDATE.*(\d+)/ ? $1.to_i : nil}.compact.should == [1, 2, 3]
  end 

  specify "should apply migrations correctly in the up direction with target" do
    Sequel::Migrator.apply(@db, @dirname, 2)
    @db.creates.should == [1111, 2222]
    @db.version.should == 2
    @db.sqls.map{|x| x =~ /\AUPDATE.*(\d+)/ ? $1.to_i : nil}.compact.should == [1, 2]
  end
  
  specify "should apply migrations correctly in the up direction with target and existing" do
    Sequel::Migrator.apply(@db, @dirname, 2, 1)
    @db.creates.should == [2222]
    @db.version.should == 2
    @db.sqls.map{|x| x =~ /\AUPDATE.*(\d+)/ ? $1.to_i : nil}.compact.should == [2]
  end

  specify "should apply migrations correctly in the down direction with target" do
    @db.create_table(:schema_info){Integer :version, :default=>0}
    @db[:schema_info].insert(:version=>3)
    @db.version.should == 3
    Sequel::Migrator.apply(@db, @dirname, 0)
    @db.drops.should == [3333, 2222, 1111]
    @db.version.should == 0
    @db.sqls.map{|x| x =~ /\AUPDATE.*(\d+)/ ? $1.to_i : nil}.compact.should == [2, 1, 0]
  end
  
  specify "should apply migrations correctly in the down direction with target and existing" do
    Sequel::Migrator.apply(@db, @dirname, 1, 2)
    @db.drops.should == [2222]
    @db.version.should == 1
    @db.sqls.map{|x| x =~ /\AUPDATE.*(\d+)/ ? $1.to_i : nil}.compact.should == [1]
  end
  
  specify "should return the target version" do
    Sequel::Migrator.apply(@db, @dirname, 3, 2).should == 3
    Sequel::Migrator.apply(@db, @dirname, 0).should == 0
    Sequel::Migrator.apply(@db, @dirname).should == 3
  end
end
