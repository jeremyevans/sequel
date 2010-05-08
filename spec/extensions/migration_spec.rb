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

  specify "should add a column name if it doesn't already exist in the schema_info table" do
    @db.create_table(:schema_info){Integer :v}
    @db.should_receive(:alter_table).with(:schema_info)
    Sequel::Migrator.apply(@db, @dirname)
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

context "Sequel::TimestampMigrator" do
  before do
    $sequel_migration_version = 0
    $sequel_migration_files = []
    @dsc = dsc = Class.new(MockDataset) do
      def columns
        case opts[:from].first
        when :schema_info
          [:version]
        when :schema_migrations
          [:filename]
        when :sm
          [:fn]
        end
      end

      def fetch_rows(sql)
        case opts[:from].first
        when :schema_info
          yield({:version=>$sequel_migration_version})
        when :schema_migrations
          $sequel_migration_files.sort.each{|f| yield(:filename=>f)}
        when :sm
          $sequel_migration_files.sort.each{|f| yield(:fn=>f)}
        end
      end

      def insert(h={})
        case opts[:from].first
        when :schema_info
          $sequel_migration_version = h.values.first
        when :schema_migrations, :sm
          $sequel_migration_files << h.values.first
        end
      end

      def update(h={})
        case opts[:from].first
        when :schema_info
          $sequel_migration_version = h.values.first
        end
      end

      def delete
        case opts[:from].first
        when :schema_migrations, :sm
          $sequel_migration_files.delete(opts[:where].args.last)
        end
      end
    end
    dbc = Class.new(MockDatabase) do
      tables = {}
      define_method(:dataset){|*a| dsc.new(self, *a)}
      define_method(:create_table){|name, *args| tables[name] = true}
      define_method(:drop_table){|*names| names.each{|n| tables.delete(n)}}
      define_method(:table_exists?){|name| tables.has_key?(name)}
    end
    @db = dbc.new
    @m = Sequel::Migrator
  end

  after do
    Object.send(:remove_const, "CreateSessions") if Object.const_defined?("CreateSessions")
    Object.send(:remove_const, "CreateArtists") if Object.const_defined?("CreateArtists")
    Object.send(:remove_const, "CreateAlbums") if Object.const_defined?("CreateAlbums")
  end
  
  specify "should handle migrating up or down all the way" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    [:schema_migrations, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253851_create_nodes.rb 1273253853_3_create_users.rb'
    @m.apply(@db, @dir, 0)
    [:sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == []
  end

  specify "should handle migrating up or down to specific timestamps" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir, 1273253851)
    [:schema_migrations, :sm1111, :sm2222].each{|n| @db.table_exists?(n).should be_true}
    @db.table_exists?(:sm3333).should be_false
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253851_create_nodes.rb'
    @m.apply(@db, @dir, 1273253849)
    [:sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db.table_exists?(:sm1111).should be_true
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb'
  end

  specify "should apply all missing files when migrating up" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    @dir = 'spec/files/interleaved_timestamped_migrations'
    @m.apply(@db, @dir)
    [:schema_migrations, :sm1111, :sm1122, :sm2222, :sm2233, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253850_create_artists.rb 1273253851_create_nodes.rb 1273253852_create_albums.rb 1273253853_3_create_users.rb'
  end

  specify "should not apply down action to migrations where up action hasn't been applied" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    @dir = 'spec/files/interleaved_timestamped_migrations'
    @m.apply(@db, @dir, 0)
    [:sm1111, :sm1122, :sm2222, :sm2233, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == []
  end

  specify "should handle updating to a specific timestamp when interleaving migrations" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    @dir = 'spec/files/interleaved_timestamped_migrations'
    @m.apply(@db, @dir, 1273253851)
    [:schema_migrations, :sm1111, :sm1122, :sm2222].each{|n| @db.table_exists?(n).should be_true}
    [:sm2233, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253850_create_artists.rb 1273253851_create_nodes.rb'
  end

  specify "should correctly update schema_migrations table when an error occurs when migrating up or down" do
    @dir = 'spec/files/bad_timestamped_migrations'
    proc{@m.apply(@db, @dir)}.should raise_error
    [:schema_migrations, :sm1111, :sm2222].each{|n| @db.table_exists?(n).should be_true}
    @db.table_exists?(:sm3333).should be_false
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253851_create_nodes.rb'
    proc{@m.apply(@db, @dir, 0)}.should raise_error
    [:sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db.table_exists?(:sm1111).should be_true
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb'
  end

  specify "should handle multiple migrations with the same timestamp correctly" do
    @dir = 'spec/files/duplicate_timestamped_migrations'
    @m.apply(@db, @dir)
    [:schema_migrations, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253853_create_nodes.rb 1273253853_create_users.rb'
    @m.apply(@db, @dir, 1273253853)
    [:sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253853_create_nodes.rb 1273253853_create_users.rb'
    @m.apply(@db, @dir, 1273253849)
    [:sm1111].each{|n| @db.table_exists?(n).should be_true}
    [:sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb'
    @m.apply(@db, @dir, 1273253848)
    [:sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == []
  end

  specify "should convert schema_info table to schema_migrations table" do
    @dir = 'spec/files/integer_migrations'
    @m.apply(@db, @dir)
    [:schema_info, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_false}

    @dir = 'spec/files/convert_to_timestamp_migrations'
    @m.apply(@db, @dir)
    [:schema_info, :sm1111, :sm2222, :sm3333, :schema_migrations, :sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'001_create_sessions.rb 002_create_nodes.rb 003_3_create_users.rb 1273253850_create_artists.rb 1273253852_create_albums.rb'

    @m.apply(@db, @dir, 4)
    [:schema_info, :schema_migrations, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    [:sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == %w'001_create_sessions.rb 002_create_nodes.rb 003_3_create_users.rb'

    @m.apply(@db, @dir, 0)
    [:schema_info, :schema_migrations].each{|n| @db.table_exists?(n).should be_true}
    [:sm1111, :sm2222, :sm3333, :sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == []
  end

  specify "should handle unapplied migrations when migrating schema_info table to schema_migrations table" do
    @dir = 'spec/files/integer_migrations'
    @m.apply(@db, @dir, 2)
    [:schema_info, :sm1111, :sm2222].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :sm3333, :sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_false}

    @dir = 'spec/files/convert_to_timestamp_migrations'
    @m.apply(@db, @dir, 1273253850)
    [:schema_info, :sm1111, :sm2222, :sm3333, :schema_migrations, :sm1122].each{|n| @db.table_exists?(n).should be_true}
    [:sm2233].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == %w'001_create_sessions.rb 002_create_nodes.rb 003_3_create_users.rb 1273253850_create_artists.rb'
  end

  specify "should handle unapplied migrations when migrating schema_info table to schema_migrations table and target is less than last integer migration version" do
    @dir = 'spec/files/integer_migrations'
    @m.apply(@db, @dir, 1)
    [:schema_info, :sm1111].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :sm2222, :sm3333, :sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_false}

    @dir = 'spec/files/convert_to_timestamp_migrations'
    @m.apply(@db, @dir, 2)
    [:schema_info, :sm1111, :sm2222, :schema_migrations].each{|n| @db.table_exists?(n).should be_true}
    [:sm3333, :sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == %w'001_create_sessions.rb 002_create_nodes.rb'

    @m.apply(@db, @dir)
    [:schema_info, :sm1111, :sm2222, :schema_migrations, :sm3333, :sm1122, :sm2233].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'001_create_sessions.rb 002_create_nodes.rb 003_3_create_users.rb 1273253850_create_artists.rb 1273253852_create_albums.rb'
  end

  specify "should raise error for applied migrations not in file system" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    [:schema_migrations, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253851_create_nodes.rb 1273253853_3_create_users.rb'

    @dir = 'spec/files/missing_timestamped_migrations'
    proc{@m.apply(@db, @dir, 0)}.should raise_error(Sequel::Migrator::Error)
    [:schema_migrations, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253851_create_nodes.rb 1273253853_3_create_users.rb'
  end
  
  specify "should raise error missing column name in existing schema_migrations table" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    proc{@m.run(@db, @dir, :column=>:fn)}.should raise_error(Sequel::Migrator::Error)
  end
  
  specify "should :table and :column options" do
    @dir = 'spec/files/timestamped_migrations'
    @m.run(@db, @dir, :table=>:sm, :column=>:fn)
    [:sm, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:sm].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253851_create_nodes.rb 1273253853_3_create_users.rb'
    @m.run(@db, @dir, :target=>0, :table=>:sm, :column=>:fn)
    [:sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:sm].select_order_map(:fn).should == []
  end

  specify "should return nil" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir, 1273253850).should == nil
    @m.apply(@db, @dir, 0).should == nil
    @m.apply(@db, @dir).should == nil
  end
end
