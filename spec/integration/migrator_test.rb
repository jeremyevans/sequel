require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

Sequel.extension :migration
describe Sequel::Migrator do
  before do
    @db = INTEGRATION_DB
    @m = Sequel::Migrator
  end
  after do
    @db.disconnect
    [:schema_info, :schema_migrations, :sm1111, :sm1122, :sm2222, :sm2233, :sm3333, :sm11111, :sm22222].each{|n| @db.drop_table(n) rescue nil}
  end
  
  specify "should be able to migrate up and down all the way successfully" do
    @dir = 'spec/files/integer_migrations'
    @m.apply(@db, @dir)
    [:schema_info, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_info].get(:version).should == 3
    @m.apply(@db, @dir, 0)
    [:sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_info].get(:version).should == 0
  end
  
  specify "should be able to migrate up and down to specific versions successfully" do
    @dir = 'spec/files/integer_migrations'
    @m.apply(@db, @dir, 2)
    [:schema_info, :sm1111, :sm2222].each{|n| @db.table_exists?(n).should be_true}
    @db.table_exists?(:sm3333).should be_false
    @db[:schema_info].get(:version).should == 2
    @m.apply(@db, @dir, 1)
    [:sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db.table_exists?(:sm1111).should be_true
    @db[:schema_info].get(:version).should == 1
  end

  specify "should correctly set migration version to the last successful migration if the migration raises an error when migrating up" do
    @dir = 'spec/files/bad_up_migration'
    proc{@m.apply(@db, @dir)}.should raise_error
    [:schema_info, :sm11111].each{|n| @db.table_exists?(n).should be_true}
    @db.table_exists?(:sm22222).should be_false
    @db[:schema_info].get(:version).should == 1
    @m.apply(@db, @dir, 0)
    [:sm11111, :sm22222].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_info].get(:version).should == 0
  end

  specify "should correctly set migration version to the last successful migration if the migration raises an error when migrating down" do
    @dir = 'spec/files/bad_down_migration'
    @m.apply(@db, @dir)
    [:schema_info, :sm11111, :sm22222].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_info].get(:version).should == 2
    proc{@m.apply(@db, @dir, 0)}.should raise_error
    [:sm22222].each{|n| @db.table_exists?(n).should be_false}
    @db.table_exists?(:sm11111).should be_true
    @db[:schema_info].get(:version).should == 1
  end

  specify "should handle migrating up or down all the way with timestamped migrations" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    [:schema_migrations, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253851_create_nodes.rb 1273253853_3_create_users.rb'
    @m.apply(@db, @dir, 0)
    [:sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == []
  end

  specify "should handle migrating up or down to specific timestamps with timestamped migrations" do
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

  specify "should apply all missing files when migrating up with timestamped migrations" do
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

  specify "should handle updating to a specific timestamp when interleaving migrations with timestamps" do
    @dir = 'spec/files/timestamped_migrations'
    @m.apply(@db, @dir)
    @dir = 'spec/files/interleaved_timestamped_migrations'
    @m.apply(@db, @dir, 1273253851)
    [:schema_migrations, :sm1111, :sm1122, :sm2222].each{|n| @db.table_exists?(n).should be_true}
    [:sm2233, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_migrations].select_order_map(:filename).should == %w'1273253849_create_sessions.rb 1273253850_create_artists.rb 1273253851_create_nodes.rb'
  end

  specify "should correctly update schema_migrations table when an error occurs when migrating up or down using timestamped migrations" do
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

  specify "should handle reversible migrations" do
    @dir = 'spec/files/reversible_migrations'
    @db.drop_table(:a) rescue nil
    @db.drop_table(:b) rescue nil
    @m.apply(@db, @dir, 1)
    [:schema_info, :a].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :b].each{|n| @db.table_exists?(n).should be_false}
    @db[:a].columns.should == [:a]

    @m.apply(@db, @dir, 2)
    [:schema_info, :a].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :b].each{|n| @db.table_exists?(n).should be_false}
    @db[:a].columns.should == [:a, :b]

    @m.apply(@db, @dir, 3)
    [:schema_info, :a].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :b].each{|n| @db.table_exists?(n).should be_false}
    @db[:a].columns.should == [:a, :c]

    @m.apply(@db, @dir, 4)
    [:schema_info, :b].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :a].each{|n| @db.table_exists?(n).should be_false}
    @db[:b].columns.should == [:a, :c]

    @m.apply(@db, @dir, 5)
    [:schema_info, :b].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :a].each{|n| @db.table_exists?(n).should be_false}
    @db[:b].columns.should == [:a, :c, :e]

    @m.apply(@db, @dir, 4)
    [:schema_info, :b].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :a].each{|n| @db.table_exists?(n).should be_false}
    @db[:b].columns.should == [:a, :c]

    @m.apply(@db, @dir, 3)
    [:schema_info, :a].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :b].each{|n| @db.table_exists?(n).should be_false}
    @db[:a].columns.should == [:a, :c]

    @m.apply(@db, @dir, 2)
    [:schema_info, :a].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :b].each{|n| @db.table_exists?(n).should be_false}
    @db[:a].columns.should == [:a, :b]

    @m.apply(@db, @dir, 1)
    [:schema_info, :a].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :b].each{|n| @db.table_exists?(n).should be_false}
    @db[:a].columns.should == [:a]

    @m.apply(@db, @dir, 0)
    [:schema_info].each{|n| @db.table_exists?(n).should be_true}
    [:schema_migrations, :a, :b].each{|n| @db.table_exists?(n).should be_false}
  end
end
