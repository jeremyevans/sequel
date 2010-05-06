require File.join(File.dirname(__FILE__), 'spec_helper.rb')

Sequel.extension :migration
describe Sequel::Migrator do
  before do
    @db = INTEGRATION_DB
    @m = Sequel::Migrator
    @dir = 'spec/files/integer_migrations'
  end
  after do
    [:schema_info, :sm1111, :sm2222, :sm3333].each{|n| @db.drop_table(n) rescue nil}
  end
  
  specify "should be able to migrate up and down all the way successfully" do
    @m.apply(@db, @dir)
    [:schema_info, :sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_true}
    @db[:schema_info].get(:version).should == 3
    @m.apply(@db, @dir, 0)
    [:sm1111, :sm2222, :sm3333].each{|n| @db.table_exists?(n).should be_false}
    @db[:schema_info].get(:version).should == 0
  end
  
  specify "should be able to migrate up and down to specific versions successfully" do
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
end
