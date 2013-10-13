SEQUEL_ADAPTER_TEST = :mssql

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

def DB.sqls
  (@sqls ||= [])
end
logger = Logger.new($stdout)  #Object.new
def logger.method_missing(m, msg)
  DB.sqls << msg
end
DB.loggers = [logger]


describe "MSSSQL read schema" do
  before do
    @db = DB
    @db.create_table! :items do
      primary_key :id
      String :name, :size => 20
      Integer :intval
      column :value, 'varbinary(max)'
      column :lock_version, 'timestamp'
    end
    #DB.disconnect
    #@db = MSSQLTestHelper.connect_db
    #@ds = @db[:test5]
  end
  after do
    @db.drop_table?(:items)
  end

  cspecify "timestamp should not be updated", :odbc do
    @c = Class.new(Sequel::Model(:items))
    @c.plugin :sql_optimistic_locking
    blob = Sequel::SQL::Blob.new("01234")
    @o = @c.create(name: 'max varbinary test', value: blob)
    @o = @c.first
    @ts = @o.lock_version
    @o.value = Sequel::SQL::Blob.new("43210")
    @o.save
    @o.lock_version.should_not eql @ts
  end

  cspecify "should allow large text and binary values", :odbc do
    @c = Class.new(Sequel::Model(:items))
    blob = Sequel::SQL::Blob.new("0" * (65*1024))
    @o = @c.create(name: 'max varbinary test', value: blob)
    @o = @c.first
    @o.value.length.should == blob.length
    @o.value.should == blob
  end

  cspecify "timestamp should be filled by server", :odbc do
    @c = Class.new(Sequel::Model(:items))
    blob = Sequel::SQL::Blob.new("01234")
    @o = @c.create(name: 'max varbinary test', value: blob)
    @o = @c.first
    @o.lock_version.should_not eql nil
    @o.lock_version.should be_kind_of(Sequel::SQL::Blob)
  end

end
