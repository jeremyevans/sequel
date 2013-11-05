SEQUEL_ADAPTER_TEST = :mssql

require File.join(File.dirname(File.expand_path(__FILE__)), '../adapters/spec_helper.rb')

describe "MSSSQL optimistic locking plugin" do
  before do
    @db = DB
    @db.create_table! :items do
      primary_key :id
      String :name, :size => 20
      column :timestamp, 'timestamp'
    end
   end
  after do
    @db.drop_table?(:items)
  end

  cspecify "timestamp should be filled by server", :odbc do
    @c = Class.new(Sequel::Model(:items))
    @c.plugin :mssql_optimistic_locking
    @o = @c.create(name: 'test')
    @o = @c.first
    @o.timestamp.should_not eql nil
  end

  cspecify "create and update should work", :odbc do
    @c = Class.new(Sequel::Model(:items))
    @c.plugin :mssql_optimistic_locking
    @o = @c.create(name: 'test')
    @o = @c.first
    @ts = @o.timestamp
    @o.name = 'test2'
    @o.save
    @o.timestamp.should_not eql @ts
  end


end
