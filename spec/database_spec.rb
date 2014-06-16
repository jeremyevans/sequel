SEQUEL_ADAPTER_TEST = :fdbsql

require 'sequel'
SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path
require File.join("#{SEQUEL_PATH}",'spec','adapters','spec_helper.rb')

describe 'Fdbsql' do
  before(:all) do
    @db = DB
  end

  describe 'automatic NotCommitted retry' do
    before do
      @db.drop_table?(:some_table)
      @db.create_table(:some_table) {text :name; primary_key :id}
      @db2 = Sequel.connect(@db.url)
    end
    after do
      @db2.disconnect
      @db.drop_table?(:some_table)
    end
    specify 'within a transaction' do
      proc do
        @db.transaction do
          @db[:some_table].insert(name: 'a')
          @db2.drop_table(:some_table)
        end
        # it probably should wrap this exception, but non of the other adapters wrap
        # commit exceptions, so I'm not going to either
      end.should raise_error(PG::TRIntegrityConstraintViolation)
    end
  end

  describe 'connection.in_transaction' do
    before(:all) do
      raise 'too many servers' if @db.servers.count > 1
      @db.drop_table?(:some_table)
      @db.create_table(:some_table) {text :name; primary_key :id}
      @conn = @db.pool.hold(@db.servers.first) {|conn| conn}
    end
    after(:all) do
      @db.drop_table?(:some_table)
    end

    specify 'is unset by default' do
      @conn.in_transaction.should be_false
    end
    specify 'is set in a transaction' do
      @db.transaction do
        @conn.in_transaction.should be_true
      end
    end
    specify 'is unset after a commit' do
      @db.transaction do
        @db[:some_table].insert(name: 'a')
        @conn.in_transaction.should be_true
      end
      @conn.in_transaction.should be_false
    end
    specify 'is unset after a rollback' do
      @db.transaction do
        raise Sequel::Rollback.new
      end
      @conn.in_transaction.should be_false
    end
  end
end
