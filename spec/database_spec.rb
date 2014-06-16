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

  describe 'schema_parsing' do
    after do
      @db.drop_table?(:test)
    end

    specify 'without primary key' do
      @db.create_table(:test) do
        text :name
        int :value
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      schema[0][0].should eq :name
      schema[1][0].should eq :value
      schema.each {|col| col[1][:primary_key].should be_false}
    end

    specify 'with one primary key' do
      @db.create_table(:test) do
        text :name
        primary_key :id
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      id_col = schema[0]
      name_col = schema[1]
      name_col[0].should eq :name
      id_col[0].should eq :id
      name_col[1][:primary_key].should be_false
      id_col[1][:primary_key].should be_true
    end

    specify 'with multiple primary keys' do
      @db.create_table(:test) do
        Integer :id
        Integer :id2
        primary_key [:id, :id2]
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      id2_col = schema[1]
      id_col = schema[0]
      id_col[0].should eq :id
      id2_col[0].should eq :id2
      id_col[1][:primary_key].should be_true
      id2_col[1][:primary_key].should be_true
    end

    specify 'with other constraints' do
      @db.create_table(:test) do
        primary_key :id
        Integer :unique, unique: true
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      id_col = schema[0]
      unique_col = schema[1]
      id_col[0].should eq :id
      unique_col[0].should eq :unique
      id_col[1][:primary_key].should be_true
      unique_col[1][:primary_key].should be_false
    end
    after do
      @db.drop_table?(:other_table)
    end
    specify 'with other tables' do
      @db.create_table(:test) do
        Integer :id
        text :name
      end
      @db.create_table(:other_table) do
        primary_key :id
        varchar :name, unique: true
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      schema.each {|col| col[1][:primary_key].should be_false}
    end
  end
end
