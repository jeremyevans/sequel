SEQUEL_ADAPTER_TEST = :fdbsql

require 'sequel'
SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path
require File.join("#{SEQUEL_PATH}",'spec','adapters','spec_helper.rb')

def DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  DB.sqls << msg
end
DB.loggers << logger

describe 'Fdbsql' do
  before do
    @db = DB
    DB.sqls.clear
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
end
