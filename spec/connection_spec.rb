SEQUEL_ADAPTER_TEST = :fdbsql unless defined? SEQUEL_ADAPTER_TEST and SEQUEL_ADAPTER_TEST == :fdbsql

unless defined? SEQUEL_PATH
  require 'sequel'
  SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path
  require File.join("#{SEQUEL_PATH}",'spec','adapters','spec_helper.rb')
end

def DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  DB.sqls << msg
end
DB.loggers << logger


describe 'Fdbsql::Connection' do
  before do
    @fake_conn = double('connection class')
    stub_const('PG::Connection', @fake_conn)
  end

  def fake_conn
    fake_conn_instance = double("fake connection")
    fake_conn_instance.stub(:set_notice_receiver)
    fake_conn_instance.stub(:query).with('SELECT VERSION()', nil).ordered.and_return([{'_SQL_COL_1' => 'FoundationDB 1.9.6'}])
    yield fake_conn_instance
    @fake_conn.stub(:new).and_return(fake_conn_instance)
  end

  describe 'Automatic retry on NotCommitted' do

    describe 'outside a transaction' do
      specify 'retries a finite number of times' do
        result = double('result')
        e = PG::TRIntegrityConstraintViolation.new
        e.stub(:result).and_return(result)
        result.stub(:error_field).with(::PGresult::PG_DIAG_SQLSTATE).and_return("40002")
        fake_conn {|conn| conn.stub(:query).with('SELECT 3', nil).ordered.and_raise(e)}
        conn = Sequel::Fdbsql::Connection.new(nil, {})
        proc do
          conn.query('SELECT 3')
        end.should raise_error(PG::TRIntegrityConstraintViolation)
      end

      specify 'retries more than 5 times' do
        result = double('result')
        e = PG::TRIntegrityConstraintViolation.new
        e.stub(:result).and_return(result)
        result.stub(:error_field).with(::PGresult::PG_DIAG_SQLSTATE).and_return("40002")
        time = 0
        fake_conn do |conn|
          conn.stub(:query).with('SELECT 3', nil).ordered do
            raise e if (time += 1) < 5
            3
          end
        end
        conn = Sequel::Fdbsql::Connection.new(nil, {})
        conn.query('SELECT 3')
      end
    end
    describe 'inside a transaction' do
      specify 'does not retry' do
        result = double('result')
        e = PG::TRIntegrityConstraintViolation.new
        e.stub(:result).and_return(result)
        result.stub(:error_field).with(::PGresult::PG_DIAG_SQLSTATE).and_return("40002")
        fake_conn {|conn| conn.stub(:query).with('SELECT 3', nil).once.ordered.and_raise(e)}
        conn = Sequel::Fdbsql::Connection.new(nil, {})
        conn.in_transaction = true
        proc do
          conn.query('SELECT 3')
        end.should raise_error(PG::TRIntegrityConstraintViolation)
      end
    end
  end

  describe 'checks sql layer version' do
    ['1.9.5', '0.9.6', '1.8.6'].each do |version|
      it "throws error for #{version}" do
        fake_conn {|conn| conn.stub(:query).with('SELECT VERSION()', nil).and_return([{'_SQL_COL_1' => "FoundationDB #{version}"}])}
        proc do
          conn = Sequel::Fdbsql::Connection.new(nil, {})
        end.should raise_error(Sequel::DatabaseError, /Unsupported.*version.*#{version}/)
      end
    end
    ['1.9.6', '1.9.7', '1.10.0', '2.0.0', '2.9.5'].each do |version|
      it "does not throw error for #{version}" do
        fake_conn {|conn| conn.stub(:query).with('SELECT 3', nil).and_return([{'_SQL_COL_1' => "FoundationDB #{version}"}])}
        conn = Sequel::Fdbsql::Connection.new(nil, {})
      end
    end
  end
end
