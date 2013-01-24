require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe Sequel::Database do
  before do
    @db = INTEGRATION_DB
  end

  specify "should provide disconnect functionality" do
    @db.disconnect
    @db.pool.size.should == 0
    @db.test_connection
    @db.pool.size.should == 1
  end

  specify "should provide disconnect functionality after preparing a statement" do
    @db.create_table!(:items){Integer :i}
    @db[:items].prepare(:first, :a).call
    @db.disconnect
    @db.pool.size.should == 0
    @db.drop_table?(:items)
  end

  specify "should raise Sequel::DatabaseError on invalid SQL" do
    proc{@db << "SELECT"}.should raise_error(Sequel::DatabaseError)
  end

  describe "constraint violations" do
    before do
      @db.drop_table?(:test2, :test)
    end
    after do
      @db.drop_table?(:test2, :test)
    end

    cspecify "should raise Sequel::UniqueConstraintViolation when a unique constraint is violated", [:jdbc, :sqlite], [:db2] do
      @db.create_table!(:test){String :a, :unique=>true, :null=>false}
      @db[:test].insert('1')
      proc{@db[:test].insert('1')}.should raise_error(Sequel::UniqueConstraintViolation)
      @db[:test].insert('2')
      proc{@db[:test].update(:a=>'1')}.should raise_error(Sequel::UniqueConstraintViolation)
    end

    cspecify "should raise Sequel::CheckConstraintViolation when a check constraint is violated", :mysql, [:jdbc, :sqlite], [:db2] do
      @db.create_table!(:test){String :a; check Sequel.~(:a=>'1')}
      proc{@db[:test].insert('1')}.should raise_error(Sequel::CheckConstraintViolation)
      @db[:test].insert('2')
      proc{@db[:test].insert('1')}.should raise_error(Sequel::CheckConstraintViolation)
    end

    cspecify "should raise Sequel::ForeignKeyConstraintViolation when a foreign key constraint is violated", [:jdbc, :sqlite], [:db2]  do
      @db.create_table!(:test, :engine=>:InnoDB){primary_key :id}
      @db.create_table!(:test2, :engine=>:InnoDB){foreign_key :tid, :test}
      proc{@db[:test2].insert(:tid=>1)}.should raise_error(Sequel::ForeignKeyConstraintViolation)
      @db[:test].insert
      @db[:test2].insert(:tid=>1)
      proc{@db[:test2].where(:tid=>1).update(:tid=>3)}.should raise_error(Sequel::ForeignKeyConstraintViolation)
      proc{@db[:test].where(:id=>1).delete}.should raise_error(Sequel::ForeignKeyConstraintViolation)
    end

    cspecify "should raise Sequel::NotNullConstraintViolation when a not null constraint is violated", [:jdbc, :sqlite], [:db2] do
      @db.create_table!(:test){Integer :a, :null=>false}
      proc{@db[:test].insert(:a=>nil)}.should raise_error(Sequel::NotNullConstraintViolation)
      unless @db.database_type == :mysql
        # Broken mysql silently changes NULL here to 0, and doesn't raise an exception.
        @db[:test].insert(2)
        proc{@db[:test].update(:a=>nil)}.should raise_error(Sequel::NotNullConstraintViolation)
      end
    end
  end

  specify "should store underlying wrapped exception in Sequel::DatabaseError" do
    begin
      @db << "SELECT"
    rescue Sequel::DatabaseError=>e
      if defined?(Java::JavaLang::Exception)
        (e.wrapped_exception.is_a?(Exception) || e.wrapped_exception.is_a?(Java::JavaLang::Exception)).should be_true
      else
        e.wrapped_exception.should be_a_kind_of(Exception)
      end
    end
  end

  specify "should not have the connection pool swallow non-StandardError based exceptions" do
    proc{@db.pool.hold{raise Interrupt, "test"}}.should raise_error(Interrupt)
  end

  specify "should be able to disconnect connections more than once without exceptions" do
    conn = @db.synchronize{|c| c}
    @db.disconnect
    @db.disconnect_connection(conn)
    @db.disconnect_connection(conn)
  end

  cspecify "should provide ability to check connections for validity", [:do, :postgres] do
    conn = @db.synchronize{|c| c}
    @db.valid_connection?(conn).should be_true
    @db.disconnect
    @db.valid_connection?(conn).should be_false
  end
end
