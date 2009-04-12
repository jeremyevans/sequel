require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe "Database transactions" do
  before do
    INTEGRATION_DB.drop_table(:items) if INTEGRATION_DB.table_exists?(:items)
    INTEGRATION_DB.create_table(:items){String :name; Integer :value}
    @d = INTEGRATION_DB[:items]
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items) if INTEGRATION_DB.table_exists?(:items)
  end

  specify "should support transactions" do
    INTEGRATION_DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.should == 1
  end

  specify "should have #transaction yield the connection" do
    INTEGRATION_DB.transaction do |conn|
      conn.should_not == nil
    end
  end

  specify "should correctly rollback transactions" do
    proc do
      INTEGRATION_DB.transaction do
        @d << {:name => 'abc', :value => 1}
        raise Interrupt, 'asdf'
      end
    end.should raise_error(Interrupt)

    proc do
      INTEGRATION_DB.transaction do
        @d << {:name => 'abc', :value => 1}
        raise Sequel::Rollback
      end
    end.should_not raise_error

    @d.count.should == 0
  end

  specify "should handle returning inside of the block by committing" do
    def INTEGRATION_DB.ret_commit
      transaction do
        self[:items] << {:name => 'abc'}
        return
        self[:items] << {:name => 'd'}
      end
    end

    @d.count.should == 0
    INTEGRATION_DB.ret_commit
    @d.count.should == 1
    INTEGRATION_DB.ret_commit
    @d.count.should == 2
    proc do
      INTEGRATION_DB.transaction do
        raise Interrupt, 'asdf'
      end
    end.should raise_error(Interrupt)

    @d.count.should == 2
  end
end
