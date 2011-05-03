require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Database transactions" do
  before do
    @db = INTEGRATION_DB
    @db.drop_table(:items) if @db.table_exists?(:items)
    @db.create_table(:items, :engine=>'InnoDB'){String :name; Integer :value}
    @d = @db[:items]
    clear_sqls
  end
  after do
    @db.drop_table(:items) if @db.table_exists?(:items)
  end

  specify "should support transactions" do
    @db.transaction{@d << {:name => 'abc', :value => 1}}
    @d.count.should == 1
  end

  specify "should have #transaction yield the connection" do
    @db.transaction{|conn| conn.should_not == nil}
  end

  specify "should correctly rollback transactions" do
    proc do
      @db.transaction do
        @d << {:name => 'abc', :value => 1}
        raise Interrupt, 'asdf'
      end
    end.should raise_error(Interrupt)

    proc do
      @db.transaction do
        @d << {:name => 'abc', :value => 1}
        raise Sequel::Rollback
      end
    end.should_not raise_error

    @d.count.should == 0
  end

  specify "should support nested transactions" do
    @db.transaction do
      @db.transaction do
        @d << {:name => 'abc', :value => 1}
      end 
    end 
    @d.count.should == 1

    @d.delete
    proc {@db.transaction do
      @d << {:name => 'abc', :value => 1}
      @db.transaction do
        raise Sequel::Rollback
      end 
    end}.should_not raise_error
    @d.count.should == 0

    proc {@db.transaction do
      @d << {:name => 'abc', :value => 1}
      @db.transaction do
        raise Interrupt, 'asdf'
      end 
    end}.should raise_error(Interrupt)
    @d.count.should == 0
  end 
  
  if INTEGRATION_DB.supports_savepoints?
    cspecify "should support nested transactions through savepoints using the savepoint option", [:jdbc, :sqlite] do
      @db.transaction do
        @d << {:name => '1'}
        @db.transaction(:savepoint=>true) do
          @d << {:name => '2'}
          @db.transaction do
            @d << {:name => '3'}
            raise Sequel::Rollback
          end
        end
        @d << {:name => '4'}
        @db.transaction do
          @d << {:name => '6'}
          @db.transaction(:savepoint=>true) do
            @d << {:name => '7'}
            raise Sequel::Rollback
          end
        end
        @d << {:name => '5'}
      end

      @d.order(:name).map(:name).should == %w{1 4 5 6}
    end
  end

  specify "should handle returning inside of the block by committing" do
    def @db.ret_commit
      transaction do
        self[:items] << {:name => 'abc'}
        return
        self[:items] << {:name => 'd'}
      end
    end

    @d.count.should == 0
    @db.ret_commit
    @d.count.should == 1
    @db.ret_commit
    @d.count.should == 2
    proc do
      @db.transaction do
        raise Interrupt, 'asdf'
      end
    end.should raise_error(Interrupt)

    @d.count.should == 2
  end

  if INTEGRATION_DB.supports_prepared_transactions?
    specify "should commit prepared transactions using commit_prepared_transaction" do
      @db.transaction(:prepare=>'XYZ'){@d << {:name => '1'}}
      @db.commit_prepared_transaction('XYZ')
      @d.select_order_map(:name).should == ['1']
    end

    specify "should rollback prepared transactions using rollback_prepared_transaction" do
      @db.transaction(:prepare=>'XYZ'){@d << {:name => '1'}}
      @db.rollback_prepared_transaction('XYZ')
      @d.select_order_map(:name).should == []
    end
  end

  specify "should support all transaction isolation levels" do
    [:uncommitted, :committed, :repeatable, :serializable].each_with_index do |l, i|
      @db.transaction(:isolation=>l){@d << {:name => 'abc', :value => 1}}
      @d.count.should == i + 1
    end
  end

if (! defined?(RUBY_ENGINE) or RUBY_ENGINE == 'ruby' or (RUBY_ENGINE == 'rbx' && ![[:do, :sqlite], [:tinytds, :mssql]].include?([INTEGRATION_DB.adapter_scheme, INTEGRATION_DB.database_type]))) and RUBY_VERSION < '1.9'
  specify "should handle Thread#kill for transactions inside threads" do
    q = Queue.new
    q1 = Queue.new
    t = Thread.new do
      @db.transaction do
        @d << {:name => 'abc', :value => 1}
        q1.push nil
        q.pop
        @d << {:name => 'def', :value => 2}
      end
    end
    q1.pop
    t.kill
    @d.count.should == 0
  end

if INTEGRATION_DB.supports_savepoints?
  specify "should handle Thread#kill for transactions with savepoints inside threads" do
    q = Queue.new
    q1 = Queue.new
    t = Thread.new do
      @db.transaction do
        @d << {:name => 'abc', :value => 1}
        @db.transaction(:savepoint=>true) do
          @d << {:name => 'def', :value => 2}
          q1.push nil
          q.pop
          @d << {:name => 'ghi', :value => 3}
        end
        @d << {:name => 'jkl', :value => 4}
      end
    end
    q1.pop
    t.kill
    @d.count.should == 0
  end
end
end
end
