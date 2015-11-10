require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Database transactions" do
  before(:all) do
    @db = DB
    @db.create_table!(:items, :engine=>'InnoDB'){String :name; Integer :value}
    @d = @db[:items]
  end
  before do
    @d.delete
  end
  after(:all) do
    @db.drop_table?(:items)
  end

  it "should support transactions" do
    @db.transaction{@d << {:name => 'abc', :value => 1}}
    @d.count.must_equal 1
  end

  it "should have #transaction yield the connection" do
    @db.transaction{|conn| conn.wont_equal nil}
  end

  it "should have #in_transaction? work correctly" do
    @db.in_transaction?.must_equal false
    c = nil
    @db.transaction{c = @db.in_transaction?}
    c.must_equal true
  end

  it "should correctly rollback transactions" do
    proc do
      @db.transaction do
        @d << {:name => 'abc', :value => 1}
        raise Interrupt, 'asdf'
      end
    end.must_raise(Interrupt)

    @db.transaction do
      @d << {:name => 'abc', :value => 1}
      raise Sequel::Rollback
    end.must_equal nil

    proc do
      @db.transaction(:rollback=>:reraise) do
        @d << {:name => 'abc', :value => 1}
        raise Sequel::Rollback
      end
    end.must_raise(Sequel::Rollback)

    @db.transaction(:rollback=>:always) do
      @d << {:name => 'abc', :value => 1}
      2
    end.must_equal 2

    @d.count.must_equal 0
  end

  it "should support nested transactions" do
    @db.transaction do
      @db.transaction do
        @d << {:name => 'abc', :value => 1}
      end 
    end 
    @d.count.must_equal 1

    @d.delete
    @db.transaction do
      @d << {:name => 'abc', :value => 1}
      @db.transaction do
        raise Sequel::Rollback
      end 
    end
    @d.count.must_equal 0

    proc {@db.transaction do
      @d << {:name => 'abc', :value => 1}
      @db.transaction do
        raise Interrupt, 'asdf'
      end 
    end}.must_raise(Interrupt)
    @d.count.must_equal 0
  end 
  
  if DB.supports_savepoints?
    it "should handle table_exists? failures inside transactions" do
      @db.transaction do
        @d << {:name => '1'}
        @db.table_exists?(:asadf098asd9asd98sa).must_equal false
        @d << {:name => '2'}
      end
      @d.select_order_map(:name).must_equal %w'1 2'
    end

    it "should handle table_exists? failures inside savepoints" do
      @db.transaction do
        @d << {:name => '1'}
        @db.transaction(:savepoint=>true) do
          @d << {:name => '2'}
          @db.table_exists?(:asadf098asd9asd98sa).must_equal false
          @d << {:name => '3'}
        end
        @d << {:name => '4'}
      end
      @d.select_order_map(:name).must_equal %w'1 2 3 4'
    end

    it "should support nested transactions through savepoints using the savepoint option" do
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

      @d.order(:name).map(:name).must_equal %w{1 4 5 6}
    end

    it "should support nested transactions through savepoints using the auto_savepoint option" do
      @db.transaction(:auto_savepoint=>true) do
        @d << {:name => '1'}
        @db.transaction do
          @d << {:name => '2'}
          @db.transaction do
            @d << {:name => '3'}
            raise Sequel::Rollback
          end
        end
        @d << {:name => '4'}
        @db.transaction(:auto_savepoint=>true) do
          @d << {:name => '6'}
          @db.transaction do
            @d << {:name => '7'}
            raise Sequel::Rollback
          end
        end
        @d << {:name => '5'}
      end

      @d.order(:name).map(:name).must_equal %w{1 4 5 6}
    end
  end

  it "should handle returning inside of the block by committing" do
    def @db.ret_commit
      transaction do
        self[:items] << {:name => 'abc'}
        return
      end
    end

    @d.count.must_equal 0
    @db.ret_commit
    @d.count.must_equal 1
    @db.ret_commit
    @d.count.must_equal 2
    proc do
      @db.transaction do
        raise Interrupt, 'asdf'
      end
    end.must_raise(Interrupt)

    @d.count.must_equal 2
  end

  if DB.supports_prepared_transactions?
    it "should allow saving and destroying of model objects" do
      c = Class.new(Sequel::Model(@d))
      c.set_primary_key :name
      c.unrestrict_primary_key
      c.use_after_commit_rollback = false
      @db.transaction(:prepare=>'XYZ'){c.create(:name => '1'); c.create(:name => '2').destroy}
      @db.commit_prepared_transaction('XYZ')
      @d.select_order_map(:name).must_equal ['1']
    end

    it "should commit prepared transactions using commit_prepared_transaction" do
      @db.transaction(:prepare=>'XYZ'){@d << {:name => '1'}}
      @db.commit_prepared_transaction('XYZ')
      @d.select_order_map(:name).must_equal ['1']
    end

    it "should rollback prepared transactions using rollback_prepared_transaction" do
      @db.transaction(:prepare=>'XYZ'){@d << {:name => '1'}}
      @db.rollback_prepared_transaction('XYZ')
      @d.select_order_map(:name).must_equal []
    end

    if DB.supports_savepoints_in_prepared_transactions?
      it "should support savepoints when using prepared transactions" do
        @db.transaction(:prepare=>'XYZ'){@db.transaction(:savepoint=>true){@d << {:name => '1'}}}
        @db.commit_prepared_transaction('XYZ')
        @d.select_order_map(:name).must_equal ['1']
      end
    end
  end

  it "should support all transaction isolation levels" do
    [:uncommitted, :committed, :repeatable, :serializable].each_with_index do |l, i|
      @db.transaction(:isolation=>l){@d << {:name => 'abc', :value => 1}}
      @d.count.must_equal i + 1
    end
  end

  it "should support after_commit outside transactions" do
    c = nil
    @db.after_commit{c = 1}
    c.must_equal 1
  end

  it "should support after_rollback outside transactions" do
    c = nil
    @db.after_rollback{c = 1}
    c.must_equal nil
  end

  it "should support after_commit inside transactions" do
    c = nil
    @db.transaction{@db.after_commit{c = 1}; c.must_equal nil}
    c.must_equal 1
  end

  it "should support after_rollback inside transactions" do
    c = nil
    @db.transaction{@db.after_rollback{c = 1}; c.must_equal nil}
    c.must_equal nil
  end

  it "should not call after_commit if the transaction rolls back" do
    c = nil
    @db.transaction{@db.after_commit{c = 1}; c.must_equal nil; raise Sequel::Rollback}
    c.must_equal nil
  end

  it "should call after_rollback if the transaction rolls back" do
    c = nil
    @db.transaction{@db.after_rollback{c = 1}; c.must_equal nil; raise Sequel::Rollback}
    c.must_equal 1
  end

  it "should support multiple after_commit blocks inside transactions" do
    c = []
    @db.transaction{@db.after_commit{c << 1}; @db.after_commit{c << 2}; c.must_equal []}
    c.must_equal [1, 2]
  end

  it "should support multiple after_rollback blocks inside transactions" do
    c = []
    @db.transaction{@db.after_rollback{c << 1}; @db.after_rollback{c << 2}; c.must_equal []; raise Sequel::Rollback}
    c.must_equal [1, 2]
  end

  it "should support after_commit inside nested transactions" do
    c = nil
    @db.transaction{@db.transaction{@db.after_commit{c = 1}}; c.must_equal nil}
    c.must_equal 1
  end

  it "should support after_rollback inside nested transactions" do
    c = nil
    @db.transaction{@db.transaction{@db.after_rollback{c = 1}}; c.must_equal nil; raise Sequel::Rollback}
    c.must_equal 1
  end

  if DB.supports_savepoints?
    it "should support after_commit inside savepoints" do
      c = nil
      @db.transaction{@db.transaction(:savepoint=>true){@db.after_commit{c = 1}}; c.must_equal nil}
      c.must_equal 1
    end

    it "should support after_rollback inside savepoints" do
      c = nil
      @db.transaction{@db.transaction(:savepoint=>true){@db.after_rollback{c = 1}}; c.must_equal nil; raise Sequel::Rollback}
      c.must_equal 1
    end
  end

  if DB.supports_prepared_transactions?
    it "should raise an error if you attempt to use after_commit or after_rollback inside a prepared transaction" do
      proc{@db.transaction(:prepare=>'XYZ'){@db.after_commit{}}}.must_raise(Sequel::Error)
      proc{@db.transaction(:prepare=>'XYZ'){@db.after_rollback{}}}.must_raise(Sequel::Error)
    end

    if DB.supports_savepoints_in_prepared_transactions?
      it "should raise an error if you attempt to use after_commit or after rollback inside a savepoint in a prepared transaction" do
        proc{@db.transaction(:prepare=>'XYZ'){@db.transaction(:savepoint=>true){@db.after_commit{}}}}.must_raise(Sequel::Error)
        proc{@db.transaction(:prepare=>'XYZ'){@db.transaction(:savepoint=>true){@db.after_rollback{}}}}.must_raise(Sequel::Error)
      end
    end
  end
end

if (! defined?(RUBY_ENGINE) or RUBY_ENGINE == 'ruby' or (RUBY_ENGINE == 'rbx' && !Sequel.guarded?([:do, :sqlite], [:tinytds, :mssql]))) and RUBY_VERSION < '1.9'
  describe "Database transactions and Thread#kill" do
    before do
      @db = DB
      @db.create_table!(:items, :engine=>'InnoDB'){String :name; Integer :value}
      @d = @db[:items]
    end
    after do
      @db.drop_table?(:items)
    end

    it "should handle transactions inside threads" do
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
      @d.count.must_equal 0
    end

    if DB.supports_savepoints?
      it "should handle transactions with savepoints inside threads" do
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
        @d.count.must_equal 0
      end
    end
  end
end

describe "Database transaction retrying" do
  before(:all) do
    @db = DB
    @db.create_table!(:items, :engine=>'InnoDB'){String :a, :unique=>true, :null=>false}
    @d = @db[:items]
  end
  before do
    @d.delete
  end
  after(:all) do
    @db.drop_table?(:items)
  end

  cspecify "should be supported using the :retry_on option", [:db2] do
    @d.insert('b')
    @d.insert('c')
    s = 'a'
    @db.transaction(:retry_on=>Sequel::ConstraintViolation) do
      s = s.succ
      @d.insert(s)
    end
    @d.select_order_map(:a).must_equal %w'b c d'
  end

  cspecify "should limit number of retries via the :num_retries option", [:db2] do
    @d.insert('b')
    @d.insert('c')
    s = 'a'
    lambda do
      @db.transaction(:num_retries=>1, :retry_on=>Sequel::ConstraintViolation) do
        s = s.succ
        @d.insert(s)
      end
    end.must_raise(Sequel::UniqueConstraintViolation, Sequel::ConstraintViolation)
    @d.select_order_map(:a).must_equal %w'b c'
  end
end

