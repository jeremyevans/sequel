require File.join(File.dirname(__FILE__), 'spec_helper')

context "A worker" do
  setup do
    @w = Sequel::Worker.new
  end
  
  teardown do
    sleep 0.1
    @w.join if @w
  end
  
  specify "should be a thread" do
    @w.should be_a_kind_of(Thread)
  end
  
  specify "should be alive until it is joined" do
    @w.should be_alive
  end
  
  specify "should be busy if any jobs are pending" do
    @w.should_not be_busy
    @w.add {sleep 0.5}
    @w.should be_busy
  end
  
  specify "should accept jobs and perform them in the correct order" do
    values = []
    @w.add {values << 1}
    @w.async {values << 2}
    @w << proc {values << 3}
    
    @w.join
    values.should == [1, 2, 3]
  end
  
  specify "should isolate errors and hold them in #errors" do
    values = []
    @w.add {values << 1}
    @w.async {values << 2}
    @w.async {raise "bad bad bad"}
    @w << proc {values << 3}
    
    @w.join
    values.should == [1, 2, 3]
    @w.errors.size.should == 1
    @w.errors.first.message.should == 'bad bad bad'
  end
end

context "A worker with a given db" do
  setup do
    @db = MockDatabase.new
    @m = Module.new do
      def transaction
        execute('BEGIN')
        yield
        execute('COMMIT')
      rescue
        execute('ROLLBACK')
      end
    end
    @db.extend(@m)
    @w = Sequel::Worker.new(@db)
  end
  
  teardown do
    @w.join if @w
  end
  
  specify "should wrap everything in a transaction" do
    @w.async {@db[:items] << {:x => 1}}
    @w.join
    @w = nil
    @db.sqls.should == [
      'BEGIN',
      'INSERT INTO items (x) VALUES (1)',
      'COMMIT'
    ]
  end

  specify "should rollback the transaction if any error is raised" do
    @w.async {@db[:items] << {:x => 1}}
    @w.async {sleep 0.2; raise "that's bad"}
    @w.async {@db[:items] << {:x => 2}}
    @w.join
    @db.sqls.should == [
      'BEGIN',
      'INSERT INTO items (x) VALUES (1)',
      'INSERT INTO items (x) VALUES (2)',
      'ROLLBACK'
    ]
    @w.errors.size.should == 1
    @w.errors.first.message.should == "that's bad"
  end
end