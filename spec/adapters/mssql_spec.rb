require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(MSSQL_DB)
  MSSQL_URL = 'jdbc:sqlserver://localhost;integratedSecurity=true;database=sandbox' unless defined? MSSQL_URL
  MSSQL_DB = Sequel.connect(ENV['SEQUEL_MSSQL_SPEC_DB']||MSSQL_URL)
end
INTEGRATION_DB = MSSQL_DB unless defined?(INTEGRATION_DB)

context "A MSSQL database" do
  before do
    @db = MSSQL_DB
  end

  specify "should be able to read fractional part of timestamp" do
    rs = @db["select getutcdate() as full_date, cast(datepart(millisecond, getutcdate()) as int) as milliseconds"].first
    rs[:milliseconds].should == rs[:full_date].usec/1000
  end

  specify "should be able to write fractional part of timestamp" do
    t = Time.utc(2001, 12, 31, 23, 59, 59, 997000)
    (t.usec/1000).should == @db["select cast(datepart(millisecond, ?) as int) as milliseconds", t].get
  end
  
  specify "should not raise an error when getting the server version" do
    proc{@db.server_version}.should_not raise_error
    proc{@db.dataset.server_version}.should_not raise_error
  end
end

context "MSSQL Dataset#output" do
  before do
    @db = MSSQL_DB
    @db.create_table!(:items){String :name; Integer :value}
    @db.create_table!(:out){String :name; Integer :value}
    @ds = @db[:items]
  end
  after do
    @db.drop_table(:items)
    @db.drop_table(:out)
  end

  specify "should format OUTPUT clauses for DELETE statements" do
    @ds.output(:out, [:deleted__name, :deleted__value]).delete_sql.should =~
      /DELETE FROM ITEMS OUTPUT DELETED.(NAME|VALUE), DELETED.(NAME|VALUE) INTO OUT/
    @ds.output(:out, {:name => :deleted__name, :value => :deleted__value}).delete_sql.should =~
      /DELETE FROM ITEMS OUTPUT DELETED.(NAME|VALUE), DELETED.(NAME|VALUE) INTO OUT \((NAME|VALUE), (NAME|VALUE)\)/
  end

  specify "should format OUTPUT clauses for INSERT statements" do
    @ds.output(:out, [:inserted__name, :inserted__value]).insert_sql(:name => "name", :value => 1).should =~
      /INSERT INTO ITEMS \((NAME|VALUE), (NAME|VALUE)\) OUTPUT INSERTED.(NAME|VALUE), INSERTED.(NAME|VALUE) INTO OUT VALUES \((N'name'|1), (N'name'|1)\)/
    @ds.output(:out, {:name => :inserted__name, :value => :inserted__value}).insert_sql(:name => "name", :value => 1).should =~
      /INSERT INTO ITEMS \((NAME|VALUE), (NAME|VALUE)\) OUTPUT INSERTED.(NAME|VALUE), INSERTED.(NAME|VALUE) INTO OUT \((NAME|VALUE), (NAME|VALUE)\) VALUES \((N'name'|1), (N'name'|1)\)/
  end

  specify "should format OUTPUT clauses for UPDATE statements" do
    @ds.output(:out, [:inserted__name, :deleted__value]).update_sql(:value => 2).should =~
      /UPDATE ITEMS SET VALUE = 2 OUTPUT (INSERTED.NAME|DELETED.VALUE), (INSERTED.NAME|DELETED.VALUE) INTO OUT/
    @ds.output(:out, {:name => :inserted__name, :value => :deleted__value}).update_sql(:value => 2).should =~
      /UPDATE ITEMS SET VALUE = 2 OUTPUT (INSERTED.NAME|DELETED.VALUE), (INSERTED.NAME|DELETED.VALUE) INTO OUT \((NAME|VALUE), (NAME|VALUE)\)/
  end

  specify "should execute OUTPUT clauses in DELETE statements" do
    @ds.insert(:name => "name", :value => 1)
    @ds.output(:out, [:deleted__name, :deleted__value]).delete
    @db[:out].all.should == [{:name => "name", :value => 1}]
    @ds.insert(:name => "name", :value => 2)
    @ds.output(:out, {:name => :deleted__name, :value => :deleted__value}).delete
    @db[:out].all.should == [{:name => "name", :value => 1}, {:name => "name", :value => 2}]
  end

  specify "should execute OUTPUT clauses in INSERT statements" do
    @ds.output(:out, [:inserted__name, :inserted__value]).insert(:name => "name", :value => 1)
    @db[:out].all.should == [{:name => "name", :value => 1}]
    @ds.output(:out, {:name => :inserted__name, :value => :inserted__value}).insert(:name => "name", :value => 2)
    @db[:out].all.should == [{:name => "name", :value => 1}, {:name => "name", :value => 2}]
  end

  specify "should execute OUTPUT clauses in UPDATE statements" do
    @ds.insert(:name => "name", :value => 1)
    @ds.output(:out, [:inserted__name, :deleted__value]).update(:value => 2)
    @db[:out].all.should == [{:name => "name", :value => 1}]
    @ds.output(:out, {:name => :inserted__name, :value => :deleted__value}).update(:value => 3)
    @db[:out].all.should == [{:name => "name", :value => 1}, {:name => "name", :value => 2}]
  end
end

context "MSSQL joined datasets" do
  before do
    @db = MSSQL_DB
  end

  specify "should format DELETE statements" do
    @db[:t1].inner_join(:t2, :t1__pk => :t2__pk).delete_sql.should ==
      "DELETE FROM T1 FROM T1 INNER JOIN T2 ON (T1.PK = T2.PK)"
  end

  specify "should format UPDATE statements" do
    @db[:t1].inner_join(:t2, :t1__pk => :t2__pk).update_sql(:pk => :t2__pk).should ==
      "UPDATE T1 SET PK = T2.PK FROM T1 INNER JOIN T2 ON (T1.PK = T2.PK)"
  end
end
