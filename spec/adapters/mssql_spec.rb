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

  specify "read milliseconds" do
    rs = @db["select getutcdate() as full_date, datepart(millisecond, getutcdate()) as milliseconds"].first
    rs[:milliseconds].should == rs[:full_date].usec/1000
  end

  specify "write milliseconds" do
    t = Time.utc(9999, 12, 31, 23, 59, 59, 997000)
    @db["select cast(datepart(millisecond, ?) as int) as milliseconds", t].get.should == t.usec/1000
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
    @ds.output(:out, [:deleted__name, :deleted__value]).delete_sql.should ==
      "DELETE FROM ITEMS OUTPUT DELETED.NAME, DELETED.VALUE INTO OUT"
    @ds.output(:out, {:name => :deleted__name, :value => :deleted__value}).delete_sql.should ==
      "DELETE FROM ITEMS OUTPUT DELETED.NAME, DELETED.VALUE INTO OUT (NAME, VALUE)"
  end

  specify "should format OUTPUT clauses for INSERT statements" do
    @ds.output(:out, [:inserted__name, :inserted__value]).insert_sql(:name => "name", :value => 1).should ==
      "INSERT INTO ITEMS (NAME, VALUE) OUTPUT INSERTED.NAME, INSERTED.VALUE INTO OUT VALUES (N'name', 1)"
    @ds.output(:out, {:name => :inserted__name, :value => :inserted__value}).insert_sql(:name => "name", :value => 1).should ==
      "INSERT INTO ITEMS (NAME, VALUE) OUTPUT INSERTED.NAME, INSERTED.VALUE INTO OUT (NAME, VALUE) VALUES (N'name', 1)"
  end

  specify "should format OUTPUT clauses for UPDATE statements" do
    @ds.output(:out, [:inserted__name, :deleted__value]).update_sql(:value => 2).should ==
      "UPDATE ITEMS SET VALUE = 2 OUTPUT INSERTED.NAME, DELETED.VALUE INTO OUT"
    @ds.output(:out, {:name => :inserted__name, :value => :deleted__value}).update_sql(:value => 2).should ==
      "UPDATE ITEMS SET VALUE = 2 OUTPUT INSERTED.NAME, DELETED.VALUE INTO OUT (NAME, VALUE)"
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
