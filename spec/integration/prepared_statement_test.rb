require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Prepared Statements and Bound Arguments" do
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      integer :numb
    end
    @c = Class.new(Sequel::Model(:items))
    @ds = @db[:items]
    @ds.insert(:numb=>10)
  end
  after do
    @db.drop_table(:items)
  end
  
  specify "should support bound variables with select, all, and first" do
    @ds.filter(:numb=>:$n).call(:select, :n=>10).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).call(:all, :n=>10).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).call(:first, :n=>10).should == {:id=>1, :numb=>10}
  end
    
  specify "should support blocks for select and all" do
    @ds.filter(:numb=>:$n).call(:select, :n=>10){|r| r[:numb] *= 2}.should == [{:id=>1, :numb=>20}]
    @ds.filter(:numb=>:$n).call(:all, :n=>10){|r| r[:numb] *= 2}.should == [{:id=>1, :numb=>20}]
  end
    
  specify "should support binding variables before the call with #bind" do
    @ds.filter(:numb=>:$n).bind(:n=>10).call(:select).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>10).call(:all).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>10).call(:first).should == {:id=>1, :numb=>10}
    
    @ds.bind(:n=>10).filter(:numb=>:$n).call(:select).should == [{:id=>1, :numb=>10}]
    @ds.bind(:n=>10).filter(:numb=>:$n).call(:all).should == [{:id=>1, :numb=>10}]
    @ds.bind(:n=>10).filter(:numb=>:$n).call(:first).should == {:id=>1, :numb=>10}
  end
  
  specify "should allow overriding variables specified with #bind" do
    @ds.filter(:numb=>:$n).bind(:n=>1).call(:select, :n=>10).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).call(:all, :n=>10).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).call(:first, :n=>10).should == {:id=>1, :numb=>10}
    
    @ds.filter(:numb=>:$n).bind(:n=>1).bind(:n=>10).call(:select).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).bind(:n=>10).call(:all).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).bind(:n=>1).bind(:n=>10).call(:first).should == {:id=>1, :numb=>10}
  end

  specify "should support placeholder literal strings with call" do
    @ds.filter("numb = ?", :$n).call(:select, :n=>10).should == [{:id=>1, :numb=>10}]
  end

  specify "should support named placeholder literal strings and handle multiple named placeholders correctly with call" do
    @ds.filter("numb = :n", :n=>:$n).call(:select, :n=>10).should == [{:id=>1, :numb=>10}]
    @ds.insert(:numb=>20)
    @ds.insert(:numb=>30)
    @ds.filter("numb > :n1 AND numb < :n2 AND numb = :n3", :n3=>:$n3, :n2=>:$n2, :n1=>:$n1).call(:select, :n3=>20, :n2=>30, :n1=>10).should == [{:id=>2, :numb=>20}]
  end

  specify "should support datasets with static sql and placeholders with call" do
    @db["SELECT * FROM items WHERE numb = ?", :$n].call(:select, :n=>10).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects with call" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n)).filter(:id=>:$j).call(:select, :n=>10, :i=>1, :j=>1).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects with literal strings with call" do
    @ds.filter(:id=>:$i, :numb=>@ds.select(:numb).filter("numb = ?", :$n)).call(:select, :n=>10, :i=>1).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects with static sql and placeholders with call" do
    @ds.filter(:id=>:$i, :numb=>@db["SELECT numb FROM items WHERE numb = ?", :$n]).call(:select, :n=>10, :i=>1).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects of subselects with call" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n))).filter(:id=>:$j).call(:select, :n=>10, :i=>1, :j=>1).should == [{:id=>1, :numb=>10}]
  end
  
  cspecify "should support using a bound variable for a limit and offset", [:jdbc, :db2] do
    @ds.insert(:numb=>20)
    ds = @ds.limit(:$n, :$n2).order(:id)
    ds.call(:select, :n=>1, :n2=>0).should == [{:id=>1, :numb=>10}]
    ds.call(:select, :n=>1, :n2=>1).should == [{:id=>2, :numb=>20}]
    ds.call(:select, :n=>1, :n2=>2).should == []
    ds.call(:select, :n=>2, :n2=>0).should == [{:id=>1, :numb=>10}, {:id=>2, :numb=>20}]
    ds.call(:select, :n=>2, :n2=>1).should == [{:id=>2, :numb=>20}]
  end

  specify "should support bound variables with insert" do
    @ds.call(:insert, {:n=>20}, :numb=>:$n)
    @ds.count.should == 2
    @ds.order(:id).map(:numb).should == [10, 20]
  end

  specify "should support bound variables with NULL values" do
    @ds.delete
    @ds.call(:insert, {:n=>nil}, :numb=>:$n)
    @ds.count.should == 1
    @ds.map(:numb).should == [nil]
  end

  specify "should have insert return primary key value when using bound arguments" do
    @ds.call(:insert, {:n=>20}, :numb=>:$n).should == 2
    @ds.filter(:id=>2).first[:numb].should == 20
  end

  specify "should support bound variables with delete" do
    @ds.filter(:numb=>:$n).call(:delete, :n=>10).should == 1
    @ds.count.should == 0
  end

  specify "should support bound variables with update" do
    @ds.filter(:numb=>:$n).call(:update, {:n=>10, :nn=>20}, :numb=>:numb+:$nn).should == 1
    @ds.all.should == [{:id=>1, :numb=>30}]
  end
  
  specify "should support prepared statements with select, first, and all" do
    @ds.filter(:numb=>:$n).prepare(:select, :select_n)
    @db.call(:select_n, :n=>10).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).prepare(:all, :select_n)
    @db.call(:select_n, :n=>10).should == [{:id=>1, :numb=>10}]
    @ds.filter(:numb=>:$n).prepare(:first, :select_n)
    @db.call(:select_n, :n=>10).should == {:id=>1, :numb=>10}
  end

  specify "should support prepared statements being call multiple times with different arguments" do
    @ds.filter(:numb=>:$n).prepare(:select, :select_n)
    @db.call(:select_n, :n=>10).should == [{:id=>1, :numb=>10}]
    @db.call(:select_n, :n=>0).should == []
    @db.call(:select_n, :n=>10).should == [{:id=>1, :numb=>10}]
  end

  specify "should support placeholder literal strings with prepare" do
    @ds.filter("numb = ?", :$n).prepare(:select, :seq_select).call(:n=>10).should == [{:id=>1, :numb=>10}]
  end

  specify "should support named placeholder literal strings and handle multiple named placeholders correctly with prepare" do
    @ds.filter("numb = :n", :n=>:$n).prepare(:select, :seq_select).call(:n=>10).should == [{:id=>1, :numb=>10}]
    @ds.insert(:numb=>20)
    @ds.insert(:numb=>30)
    @ds.filter("numb > :n1 AND numb < :n2 AND numb = :n3", :n3=>:$n3, :n2=>:$n2, :n1=>:$n1).call(:select, :n3=>20, :n2=>30, :n1=>10).should == [{:id=>2, :numb=>20}]
  end

  specify "should support datasets with static sql and placeholders with prepare" do
    @db["SELECT * FROM items WHERE numb = ?", :$n].prepare(:select, :seq_select).call(:n=>10).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects with prepare" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n)).filter(:id=>:$j).prepare(:select, :seq_select).call(:n=>10, :i=>1, :j=>1).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects with literal strings with prepare" do
    @ds.filter(:id=>:$i, :numb=>@ds.select(:numb).filter("numb = ?", :$n)).prepare(:select, :seq_select).call(:n=>10, :i=>1).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects with static sql and placeholders with prepare" do
    @ds.filter(:id=>:$i, :numb=>@db["SELECT numb FROM items WHERE numb = ?", :$n]).prepare(:select, :seq_select).call(:n=>10, :i=>1).should == [{:id=>1, :numb=>10}]
  end

  specify "should support subselects of subselects with prepare" do
    @ds.filter(:id=>:$i).filter(:numb=>@ds.select(:numb).filter(:numb=>@ds.select(:numb).filter(:numb=>:$n))).filter(:id=>:$j).prepare(:select, :seq_select).call(:n=>10, :i=>1, :j=>1).should == [{:id=>1, :numb=>10}]
  end
  
  cspecify "should support using a prepared_statement for a limit and offset", :db2 do
    @ds.insert(:numb=>20)
    ps = @ds.limit(:$n, :$n2).order(:id).prepare(:select, :seq_select)
    ps.call(:n=>1, :n2=>0).should == [{:id=>1, :numb=>10}]
    ps.call(:n=>1, :n2=>1).should == [{:id=>2, :numb=>20}]
    ps.call(:n=>1, :n2=>2).should == []
    ps.call(:n=>2, :n2=>0).should == [{:id=>1, :numb=>10}, {:id=>2, :numb=>20}]
    ps.call(:n=>2, :n2=>1).should == [{:id=>2, :numb=>20}]
  end

  specify "should support prepared statements with insert" do
    @ds.prepare(:insert, :insert_n, :numb=>:$n)
    @db.call(:insert_n, :n=>20)
    @ds.count.should == 2
    @ds.order(:id).map(:numb).should == [10, 20]
  end

  specify "should support prepared statements with NULL values" do
    @ds.delete
    @ds.prepare(:insert, :insert_n, :numb=>:$n)
    @db.call(:insert_n, :n=>nil)
    @ds.count.should == 1
    @ds.map(:numb).should == [nil]
  end

  specify "should have insert return primary key value when using prepared statements" do
    @ds.prepare(:insert, :insert_n, :numb=>:$n)
    @db.call(:insert_n, :n=>20).should == 2
    @ds.filter(:id=>2).first[:numb].should == 20
  end

  specify "should support prepared statements with delete" do
    @ds.filter(:numb=>:$n).prepare(:delete, :delete_n)
    @db.call(:delete_n, :n=>10).should == 1
    @ds.count.should == 0
  end

  specify "should support prepared statements with update" do
    @ds.filter(:numb=>:$n).prepare(:update, :update_n, :numb=>:numb+:$nn)
    @db.call(:update_n, :n=>10, :nn=>20).should == 1
    @ds.all.should == [{:id=>1, :numb=>30}]
  end
  
  specify "model datasets should return model instances when using select, all, and first with bound variables" do
    @c.filter(:numb=>:$n).call(:select, :n=>10).should == [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).call(:all, :n=>10).should == [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).call(:first, :n=>10).should == @c.load(:id=>1, :numb=>10)
  end
  
  specify "model datasets should return model instances when using select, all, and first with prepared statements" do
    @c.filter(:numb=>:$n).prepare(:select, :select_n1)
    @db.call(:select_n1, :n=>10).should == [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).prepare(:all, :select_n1)
    @db.call(:select_n1, :n=>10).should == [@c.load(:id=>1, :numb=>10)]
    @c.filter(:numb=>:$n).prepare(:first, :select_n1)
    @db.call(:select_n1, :n=>10).should == @c.load(:id=>1, :numb=>10)
  end
end

describe "Bound Argument Types" do
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      Date :d
      DateTime :dt
      File :file
      String :s
      Time :t
      Float :f
      TrueClass :b
    end
    @ds = @db[:items]
    @vs = {:d=>Date.civil(2010, 10, 11), :dt=>DateTime.civil(2010, 10, 12, 13, 14, 15), :f=>1.0, :s=>'str', :t=>Time.at(20101010), :file=>Sequel::SQL::Blob.new('blob'), :b=>true}
    @ds.insert(@vs)
    @ds.meta_def(:ba) do |sym, type|
      prepared_arg_placeholder == '$' ? :"#{sym}__#{type}" : sym
    end
  end
  after do
    Sequel.datetime_class = Time
    @db.drop_table(:items)
  end

  cspecify "should handle date type", [:do, :sqlite], :mssql, [:jdbc, :sqlite], :oracle do 
    @ds.filter(:d=>:$x).prepare(:first, :ps_date).call(:x=>@vs[:d])[:d].should == @vs[:d]
  end

  cspecify "should handle datetime type", [:do], [:mysql2], [:swift], [:jdbc, :sqlite], [:tinytds], [:oracle] do
    Sequel.datetime_class = DateTime
    @ds.filter(:dt=>:$x).prepare(:first, :ps_datetime).call(:x=>@vs[:dt])[:dt].should == @vs[:dt]
  end

  cspecify "should handle time type", [:do], [:jdbc, :sqlite] do
    @ds.filter(:t=>:$x).prepare(:first, :ps_time).call(:x=>@vs[:t])[:t].should == @vs[:t]
  end

  cspecify "should handle blob type", [:swift], [:odbc], [:jdbc, :db2], :oracle do
    @ds.filter(:file=>:$x).prepare(:first, :ps_blob).call(:x=>@vs[:file])[:file].should == @vs[:file]
  end

  cspecify "should handle float type", [:swift, :sqlite] do
    @ds.filter(:f=>:$x).prepare(:first, :ps_float).call(:x=>@vs[:f])[:f].should == @vs[:f]
  end

  specify "should handle string type" do
    @ds.filter(:s=>:$x).prepare(:first, :ps_string).call(:x=>@vs[:s])[:s].should == @vs[:s]
  end

  cspecify "should handle boolean type", [:do, :sqlite], [:odbc, :mssql], [:jdbc, :sqlite], [:jdbc, :db2], :oracle do
    @ds.filter(:b=>:$x).prepare(:first, :ps_string).call(:x=>@vs[:b])[:b].should == @vs[:b]
  end
end unless INTEGRATION_DB.adapter_scheme == :swift && INTEGRATION_DB.database_type == :postgres

describe "Dataset#unbind" do
  before do
    @ds = ds = INTEGRATION_DB[:items]
    @ct = proc do |t, v|
      INTEGRATION_DB.create_table!(:items) do
        column :c, t
      end
      ds.insert(:c=>v)
    end
    @u = proc{|ds| ds, bv = ds.unbind; ds.call(:first, bv)}
  end
  after do
    INTEGRATION_DB.drop_table(:items) rescue nil
  end
  
  specify "should unbind values assigned to equality and inequality statements" do
    @ct[Integer, 10]
    @u[@ds.filter(:c=>10)].should == {:c=>10}
    @u[@ds.exclude(:c=>10)].should == nil
    @u[@ds.filter{c < 10}].should == nil
    @u[@ds.filter{c <= 10}].should == {:c=>10}
    @u[@ds.filter{c > 10}].should == nil
    @u[@ds.filter{c >= 10}].should == {:c=>10}
  end

  cspecify "should handle numerics and strings", [:odbc], [:swift, :sqlite] do
    @ct[Integer, 10]
    @u[@ds.filter(:c=>10)].should == {:c=>10}
    @ct[Float, 0.0]
    @u[@ds.filter{c < 1}].should == {:c=>0.0}
    @ct[String, 'foo']
    @u[@ds.filter(:c=>'foo')].should == {:c=>'foo'}

    INTEGRATION_DB.create_table!(:items) do
      BigDecimal :c, :size=>[15,2]
    end
    @ds.insert(:c=>BigDecimal.new('1.1'))
    @u[@ds.filter{c > 0}].should == {:c=>BigDecimal.new('1.1')}
  end

  cspecify "should handle dates and times", [:sqlite], [:do], [:jdbc, :mssql], [:tinytds], :oracle do
    @ct[Date, Date.today]
    @u[@ds.filter(:c=>Date.today)].should == {:c=>Date.today}
    t = Time.now
    @ct[Time, t]
    @u[@ds.filter{c < t + 1}][:c].to_i.should == t.to_i
  end

  specify "should handle QualifiedIdentifiers" do
    @ct[Integer, 10]
    @u[@ds.filter{items__c > 1}].should == {:c=>10}
  end

  specify "should handle deep nesting" do
    INTEGRATION_DB.create_table!(:items) do
      Integer :a
      Integer :b
      Integer :c
      Integer :d
    end
    @ds.insert(:a=>2, :b=>0, :c=>3, :d=>5)
    @u[@ds.filter{a > 1}.and{b < 2}.or(:c=>3).and({~{:d=>4}=>1}.case(0) => 1)].should == {:a=>2, :b=>0, :c=>3, :d=>5}
    @u[@ds.filter{a > 1}.and{b < 2}.or(:c=>3).and({~{:d=>5}=>1}.case(0) => 1)].should == nil
  end

  specify "should handle case where the same variable has the same value in multiple places " do
    @ct[Integer, 1]
    @u[@ds.filter{c > 1}.or{c < 1}.invert].should == {:c=>1}
    @u[@ds.filter{c > 1}.or{c < 1}].should == nil
  end
end    
