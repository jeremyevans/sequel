require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe "Prepared Statements and Bound Arguments" do
  before do
    INTEGRATION_DB.create_table!(:items) do
      primary_key :id
      integer :number
    end
    @c = Class.new(Sequel::Model(:items))
    @ds = INTEGRATION_DB[:items]
    @ds.insert(:number=>10)
    @ds.meta_def(:ba) do |sym|
      prepared_arg_placeholder == '$' ? :"#{sym}__int" : sym
    end
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items)
  end
  
  specify "should support bound variables with select, all, and first" do
    @ds.filter(:number=>@ds.ba(:$n)).call(:select, :n=>10).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).call(:all, :n=>10).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).call(:first, :n=>10).should == {:id=>1, :number=>10}
  end
    
  specify "should support blocks for select and all" do
    @ds.filter(:number=>@ds.ba(:$n)).call(:select, :n=>10){|r| r[:number] *= 2}.should == [{:id=>1, :number=>20}]
    @ds.filter(:number=>@ds.ba(:$n)).call(:all, :n=>10){|r| r[:number] *= 2}.should == [{:id=>1, :number=>20}]
  end
    
  specify "should support binding variables before the call with #bind" do
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>10).call(:select).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>10).call(:all).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>10).call(:first).should == {:id=>1, :number=>10}
    
    @ds.bind(:n=>10).filter(:number=>@ds.ba(:$n)).call(:select).should == [{:id=>1, :number=>10}]
    @ds.bind(:n=>10).filter(:number=>@ds.ba(:$n)).call(:all).should == [{:id=>1, :number=>10}]
    @ds.bind(:n=>10).filter(:number=>@ds.ba(:$n)).call(:first).should == {:id=>1, :number=>10}
  end
  
  specify "should allow overriding variables specified with #bind" do
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>1).call(:select, :n=>10).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>1).call(:all, :n=>10).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>1).call(:first, :n=>10).should == {:id=>1, :number=>10}
    
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>1).bind(:n=>10).call(:select).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>1).bind(:n=>10).call(:all).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).bind(:n=>1).bind(:n=>10).call(:first).should == {:id=>1, :number=>10}
  end

  specify "should support placeholder literal strings with call" do
    @ds.filter("number = ?", @ds.ba(:$n)).call(:select, :n=>10).should == [{:id=>1, :number=>10}]
  end

  specify "should support named placeholder literal strings and handle multiple named placeholders correctly with call" do
    @ds.filter("number = :n", :n=>@ds.ba(:$n)).call(:select, :n=>10).should == [{:id=>1, :number=>10}]
    @ds.insert(:number=>20)
    @ds.insert(:number=>30)
    @ds.filter("number > :n1 AND number < :n2 AND number = :n3", :n3=>@ds.ba(:$n3), :n2=>@ds.ba(:$n2), :n1=>@ds.ba(:$n1)).call(:select, :n3=>20, :n2=>30, :n1=>10).should == [{:id=>2, :number=>20}]
  end

  specify "should support datasets with static sql and placeholders with call" do
    INTEGRATION_DB["SELECT * FROM items WHERE number = ?", @ds.ba(:$n)].call(:select, :n=>10).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects with call" do
    @ds.filter(:id=>:$i).filter(:number=>@ds.select(:number).filter(:number=>@ds.ba(:$n))).filter(:id=>:$j).call(:select, :n=>10, :i=>1, :j=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects with literal strings with call" do
    @ds.filter(:id=>:$i, :number=>@ds.select(:number).filter("number = ?", @ds.ba(:$n))).call(:select, :n=>10, :i=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects with static sql and placeholders with call" do
    @ds.filter(:id=>:$i, :number=>INTEGRATION_DB["SELECT number FROM items WHERE number = ?", @ds.ba(:$n)]).call(:select, :n=>10, :i=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects of subselects with call" do
    @ds.filter(:id=>:$i).filter(:number=>@ds.select(:number).filter(:number=>@ds.select(:number).filter(:number=>@ds.ba(:$n)))).filter(:id=>:$j).call(:select, :n=>10, :i=>1, :j=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support bound variables with insert" do
    @ds.call(:insert, {:n=>20}, :number=>@ds.ba(:$n))
    @ds.count.should == 2
    @ds.order(:id).map(:number).should == [10, 20]
  end

  specify "should have insert return primary key value when using bound arguments" do
    @ds.call(:insert, {:n=>20}, :number=>@ds.ba(:$n)).should == 2
    @ds.filter(:id=>2).first[:number].should == 20
  end

  specify "should support bound variables with delete" do
    @ds.filter(:number=>@ds.ba(:$n)).call(:delete, :n=>10).should == 1
    @ds.count.should == 0
  end

  specify "should support bound variables with update" do
    @ds.filter(:number=>@ds.ba(:$n)).call(:update, {:n=>10, :nn=>20}, :number=>:number+@ds.ba(:$nn)).should == 1
    @ds.all.should == [{:id=>1, :number=>30}]
  end
  
  specify "should support prepared statements with select, first, and all" do
    @ds.filter(:number=>@ds.ba(:$n)).prepare(:select, :select_n)
    INTEGRATION_DB.call(:select_n, :n=>10).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).prepare(:all, :select_n)
    INTEGRATION_DB.call(:select_n, :n=>10).should == [{:id=>1, :number=>10}]
    @ds.filter(:number=>@ds.ba(:$n)).prepare(:first, :select_n)
    INTEGRATION_DB.call(:select_n, :n=>10).should == {:id=>1, :number=>10}
    if INTEGRATION_DB.class.adapter_scheme == :jdbc and INTEGRATION_DB.database_type == :sqlite
      # Work around for open prepared statements on a table not allowing the
      # dropping of a table when using SQLite over JDBC
      INTEGRATION_DB.synchronize{|c| c.prepared_statements[:select_n][1].close}
    end
  end

  specify "should support placeholder literal strings with prepare" do
    @ds.filter("number = ?", @ds.ba(:$n)).prepare(:select, :seq_select).call(:n=>10).should == [{:id=>1, :number=>10}]
  end

  specify "should support named placeholder literal strings and handle multiple named placeholders correctly with prepare" do
    @ds.filter("number = :n", :n=>@ds.ba(:$n)).prepare(:select, :seq_select).call(:n=>10).should == [{:id=>1, :number=>10}]
    @ds.insert(:number=>20)
    @ds.insert(:number=>30)
    @ds.filter("number > :n1 AND number < :n2 AND number = :n3", :n3=>@ds.ba(:$n3), :n2=>@ds.ba(:$n2), :n1=>@ds.ba(:$n1)).call(:select, :n3=>20, :n2=>30, :n1=>10).should == [{:id=>2, :number=>20}]
  end

  specify "should support datasets with static sql and placeholders with prepare" do
    INTEGRATION_DB["SELECT * FROM items WHERE number = ?", @ds.ba(:$n)].prepare(:select, :seq_select).call(:n=>10).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects with prepare" do
    @ds.filter(:id=>:$i).filter(:number=>@ds.select(:number).filter(:number=>@ds.ba(:$n))).filter(:id=>:$j).prepare(:select, :seq_select).call(:n=>10, :i=>1, :j=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects with literal strings with prepare" do
    @ds.filter(:id=>:$i, :number=>@ds.select(:number).filter("number = ?", @ds.ba(:$n))).prepare(:select, :seq_select).call(:n=>10, :i=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects with static sql and placeholders with prepare" do
    @ds.filter(:id=>:$i, :number=>INTEGRATION_DB["SELECT number FROM items WHERE number = ?", @ds.ba(:$n)]).prepare(:select, :seq_select).call(:n=>10, :i=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support subselects of subselects with prepare" do
    @ds.filter(:id=>:$i).filter(:number=>@ds.select(:number).filter(:number=>@ds.select(:number).filter(:number=>@ds.ba(:$n)))).filter(:id=>:$j).prepare(:select, :seq_select).call(:n=>10, :i=>1, :j=>1).should == [{:id=>1, :number=>10}]
  end

  specify "should support prepared statements with insert" do
    @ds.prepare(:insert, :insert_n, :number=>@ds.ba(:$n))
    INTEGRATION_DB.call(:insert_n, :n=>20)
    @ds.count.should == 2
    @ds.order(:id).map(:number).should == [10, 20]
  end

  specify "should have insert return primary key value when using prepared statements" do
    @ds.prepare(:insert, :insert_n, :number=>@ds.ba(:$n))
    INTEGRATION_DB.call(:insert_n, :n=>20).should == 2
    @ds.filter(:id=>2).first[:number].should == 20
  end

  specify "should support prepared statements with delete" do
    @ds.filter(:number=>@ds.ba(:$n)).prepare(:delete, :delete_n)
    INTEGRATION_DB.call(:delete_n, :n=>10).should == 1
    @ds.count.should == 0
  end

  specify "should support prepared statements with update" do
    @ds.filter(:number=>@ds.ba(:$n)).prepare(:update, :update_n, :number=>:number+@ds.ba(:$nn))
    INTEGRATION_DB.call(:update_n, :n=>10, :nn=>20).should == 1
    @ds.all.should == [{:id=>1, :number=>30}]
  end
  
  specify "model datasets should return model instances when using select, all, and first with bound variables" do
    @c.filter(:number=>@ds.ba(:$n)).call(:select, :n=>10).should == [@c.load(:id=>1, :number=>10)]
    @c.filter(:number=>@ds.ba(:$n)).call(:all, :n=>10).should == [@c.load(:id=>1, :number=>10)]
    @c.filter(:number=>@ds.ba(:$n)).call(:first, :n=>10).should == @c.load(:id=>1, :number=>10)
  end
  
  specify "model datasets should return model instances when using select, all, and first with prepared statements" do
    @c.filter(:number=>@ds.ba(:$n)).prepare(:select, :select_n)
    INTEGRATION_DB.call(:select_n, :n=>10).should == [@c.load(:id=>1, :number=>10)]
    @c.filter(:number=>@ds.ba(:$n)).prepare(:all, :select_n)
    INTEGRATION_DB.call(:select_n, :n=>10).should == [@c.load(:id=>1, :number=>10)]
    @c.filter(:number=>@ds.ba(:$n)).prepare(:first, :select_n)
    INTEGRATION_DB.call(:select_n, :n=>10).should == @c.load(:id=>1, :number=>10)
    if INTEGRATION_DB.class.adapter_scheme == :jdbc and INTEGRATION_DB.database_type == :sqlite
      # Work around for open prepared statements on a table not allowing the
      # dropping of a table when using SQLite over JDBC
      INTEGRATION_DB.synchronize{|c| c.prepared_statements[:select_n][1].close}
    end
  end
end
