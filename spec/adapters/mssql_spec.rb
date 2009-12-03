require File.join(File.dirname(__FILE__), 'spec_helper.rb')

require ENV['SEQUEL_MSSQL_SPEC_REQUIRE'] if ENV['SEQUEL_MSSQL_SPEC_REQUIRE']

unless defined?(MSSQL_DB)
  MSSQL_URL = 'jdbc:sqlserver://localhost;integratedSecurity=true;database=sandbox' unless defined? MSSQL_URL
  MSSQL_DB = Sequel.connect(ENV['SEQUEL_MSSQL_SPEC_DB']||MSSQL_URL)
end
INTEGRATION_DB = MSSQL_DB unless defined?(INTEGRATION_DB)

def MSSQL_DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  MSSQL_DB.sqls << msg
end
MSSQL_DB.logger = logger

MSSQL_DB.create_table! :test do
  text :name
  integer :value, :index => true
end
MSSQL_DB.create_table! :test2 do
  text :name
  integer :value
end
MSSQL_DB.create_table! :test3 do
  integer :value
  timestamp :time
end
MSSQL_DB.create_table! :test4 do
  varchar :name, :size => 20
  varbinary :value
end

context "A MSSQL database" do
  before do
    @db = MSSQL_DB
  end

  cspecify "should be able to read fractional part of timestamp", :odbc do
    rs = @db["select getutcdate() as full_date, cast(datepart(millisecond, getutcdate()) as int) as milliseconds"].first
    rs[:milliseconds].should == rs[:full_date].usec/1000
  end

  cspecify "should be able to write fractional part of timestamp", :odbc do
    t = Time.utc(2001, 12, 31, 23, 59, 59, 997000)
    (t.usec/1000).should == @db["select cast(datepart(millisecond, ?) as int) as milliseconds", t].get
  end
  
  specify "should not raise an error when getting the server version" do
    proc{@db.server_version}.should_not raise_error
    proc{@db.dataset.server_version}.should_not raise_error
  end
end

context "MSSQL Dataset#join_table" do
  specify "should emulate the USING clause with ON" do
    MSSQL_DB[:items].join(:categories, [:id]).sql.should ==
      'SELECT * FROM ITEMS INNER JOIN CATEGORIES ON (CATEGORIES.ID = ITEMS.ID)'
    MSSQL_DB[:items].join(:categories, [:id1, :id2]).sql.should ==
      'SELECT * FROM ITEMS INNER JOIN CATEGORIES ON ((CATEGORIES.ID1 = ITEMS.ID1) AND (CATEGORIES.ID2 = ITEMS.ID2))'
    MSSQL_DB[:items___i].join(:categories___c, [:id]).sql.should ==
      'SELECT * FROM ITEMS AS I INNER JOIN CATEGORIES AS C ON (C.ID = I.ID)'
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

  specify "should format OUTPUT clauses without INTO for DELETE statements" do
    @ds.output(nil, [:deleted__name, :deleted__value]).delete_sql.should =~
      /DELETE FROM ITEMS OUTPUT DELETED.(NAME|VALUE), DELETED.(NAME|VALUE)/
    @ds.output(nil, [:deleted.*]).delete_sql.should =~
      /DELETE FROM ITEMS OUTPUT DELETED.*/
  end
  
  specify "should format OUTPUT clauses with INTO for DELETE statements" do
    @ds.output(:out, [:deleted__name, :deleted__value]).delete_sql.should =~
      /DELETE FROM ITEMS OUTPUT DELETED.(NAME|VALUE), DELETED.(NAME|VALUE) INTO OUT/
    @ds.output(:out, {:name => :deleted__name, :value => :deleted__value}).delete_sql.should =~
      /DELETE FROM ITEMS OUTPUT DELETED.(NAME|VALUE), DELETED.(NAME|VALUE) INTO OUT \((NAME|VALUE), (NAME|VALUE)\)/
  end

  specify "should format OUTPUT clauses without INTO for INSERT statements" do
    @ds.output(nil, [:inserted__name, :inserted__value]).insert_sql(:name => "name", :value => 1).should =~
      /INSERT INTO ITEMS \((NAME|VALUE), (NAME|VALUE)\) OUTPUT INSERTED.(NAME|VALUE), INSERTED.(NAME|VALUE) VALUES \((N'name'|1), (N'name'|1)\)/
    @ds.output(nil, [:inserted.*]).insert_sql(:name => "name", :value => 1).should =~
      /INSERT INTO ITEMS \((NAME|VALUE), (NAME|VALUE)\) OUTPUT INSERTED.* VALUES \((N'name'|1), (N'name'|1)\)/
  end

  specify "should format OUTPUT clauses with INTO for INSERT statements" do
    @ds.output(:out, [:inserted__name, :inserted__value]).insert_sql(:name => "name", :value => 1).should =~
      /INSERT INTO ITEMS \((NAME|VALUE), (NAME|VALUE)\) OUTPUT INSERTED.(NAME|VALUE), INSERTED.(NAME|VALUE) INTO OUT VALUES \((N'name'|1), (N'name'|1)\)/
    @ds.output(:out, {:name => :inserted__name, :value => :inserted__value}).insert_sql(:name => "name", :value => 1).should =~
      /INSERT INTO ITEMS \((NAME|VALUE), (NAME|VALUE)\) OUTPUT INSERTED.(NAME|VALUE), INSERTED.(NAME|VALUE) INTO OUT \((NAME|VALUE), (NAME|VALUE)\) VALUES \((N'name'|1), (N'name'|1)\)/
  end

  specify "should format OUTPUT clauses without INTO for UPDATE statements" do
    @ds.output(nil, [:inserted__name, :deleted__value]).update_sql(:value => 2).should =~
      /UPDATE ITEMS SET VALUE = 2 OUTPUT (INSERTED.NAME|DELETED.VALUE), (INSERTED.NAME|DELETED.VALUE)/
    @ds.output(nil, [:inserted.*]).update_sql(:value => 2).should =~
      /UPDATE ITEMS SET VALUE = 2 OUTPUT INSERTED.*/
  end

  specify "should format OUTPUT clauses with INTO for UPDATE statements" do
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

context "MSSQL dataset" do
  before do
    @db = MSSQL_DB
    @ds = MSSQL_DB[:t]
  end

  context "using #with and #with_recursive" do
    before do
      @ds1 = @ds.with(:t, @db[:x])
      @ds2 = @ds.with_recursive(:t, @db[:x], @db[:t])
    end

    specify "should prepend UPDATE statements with WITH clause" do
      @ds1.update_sql(:x => :y).should == 'WITH T AS (SELECT * FROM X) UPDATE T SET X = Y'
      @ds2.update_sql(:x => :y).should == 'WITH T AS (SELECT * FROM X UNION ALL SELECT * FROM T) UPDATE T SET X = Y'
    end

    specify "should prepend DELETE statements with WITH clause" do
      @ds1.filter(:y => 1).delete_sql.should == 'WITH T AS (SELECT * FROM X) DELETE FROM T WHERE (Y = 1)'
      @ds2.filter(:y => 1).delete_sql.should == 'WITH T AS (SELECT * FROM X UNION ALL SELECT * FROM T) DELETE FROM T WHERE (Y = 1)'
    end

    specify "should prepend INSERT statements with WITH clause" do
      @ds1.insert_sql(@db[:t]).should == 'WITH T AS (SELECT * FROM X) INSERT INTO T SELECT * FROM T'
      @ds2.insert_sql(@db[:t]).should == 'WITH T AS (SELECT * FROM X UNION ALL SELECT * FROM T) INSERT INTO T SELECT * FROM T'
    end

    context "on #import" do
      before do
        @db = @db.clone
        class << @db
          attr_reader :import_sqls

          def execute(sql, opts={})
            @import_sqls ||= []
            @import_sqls << sql
          end
          alias execute_dui execute

          def transaction(opts={})
            @import_sqls ||= []
            @import_sqls << 'BEGIN'
            yield
            @import_sqls << 'COMMIT'
          end
        end
      end

      specify "should prepend INSERT statements with WITH clause" do
        @db[:items].with(:items, @db[:inventory].group(:type)).import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice => 2)
        @db.import_sqls.should == [
          'BEGIN',
          "WITH ITEMS AS (SELECT * FROM INVENTORY GROUP BY TYPE) INSERT INTO ITEMS (X, Y) SELECT 1, 2 UNION ALL SELECT 3, 4",
          'COMMIT',
          'BEGIN',
          "WITH ITEMS AS (SELECT * FROM INVENTORY GROUP BY TYPE) INSERT INTO ITEMS (X, Y) SELECT 5, 6",
          'COMMIT'
        ]
      end
    end
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

describe "Offset support" do
  before do
    @db = MSSQL_DB
    @db.create_table!(:i){Integer :id; Integer :parent_id}
    @ds = @db[:i].order(:id)
    @hs = []
    @ds.row_proc = proc{|r| @hs << r.dup; r[:id] *= 2; r[:parent_id] *= 3; r}
    @ds.import [:id, :parent_id], [[1,nil],[2,nil],[3,1],[4,1],[5,3],[6,5]]
  end
  after do
    @db.drop_table(:i)
  end
  
  specify "should return correct rows" do
    @ds.limit(2, 2).all.should == [{:id=>6, :parent_id=>3}, {:id=>8, :parent_id=>3}]
  end
  
  specify "should not include offset column in hashes passed to row_proc" do
    @ds.limit(2, 2).all
    @hs.should == [{:id=>3, :parent_id=>1}, {:id=>4, :parent_id=>1}]
  end
end

describe "Common Table Expressions" do
  before do
    @db = MSSQL_DB
    @db.create_table!(:i1){Integer :id; Integer :parent_id}
    @db.create_table!(:i2){Integer :id; Integer :parent_id}
    @ds = @db[:i1]
    @ds2 = @db[:i2]
    @ds.import [:id, :parent_id], [[1,nil],[2,nil],[3,1],[4,1],[5,3],[6,5]]
  end
  after do
    @db.drop_table(:i1)
    @db.drop_table(:i2)
  end

  specify "using #with should be able to update" do
    @ds.insert(:id=>1)
    @ds2.insert(:id=>2, :parent_id=>1)
    @ds2.insert(:id=>3, :parent_id=>2)
    @ds.with(:t, @ds2).filter(:id => @db[:t].select(:id)).update(:parent_id => @db[:t].filter(:id => :i1__id).select(:parent_id).limit(1))
    @ds[:id => 1].should == {:id => 1, :parent_id => nil}
    @ds[:id => 2].should == {:id => 2, :parent_id => 1}
    @ds[:id => 3].should == {:id => 3, :parent_id => 2}
    @ds[:id => 4].should == {:id => 4, :parent_id => 1}
  end

  specify "using #with_recursive should be able to update" do
    ds = @ds.with_recursive(:t, @ds.filter(:parent_id=>1).or(:id => 1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.filter(~{:id => @db[:t].select(:i)}).update(:parent_id => 1)
    @ds[:id => 1].should == {:id => 1, :parent_id => nil}
    @ds[:id => 2].should == {:id => 2, :parent_id => 1}
    @ds[:id => 5].should == {:id => 5, :parent_id => 3}
  end

  specify "using #with should be able to insert" do
    @ds2.insert(:id=>7)
    @ds.with(:t, @ds2).insert(@db[:t])
    @ds[:id => 7].should == {:id => 7, :parent_id => nil}
  end

  specify "using #with_recursive should be able to insert" do
    ds = @ds2.with_recursive(:t, @ds.filter(:parent_id=>1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.insert @db[:t]
    @ds2.all.should == [{:id => 3, :parent_id => 1}, {:id => 4, :parent_id => 1}, {:id => 5, :parent_id => 3}, {:id => 6, :parent_id => 5}]
  end

  specify "using #with should be able to delete" do
    @ds2.insert(:id=>6)
    @ds2.insert(:id=>5)
    @ds2.insert(:id=>4)
    @ds.with(:t, @ds2).filter(:id => @db[:t].select(:id)).delete
    @ds.all.should == [{:id => 1, :parent_id => nil}, {:id => 2, :parent_id => nil}, {:id => 3, :parent_id => 1}]
  end

  specify "using #with_recursive should be able to delete" do
    @ds.insert(:id=>7, :parent_id=>2)
    ds = @ds.with_recursive(:t, @ds.filter(:parent_id=>1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.filter(:i1__id => @db[:t].select(:i)).delete
    @ds.all.should == [{:id => 1, :parent_id => nil}, {:id => 2, :parent_id => nil}, {:id => 7, :parent_id => 2}]
  end

  specify "using #with should be able to import" do
    @ds2.insert(:id=>7)
    @ds.with(:t, @ds2).import [:id, :parent_id], @db[:t].select(:id, :parent_id)
    @ds[:id => 7].should == {:id => 7, :parent_id => nil}
  end

  specify "using #with_recursive should be able to import" do
    ds = @ds2.with_recursive(:t, @ds.filter(:parent_id=>1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.import [:id, :parent_id], @db[:t].select(:i, :pi)
    @ds2.all.should == [{:id => 3, :parent_id => 1}, {:id => 4, :parent_id => 1}, {:id => 5, :parent_id => 3}, {:id => 6, :parent_id => 5}]
  end
end

context "MSSSQL::Dataset#insert" do
  before do
    @db = MSSQL_DB
    @db.create_table!(:test5){primary_key :xid; Integer :value}
    @db.sqls.clear
    @ds = @db[:test5]
  end
  after do
    @db.drop_table(:test5) rescue nil
  end

  specify "should have insert_select return nil if disable_insert_output is used" do
    @ds.disable_insert_output.insert_select(:value=>10).should == nil
  end

  specify "should have insert_select insert the record and return the inserted record" do
    @ds.meta_def(:server_version){80201}
    h = @ds.insert_select(:value=>10)
    h[:value].should == 10
    @ds.first(:xid=>h[:xid])[:value].should == 10
  end
end

context "MSSSQL::Dataset#disable_insert_output" do
  specify "should play nicely with simple_select_all?" do
    MSSQL_DB[:test].disable_insert_output.send(:simple_select_all?).should == true
  end
end
