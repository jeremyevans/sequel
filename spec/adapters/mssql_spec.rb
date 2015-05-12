SEQUEL_ADAPTER_TEST = :mssql

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

def DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  DB.sqls << msg
end
DB.loggers = [logger]

describe "A MSSQL database" do
  before do
    @db = DB
  end

  it "should be able to read fractional part of timestamp" do
    rs = @db["select getutcdate() as full_date, cast(datepart(millisecond, getutcdate()) as int) as milliseconds"].first
    rs[:milliseconds].must_equal rs[:full_date].usec/1000
  end

  it "should be able to write fractional part of timestamp" do
    t = Time.utc(2001, 12, 31, 23, 59, 59, 997000)
    (t.usec/1000).must_equal @db["select cast(datepart(millisecond, ?) as int) as milliseconds", t].get
  end
  
  it "should not raise an error when getting the server version" do
    @db.server_version
    @db.dataset.server_version
  end
end
  
describe "A MSSQL database" do
  before do
    @db = DB
    @db.create_table! :test3 do
      Integer :value
      Time :time
    end
  end
  after do
    @db.drop_table?(:test3)
  end

  it "should work with NOLOCK" do
    @db.transaction{@db[:test3].nolock.all.must_equal []}
  end
end

describe "MSSQL" do
  before(:all) do
    @db = DB
    @db.create_table!(:test3){Integer :v3}
    @db.create_table!(:test4){Integer :v4}
    @db[:test3].import([:v3], [[1], [2]])
    @db[:test4].import([:v4], [[1], [3]])
  end
  after(:all) do
    @db.drop_table?(:test3, :test4)
  end

  it "should should support CROSS APPLY" do
    @db[:test3].cross_apply(@db[:test4].where(:test3__v3=>:test4__v4)).select_order_map([:v3, :v4]).must_equal [[1,1]]
  end

  it "should should support OUTER APPLY" do
    @db[:test3].outer_apply(@db[:test4].where(:test3__v3=>:test4__v4)).select_order_map([:v3, :v4]).must_equal [[1,1], [2, nil]]
  end
end

# This spec is currently disabled as the SQL Server 2008 R2 Express doesn't support
# full text searching.  Even if full text searching is supported,
# you may need to create a full text catalog on the database first via:
#   CREATE FULLTEXT CATALOG ftscd AS DEFAULT
describe "MSSQL full_text_search" do
  before do
    @db = DB
    @db.drop_table?(:posts)
  end
  after do
    @db.drop_table?(:posts)
  end
  
  it "should support fulltext indexes and full_text_search" do
    log do
      @db.create_table(:posts){Integer :id, :null=>false; String :title; String :body; index :id, :name=>:fts_id_idx, :unique=>true; full_text_index :title, :key_index=>:fts_id_idx; full_text_index [:title, :body], :key_index=>:fts_id_idx}
      @db[:posts].insert(:title=>'ruby rails', :body=>'y')
      @db[:posts].insert(:title=>'sequel', :body=>'ruby')
      @db[:posts].insert(:title=>'ruby scooby', :body=>'x')

      @db[:posts].full_text_search(:title, 'rails').all.must_equal [{:title=>'ruby rails', :body=>'y'}]
      @db[:posts].full_text_search([:title, :body], ['sequel', 'ruby']).all.must_equal [{:title=>'sequel', :body=>'ruby'}]

      @db[:posts].full_text_search(:title, :$n).call(:select, :n=>'rails').must_equal [{:title=>'ruby rails', :body=>'y'}]
      @db[:posts].full_text_search(:title, :$n).prepare(:select, :fts_select).call(:n=>'rails').must_equal [{:title=>'ruby rails', :body=>'y'}]
    end
  end
end if false

describe "MSSQL Dataset#join_table" do
  it "should emulate the USING clause with ON" do
    DB[:items].join(:categories, [:id]).sql.must_equal 'SELECT * FROM [ITEMS] INNER JOIN [CATEGORIES] ON ([CATEGORIES].[ID] = [ITEMS].[ID])'
    ['SELECT * FROM [ITEMS] INNER JOIN [CATEGORIES] ON (([CATEGORIES].[ID1] = [ITEMS].[ID1]) AND ([CATEGORIES].[ID2] = [ITEMS].[ID2]))',
      'SELECT * FROM [ITEMS] INNER JOIN [CATEGORIES] ON (([CATEGORIES].[ID2] = [ITEMS].[ID2]) AND ([CATEGORIES].[ID1] = [ITEMS].[ID1]))'].
      must_include(DB[:items].join(:categories, [:id1, :id2]).sql)
    DB[:items___i].join(:categories___c, [:id]).sql.must_equal 'SELECT * FROM [ITEMS] AS [I] INNER JOIN [CATEGORIES] AS [C] ON ([C].[ID] = [I].[ID])'
  end
end

describe "MSSQL Dataset#output" do
  before do
    @db = DB
    @db.create_table!(:items){String :name; Integer :value}
    @db.create_table!(:out){String :name; Integer :value}
    @ds = @db[:items]
  end
  after do
    @db.drop_table?(:items, :out)
  end

  it "should format OUTPUT clauses without INTO for DELETE statements" do
    @ds.output(nil, [:deleted__name, :deleted__value]).delete_sql.must_match(/DELETE FROM \[ITEMS\] OUTPUT \[DELETED\].\[(NAME|VALUE)\], \[DELETED\].\[(NAME|VALUE)\]/)
    @ds.output(nil, [Sequel::SQL::ColumnAll.new(:deleted)]).delete_sql.must_match(/DELETE FROM \[ITEMS\] OUTPUT \[DELETED\].*/)
  end
  
  it "should format OUTPUT clauses with INTO for DELETE statements" do
    @ds.output(:out, [:deleted__name, :deleted__value]).delete_sql.must_match(/DELETE FROM \[ITEMS\] OUTPUT \[DELETED\].\[(NAME|VALUE)\], \[DELETED\].\[(NAME|VALUE)\] INTO \[OUT\]/)
    @ds.output(:out, {:name => :deleted__name, :value => :deleted__value}).delete_sql.must_match(/DELETE FROM \[ITEMS\] OUTPUT \[DELETED\].\[(NAME|VALUE)\], \[DELETED\].\[(NAME|VALUE)\] INTO \[OUT\] \(\[(NAME|VALUE)\], \[(NAME|VALUE)\]\)/)
  end

  it "should format OUTPUT clauses without INTO for INSERT statements" do
    @ds.output(nil, [:inserted__name, :inserted__value]).insert_sql(:name => "name", :value => 1).must_match(/INSERT INTO \[ITEMS\] \(\[(NAME|VALUE)\], \[(NAME|VALUE)\]\) OUTPUT \[INSERTED\].\[(NAME|VALUE)\], \[INSERTED\].\[(NAME|VALUE)\] VALUES \((N'name'|1), (N'name'|1)\)/)
    @ds.output(nil, [Sequel::SQL::ColumnAll.new(:inserted)]).insert_sql(:name => "name", :value => 1).must_match(/INSERT INTO \[ITEMS\] \(\[(NAME|VALUE)\], \[(NAME|VALUE)\]\) OUTPUT \[INSERTED\].* VALUES \((N'name'|1), (N'name'|1)\)/)
  end

  it "should format OUTPUT clauses with INTO for INSERT statements" do
    @ds.output(:out, [:inserted__name, :inserted__value]).insert_sql(:name => "name", :value => 1).must_match(/INSERT INTO \[ITEMS\] \((\[NAME\]|\[VALUE\]), (\[NAME\]|\[VALUE\])\) OUTPUT \[INSERTED\].\[(NAME|VALUE)\], \[INSERTED\].\[(NAME|VALUE)\] INTO \[OUT\] VALUES \((N'name'|1), (N'name'|1)\)/)
    @ds.output(:out, {:name => :inserted__name, :value => :inserted__value}).insert_sql(:name => "name", :value => 1).must_match(/INSERT INTO \[ITEMS\] \(\[(NAME|VALUE)\], \[(NAME|VALUE)\]\) OUTPUT \[INSERTED\].\[(NAME|VALUE)\], \[INSERTED\].\[(NAME|VALUE)\] INTO \[OUT\] \(\[(NAME|VALUE)\], \[(NAME|VALUE)\]\) VALUES \((N'name'|1), (N'name'|1)\)/)
  end

  it "should format OUTPUT clauses without INTO for UPDATE statements" do
    @ds.output(nil, [:inserted__name, :deleted__value]).update_sql(:value => 2).must_match(/UPDATE \[ITEMS\] SET \[VALUE\] = 2 OUTPUT \[(INSERTED\].\[NAME|DELETED\].\[VALUE)\], \[(INSERTED\].\[NAME|DELETED\].\[VALUE)\]/)
    @ds.output(nil, [Sequel::SQL::ColumnAll.new(:inserted)]).update_sql(:value => 2).must_match(/UPDATE \[ITEMS\] SET \[VALUE\] = 2 OUTPUT \[INSERTED\].*/)
  end

  it "should format OUTPUT clauses with INTO for UPDATE statements" do
    @ds.output(:out, [:inserted__name, :deleted__value]).update_sql(:value => 2).must_match(/UPDATE \[ITEMS\] SET \[VALUE\] = 2 OUTPUT \[(INSERTED\].\[NAME|DELETED\].\[VALUE)\], \[(INSERTED\].\[NAME|DELETED\].\[VALUE)\] INTO \[OUT\]/)
    @ds.output(:out, {:name => :inserted__name, :value => :deleted__value}).update_sql(:value => 2).must_match(/UPDATE \[ITEMS\] SET \[VALUE\] = 2 OUTPUT \[(INSERTED\].\[NAME|DELETED\].\[VALUE)\], \[(INSERTED\].\[NAME|DELETED\].\[VALUE)\] INTO \[OUT\] \(\[(NAME|VALUE)\], \[(NAME|VALUE)\]\)/)
  end

  it "should execute OUTPUT clauses in DELETE statements" do
    @ds.insert(:name => "name", :value => 1)
    @ds.output(:out, [:deleted__name, :deleted__value]).delete
    @db[:out].all.must_equal [{:name => "name", :value => 1}]
    @ds.insert(:name => "name", :value => 2)
    @ds.output(:out, {:name => :deleted__name, :value => :deleted__value}).delete
    @db[:out].all.must_equal [{:name => "name", :value => 1}, {:name => "name", :value => 2}]
  end

  it "should execute OUTPUT clauses in INSERT statements" do
    @ds.output(:out, [:inserted__name, :inserted__value]).insert(:name => "name", :value => 1)
    @db[:out].all.must_equal [{:name => "name", :value => 1}]
    @ds.output(:out, {:name => :inserted__name, :value => :inserted__value}).insert(:name => "name", :value => 2)
    @db[:out].all.must_equal [{:name => "name", :value => 1}, {:name => "name", :value => 2}]
  end

  it "should execute OUTPUT clauses in UPDATE statements" do
    @ds.insert(:name => "name", :value => 1)
    @ds.output(:out, [:inserted__name, :deleted__value]).update(:value => 2)
    @db[:out].all.must_equal [{:name => "name", :value => 1}]
    @ds.output(:out, {:name => :inserted__name, :value => :deleted__value}).update(:value => 3)
    @db[:out].all.must_equal [{:name => "name", :value => 1}, {:name => "name", :value => 2}]
  end
end

describe "MSSQL dataset using #with and #with_recursive" do
  before do
    @db = DB
    @ds = DB[:t]
      @ds1 = @ds.with(:t, @db[:x])
      @ds2 = @ds.with_recursive(:t, @db[:x], @db[:t])
  end

  it "should prepend UPDATE statements with WITH clause" do
    @ds1.update_sql(:x => :y).must_equal 'WITH [T] AS (SELECT * FROM [X]) UPDATE [T] SET [X] = [Y]'
    @ds2.update_sql(:x => :y).must_equal 'WITH [T] AS (SELECT * FROM [X] UNION ALL SELECT * FROM [T]) UPDATE [T] SET [X] = [Y]'
  end

  it "should prepend DELETE statements with WITH clause" do
    @ds1.filter(:y => 1).delete_sql.must_equal 'WITH [T] AS (SELECT * FROM [X]) DELETE FROM [T] WHERE ([Y] = 1)'
    @ds2.filter(:y => 1).delete_sql.must_equal 'WITH [T] AS (SELECT * FROM [X] UNION ALL SELECT * FROM [T]) DELETE FROM [T] WHERE ([Y] = 1)'
  end

  it "should prepend INSERT statements with WITH clause" do
    @ds1.insert_sql(@db[:t]).must_equal 'WITH [T] AS (SELECT * FROM [X]) INSERT INTO [T] SELECT * FROM [T]'
    @ds2.insert_sql(@db[:t]).must_equal 'WITH [T] AS (SELECT * FROM [X] UNION ALL SELECT * FROM [T]) INSERT INTO [T] SELECT * FROM [T]'
  end

  it "should move WITH clause on joined dataset to top level" do
    @db[:s].inner_join(@ds1).sql.must_equal "WITH [T] AS (SELECT * FROM [X]) SELECT * FROM [S] INNER JOIN (SELECT * FROM [T]) AS [T1]"
    @ds1.inner_join(@db[:s].with(:s, @db[:y])).sql.must_equal "WITH [T] AS (SELECT * FROM [X]), [S] AS (SELECT * FROM [Y]) SELECT * FROM [T] INNER JOIN (SELECT * FROM [S]) AS [T1]"
  end
end

describe "MSSQL::Dataset#import" do
  before do
    @db = DB
    @db.sqls.clear
    @ds = @db[:test]
  end
  after do
    @db.drop_table?(:test)
  end
  
  it "#import should work correctly with an arbitrary output value" do
    @db.create_table!(:test){primary_key :x; Integer :y}
    @ds.output(nil, [:inserted__y, :inserted__x]).import([:y], [[3], [4]]).must_equal [{:y=>3, :x=>1}, {:y=>4, :x=>2}]
    @ds.all.must_equal [{:x=>1, :y=>3}, {:x=>2, :y=>4}]
  end

  it "should handle WITH statements" do
    @db.create_table!(:test){Integer :x; Integer :y}
    @db[:testx].with(:testx, @db[:test]).import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice => 2)
    @ds.select_order_map([:x, :y]).must_equal [[1, 2], [3, 4], [5, 6]]
  end
end

describe "MSSQL joined datasets" do
  before do
    @db = DB
  end

  it "should format DELETE statements" do
    @db[:t1].inner_join(:t2, :t1__pk => :t2__pk).delete_sql.must_equal "DELETE FROM [T1] FROM [T1] INNER JOIN [T2] ON ([T1].[PK] = [T2].[PK])"
  end

  it "should format UPDATE statements" do
    @db[:t1].inner_join(:t2, :t1__pk => :t2__pk).update_sql(:pk => :t2__pk).must_equal "UPDATE [T1] SET [PK] = [T2].[PK] FROM [T1] INNER JOIN [T2] ON ([T1].[PK] = [T2].[PK])"
  end
end

describe "Offset support" do
  before do
    @db = DB
    @db.create_table!(:i){Integer :id; Integer :parent_id}
    @ds = @db[:i].order(:id)
    @hs = []
    @ds.row_proc = proc{|r| @hs << r.dup; r[:id] *= 2; r[:parent_id] *= 3; r}
    @ds.import [:id, :parent_id], [[1,nil],[2,nil],[3,1],[4,1],[5,3],[6,5]]
  end
  after do
    @db.drop_table?(:i)
  end
  
  it "should return correct rows" do
    @ds.limit(2, 2).all.must_equal [{:id=>6, :parent_id=>3}, {:id=>8, :parent_id=>3}]
  end
  
  it "should not include offset column in hashes passed to row_proc" do
    @ds.limit(2, 2).all
    @hs.must_equal [{:id=>3, :parent_id=>1}, {:id=>4, :parent_id=>1}]
  end
end

describe "Common Table Expressions" do
  before do
    @db = DB
    @db.create_table!(:i1){Integer :id; Integer :parent_id}
    @db.create_table!(:i2){Integer :id; Integer :parent_id}
    @ds = @db[:i1]
    @ds2 = @db[:i2]
    @ds.import [:id, :parent_id], [[1,nil],[2,nil],[3,1],[4,1],[5,3],[6,5]]
  end
  after do
    @db.drop_table?(:i1, :i2)
  end

  it "using #with should be able to update" do
    @ds.insert(:id=>1)
    @ds2.insert(:id=>2, :parent_id=>1)
    @ds2.insert(:id=>3, :parent_id=>2)
    @ds.with(:t, @ds2).filter(:id => @db[:t].select(:id)).update(:parent_id => @db[:t].filter(:id => :i1__id).select(:parent_id).limit(1))
    @ds[:id => 1].must_equal(:id => 1, :parent_id => nil)
    @ds[:id => 2].must_equal(:id => 2, :parent_id => 1)
    @ds[:id => 3].must_equal(:id => 3, :parent_id => 2)
    @ds[:id => 4].must_equal(:id => 4, :parent_id => 1)
  end

  it "using #with_recursive should be able to update" do
    ds = @ds.with_recursive(:t, @ds.filter(:parent_id=>1).or(:id => 1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.exclude(:id => @db[:t].select(:i)).update(:parent_id => 1)
    @ds[:id => 1].must_equal(:id => 1, :parent_id => nil)
    @ds[:id => 2].must_equal(:id => 2, :parent_id => 1)
    @ds[:id => 5].must_equal(:id => 5, :parent_id => 3)
  end

  it "using #with should be able to insert" do
    @ds2.insert(:id=>7)
    @ds.with(:t, @ds2).insert(@db[:t])
    @ds[:id => 7].must_equal(:id => 7, :parent_id => nil)
  end

  it "using #with_recursive should be able to insert" do
    ds = @ds2.with_recursive(:t, @ds.filter(:parent_id=>1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.insert @db[:t]
    @ds2.all.must_equal [{:id => 3, :parent_id => 1}, {:id => 4, :parent_id => 1}, {:id => 5, :parent_id => 3}, {:id => 6, :parent_id => 5}]
  end

  it "using #with should be able to delete" do
    @ds2.insert(:id=>6)
    @ds2.insert(:id=>5)
    @ds2.insert(:id=>4)
    @ds.with(:t, @ds2).filter(:id => @db[:t].select(:id)).delete
    @ds.all.must_equal [{:id => 1, :parent_id => nil}, {:id => 2, :parent_id => nil}, {:id => 3, :parent_id => 1}]
  end

  it "using #with_recursive should be able to delete" do
    @ds.insert(:id=>7, :parent_id=>2)
    ds = @ds.with_recursive(:t, @ds.filter(:parent_id=>1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.filter(:i1__id => @db[:t].select(:i)).delete
    @ds.all.must_equal [{:id => 1, :parent_id => nil}, {:id => 2, :parent_id => nil}, {:id => 7, :parent_id => 2}]
  end

  it "using #with should be able to import" do
    @ds2.insert(:id=>7)
    @ds.with(:t, @ds2).import [:id, :parent_id], @db[:t].select(:id, :parent_id)
    @ds[:id => 7].must_equal(:id => 7, :parent_id => nil)
  end

  it "using #with_recursive should be able to import" do
    ds = @ds2.with_recursive(:t, @ds.filter(:parent_id=>1), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
    ds.import [:id, :parent_id], @db[:t].select(:i, :pi)
    @ds2.all.must_equal [{:id => 3, :parent_id => 1}, {:id => 4, :parent_id => 1}, {:id => 5, :parent_id => 3}, {:id => 6, :parent_id => 5}]
  end
end

describe "MSSSQL::Dataset#insert" do
  before do
    @db = DB
    @db.create_table!(:test5){primary_key :xid; Integer :value}
    @db.create_table! :test4 do
      String :name, :size => 20
      column :value, 'varbinary(max)'
    end
    @db.sqls.clear
    @ds = @db[:test5]
  end
  after do
    @db.drop_table?(:test5, :test4)
  end

  it "should have insert_select return nil if disable_insert_output is used" do
    @ds.disable_insert_output.insert_select(:value=>10).must_equal nil
  end
  
  it "should have insert_select return nil if the server version is not 2005+" do
    def @ds.server_version() 8000760 end
    @ds.insert_select(:value=>10).must_equal nil
  end

  it "should have insert_select insert the record and return the inserted record" do
    h = @ds.insert_select(:value=>10)
    h[:value].must_equal 10
    @ds.first(:xid=>h[:xid])[:value].must_equal 10
  end

  cspecify "should allow large text and binary values", [:odbc] do
    blob = Sequel::SQL::Blob.new("0" * (65*1024))
    @db[:test4].insert(:name => 'max varbinary test', :value => blob)
    b = @db[:test4].where(:name => 'max varbinary test').get(:value)
    b.length.must_equal blob.length
    b.must_equal blob
  end

  it "should play nicely with simple_select_all?" do
    DB[:test4].disable_insert_output.send(:simple_select_all?).must_equal true
  end
end

describe "MSSSQL::Dataset#into" do
  before do
    @db = DB
  end

  it "should format SELECT statement" do
    @db[:t].into(:new).select_sql.must_equal "SELECT * INTO [NEW] FROM [T]"
  end

  it "should select rows into a new table" do
    @db.create_table!(:t) {Integer :id; String :value}
    @db[:t].insert(:id => 1, :value => "test")
    @db << @db[:t].into(:new).select_sql
    @db[:new].all.must_equal [{:id => 1, :value => "test"}]
    @db.drop_table?(:t, :new)
  end
end

describe "A MSSQL database" do
  before do
    @db = DB
  end
  after do
    @db.drop_table?(:a)
  end
  
  it "should handle many existing types for set_column_allow_null" do
    @db.create_table!(:a){column :a, 'integer'}
    @db.alter_table(:a){set_column_allow_null :a, false}
    @db.create_table!(:a){column :a, 'decimal(24, 2)'}
    @db.alter_table(:a){set_column_allow_null :a, false}
    @db.schema(:a).first.last[:column_size].must_equal 24
    @db.schema(:a).first.last[:scale].must_equal 2
    @db.create_table!(:a){column :a, 'decimal(10)'}
    @db.schema(:a).first.last[:column_size].must_equal 10
    @db.schema(:a).first.last[:scale].must_equal 0
    @db.alter_table(:a){set_column_allow_null :a, false}
    @db.create_table!(:a){column :a, 'nchar(2)'}
    @db.alter_table(:a){set_column_allow_null :a, false}
    s = @db.schema(:a).first.last
    (s[:max_chars] || s[:column_size]).must_equal 2
  end
end

describe "MSSQL::Database#rename_table" do
  after do
    DB.drop_table?(:foo)
  end

  it "should work on non-schema bound tables which need escaping" do
    DB.quote_identifiers = true
    DB.create_table! :'foo bar' do
      text :name
    end
    DB.drop_table? :foo
    DB.rename_table 'foo bar', 'foo'
  end
  
  it "should work on schema bound tables" do
    DB.execute(<<-SQL)
      IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'MY')
        EXECUTE sp_executesql N'create schema MY'
    SQL
    DB.create_table! :MY__foo do
      text :name
    end
    DB.rename_table :MY__foo, :MY__bar
    DB.rename_table :MY__bar, :foo
  end
end

describe "MSSQL::Dataset#count" do
  it "should work with a distinct query with an order clause" do
    DB.create_table!(:items){String :name; Integer :value}
    DB[:items].insert(:name => "name", :value => 1)
    DB[:items].insert(:name => "name", :value => 1)
    DB[:items].select(:name, :value).distinct.order(:name).count.must_equal 1
    DB[:items].select(:name, :value).group(:name, :value).order(:name).count.must_equal 1
  end
end

describe "MSSQL::Database#create_table" do
  it "should support collate with various other column options" do
    DB.create_table!(:items){ String :name, :size => 128, :collate => :sql_latin1_general_cp1_ci_as, :default => 'foo', :null => false, :unique => true}
    DB[:items].insert
    DB[:items].select_map(:name).must_equal ["foo"]
  end
end

describe "MSSQL::Database#mssql_unicode_strings = false" do
  before do
    DB.mssql_unicode_strings = false
  end
  after do
    DB.drop_table?(:items)
    DB.mssql_unicode_strings = true
  end

  it "should work correctly" do
    DB.create_table!(:items){String :name}
    DB[:items].mssql_unicode_strings.must_equal false
    DB[:items].insert(:name=>'foo')
    DB[:items].select_map(:name).must_equal ['foo']
  end

  it "should be overridable at the dataset level" do
    DB.create_table!(:items){String :name}
    ds = DB[:items]
    ds.mssql_unicode_strings.must_equal false
    ds.mssql_unicode_strings = true
    ds.mssql_unicode_strings.must_equal true
    ds.insert(:name=>'foo')
    ds.select_map(:name).must_equal ['foo']
  end
end

describe "A MSSQL database adds index with include" do
  before :all do
    @table_name = :test_index_include
    @db = DB
    @db.create_table! @table_name do
      integer :col1
      integer :col2
      integer :col3
    end
  end

  after :all do
    @db.drop_table? @table_name
  end

  it "should be able add index with include" do
    @db.alter_table @table_name do
      add_index [:col1], :include => [:col2,:col3]
    end
    @db.indexes(@table_name).keys.must_include("#{@table_name}_col1_index".to_sym)
  end
end

describe "MSSQL::Database#drop_column with a schema" do
  before do
    DB.run "create schema test" rescue nil
  end
  after do
    DB.drop_table(:test__items)
    DB.run "drop schema test" rescue nil
  end

  it "drops columns with a default value" do
    DB.create_table!(:test__items){ Integer :id; String :name, :default => 'widget' }
    DB.drop_column(:test__items, :name)
    DB[:test__items].columns.must_equal [:id]
  end
end

describe "Database#foreign_key_list" do
  before(:all) do
    DB.create_table! :items do
      primary_key :id
      integer     :sku
    end
    DB.create_table! :prices do
      integer     :item_id
      datetime    :valid_from
      float       :price
      primary_key [:item_id, :valid_from]
      foreign_key [:item_id], :items, :key => :id, :name => :fk_prices_items
    end
    DB.create_table! :sales do
      integer  :id
      integer  :price_item_id
      datetime :price_valid_from
      foreign_key [:price_item_id, :price_valid_from], :prices, :key => [:item_id, :valid_from], :name => :fk_sales_prices, :on_delete => :cascade
    end
  end
  after(:all) do
    DB.drop_table :sales
    DB.drop_table :prices
    DB.drop_table :items
  end
  it "should support typical foreign keys" do
    DB.foreign_key_list(:prices).must_equal [{:name      => :fk_prices_items, 
                                                   :table     => :items, 
                                                   :columns   => [:item_id], 
                                                   :key       => [:id], 
                                                   :on_update => :no_action, 
                                                   :on_delete => :no_action }]
  end
  it "should support a foreign key with multiple columns" do
    DB.foreign_key_list(:sales).must_equal [{:name      => :fk_sales_prices, 
                                                  :table     => :prices, 
                                                  :columns   => [:price_item_id, :price_valid_from], 
                                                  :key       => [:item_id, :valid_from], 
                                                  :on_update => :no_action, 
                                                  :on_delete => :cascade }]
  end

  describe "with multiple schemas" do
    before(:all) do
      DB.execute_ddl "create schema vendor"
      DB.create_table! :vendor__vendors do
        primary_key :id
        varchar     :name
      end
      DB.create_table! :vendor__mapping do
        integer :vendor_id
        integer :item_id
        foreign_key [:vendor_id], :vendor__vendors, :name => :fk_mapping_vendor
        foreign_key [:item_id], :items, :name => :fk_mapping_item
      end
    end
    after(:all) do
      DB.drop_table? :vendor__mapping
      DB.drop_table? :vendor__vendors
      DB.execute_ddl "drop schema vendor"
    end
    it "should support mixed schema bound tables" do
 DB.foreign_key_list(:vendor__mapping).sort_by{|h| h[:name].to_s}.must_equal [{:name => :fk_mapping_item, :table => :items, :columns => [:item_id], :key => [:id], :on_update => :no_action, :on_delete => :no_action }, {:name => :fk_mapping_vendor, :table => Sequel.qualify(:vendor, :vendors), :columns => [:vendor_id], :key => [:id], :on_update => :no_action, :on_delete => :no_action }]
    end
  end
end

describe "MSSQL optimistic locking plugin" do
  before do
    @db = DB
    @db.create_table! :items do
      primary_key :id
      String :name, :size => 20
      column :timestamp, 'timestamp'
    end
   end
  after do
    @db.drop_table?(:items)
  end

  it "should not allow stale updates" do
    c = Class.new(Sequel::Model(:items))
    c.plugin :mssql_optimistic_locking
    o = c.create(:name=>'test')
    o2 = c.first
    ts = o.timestamp
    ts.wont_equal nil
    o.name = 'test2'
    o.save
    o.timestamp.wont_equal ts
    proc{o2.save}.must_raise(Sequel::NoExistingObject)
  end
end unless DB.adapter_scheme == :odbc

describe "MSSQL Stored Procedure support" do
  before do
    @db = DB
    @now = DateTime.now.to_s
    @db.execute('CREATE PROCEDURE dbo.SequelTest
      (@Input varchar(25), @IntegerInput int, @Output varchar(25) OUTPUT, @IntegerOutput int OUTPUT) AS
      BEGIN SET @Output = @Input SET @IntegerOutput = @IntegerInput RETURN @IntegerInput END')
  end
  after do
    @db.execute('DROP PROCEDURE dbo.SequelTest')
  end

  describe "with unnamed parameters" do
    it "should return a hash of output variables" do
      r = @db.call_mssql_sproc(:SequelTest, {:args => [@now, 1, :output, :output]})
      r.must_be_kind_of(Hash)
      r.values_at(:var2, :var3).must_equal [@now, '1']
    end

    it "should support typed output variables" do
      @db.call_mssql_sproc(:SequelTest, {:args => [@now, 1, :output, [:output, 'int']]})[:var3].must_equal 1
    end

    it "should support named output variables" do
      @db.call_mssql_sproc(:SequelTest, {:args => [@now, 1, [:output, nil, 'output'], :output]})[:output].must_equal @now
    end

    it "should return the number of Affected Rows" do
      @db.call_mssql_sproc(:SequelTest, {:args => [@now, 1, :output, :output]})[:numrows].must_equal 1
    end

    it "should return the Result Code" do
      @db.call_mssql_sproc(:SequelTest, {:args => [@now, 1, :output, :output]})[:result].must_equal 1
    end
  end

  describe "with named parameters" do
    it "should return a hash of output variables" do
      r = @db.call_mssql_sproc(:SequelTest, :args => {
        'Input' => @now,
        'IntegerInput' => 1,
        'Output' => [:output, nil, 'output'],
        'IntegerOutput' => [:output, nil, 'integer_output']
      })
      r.must_be_kind_of(Hash)
      r.values_at(:output, :integer_output).must_equal [@now, '1']
    end

    it "should support typed output variables" do
      @db.call_mssql_sproc(:SequelTest, :args => {
        'Input' => @now,
        'IntegerInput' => 1,
        'Output' => [:output, nil, 'output'],
        'IntegerOutput' => [:output, 'int', 'integer_output']
      })[:integer_output].must_equal 1
    end

    it "should return the number of Affected Rows" do
      @db.call_mssql_sproc(:SequelTest, :args => {
        'Input' => @now,
        'IntegerInput' => 1,
        'Output' => [:output, nil, 'output'],
        'IntegerOutput' => [:output, nil, 'integer_output']
      })[:numrows].must_equal 1
    end

    it "should return the Result Code" do
      @db.call_mssql_sproc(:SequelTest, :args => {
        'Input' => @now,
        'IntegerInput' => 1,
        'Output' => [:output, nil, 'output'],
        'IntegerOutput' => [:output, nil, 'integer_output']
      })[:result].must_equal 1
    end
  end
end unless DB.adapter_scheme == :odbc
