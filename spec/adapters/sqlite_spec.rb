require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

unless defined?(SQLITE_DB)
  SQLITE_URL = 'sqlite:/' unless defined? SQLITE_URL
  SQLITE_DB = Sequel.connect(ENV['SEQUEL_SQLITE_SPEC_DB']||SQLITE_URL)
end
INTEGRATION_DB = SQLITE_DB unless defined?(INTEGRATION_DB)

describe "An SQLite database" do
  before do
    @db = SQLITE_DB
    @fk = @db.foreign_keys
  end
  after do
    @db.foreign_keys = @fk
    Sequel.datetime_class = Time
  end

  if SQLITE_DB.auto_vacuum == :none
    specify "should support getting pragma values" do
      @db.pragma_get(:auto_vacuum).to_s.should == '0'
    end
    
    specify "should support setting pragma values" do
      @db.pragma_set(:auto_vacuum, '1')
      @db.pragma_get(:auto_vacuum).to_s.should == '1'
      @db.pragma_set(:auto_vacuum, '2')
      @db.pragma_get(:auto_vacuum).to_s.should == '2'
    end
    
    specify "should support getting and setting the auto_vacuum pragma" do
      @db.auto_vacuum = :full
      @db.auto_vacuum.should == :full
      @db.auto_vacuum = :incremental
      @db.auto_vacuum.should == :incremental
      
      proc {@db.auto_vacuum = :invalid}.should raise_error(Sequel::Error)
    end
  end
  
  specify "should provide the SQLite version as an integer" do
    @db.sqlite_version.should be_a_kind_of(Integer)
  end
  
  specify "should support setting and getting the foreign_keys pragma" do
    (@db.sqlite_version >= 30619 ? [true, false] : [nil]).should include(@db.foreign_keys)
    @db.foreign_keys = true
    @db.foreign_keys = false
  end
  
  if SQLITE_DB.sqlite_version >= 30619
    specify "should enforce foreign key integrity if foreign_keys pragma is set" do
      @db.foreign_keys = true
      @db.create_table!(:fk){primary_key :id; foreign_key :parent_id, :fk}
      @db[:fk].insert(1, nil)
      @db[:fk].insert(2, 1)
      @db[:fk].insert(3, 3)
      proc{@db[:fk].insert(4, 5)}.should raise_error(Sequel::Error)
    end
  end
  
  specify "should not enforce foreign key integrity if foreign_keys pragma is unset" do
    @db.foreign_keys = false
    @db.create_table!(:fk){primary_key :id; foreign_key :parent_id, :fk}
    @db[:fk].insert(1, 2)
    @db[:fk].all.should == [{:id=>1, :parent_id=>2}]
  end
  
  specify "should support a use_timestamp_timezones setting" do
    @db.create_table!(:time){Time :time}
    @db[:time].insert(Time.now)
    @db[:time].get(:time.cast_string).should =~ /[-+]\d\d\d\d\z/
    @db.use_timestamp_timezones = false
    @db[:time].delete
    @db[:time].insert(Time.now)
    @db[:time].get(:time.cast_string).should_not =~ /[-+]\d\d\d\d\z/
    @db.use_timestamp_timezones = true
  end
  
  specify "should provide a list of existing tables" do
    @db.drop_table(:testing) rescue nil
    @db.tables.should be_a_kind_of(Array)
    @db.tables.should_not include(:testing)
    @db.create_table! :testing do
      text :name
    end
    @db.tables.should include(:testing)
  end

  specify "should support getting and setting the synchronous pragma" do
    @db.synchronous = :off
    @db.synchronous.should == :off
    @db.synchronous = :normal
    @db.synchronous.should == :normal
    @db.synchronous = :full
    @db.synchronous.should == :full
    
    proc {@db.synchronous = :invalid}.should raise_error(Sequel::Error)
  end
  
  specify "should support getting and setting the temp_store pragma" do
    @db.temp_store = :default
    @db.temp_store.should == :default
    @db.temp_store = :file
    @db.temp_store.should == :file
    @db.temp_store = :memory
    @db.temp_store.should == :memory
    
    proc {@db.temp_store = :invalid}.should raise_error(Sequel::Error)
  end
  
  cspecify "should support timestamps and datetimes and respect datetime_class", :do, :jdbc, :amalgalite, :swift do
    @db.create_table!(:time){timestamp :t; datetime :d}
    t1 = Time.at(1)
    @db[:time] << {:t => t1, :d => t1}
    @db[:time].map(:t).should == [t1]
    @db[:time].map(:d).should == [t1]
    Sequel.datetime_class = DateTime
    t2 = Sequel.string_to_datetime(t1.iso8601)
    @db[:time].map(:t).should == [t2]
    @db[:time].map(:d).should == [t2]
  end
  
  specify "should support sequential primary keys" do
    @db.create_table!(:with_pk) {primary_key :id; text :name}
    @db[:with_pk] << {:name => 'abc'}
    @db[:with_pk] << {:name => 'def'}
    @db[:with_pk] << {:name => 'ghi'}
    @db[:with_pk].order(:name).all.should == [
      {:id => 1, :name => 'abc'},
      {:id => 2, :name => 'def'},
      {:id => 3, :name => 'ghi'}
    ]
  end
  
  specify "should correctly parse the schema" do
    @db.create_table!(:time2) {timestamp :t}
    @db.schema(:time2, :reload=>true).should == [[:t, {:type=>:datetime, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"timestamp", :primary_key=>false}]]
  end
end

describe "An SQLite dataset" do
  before do
    @d = SQLITE_DB[:items]
  end
  
  specify "should handle string pattern matches correctly" do
    @d.literal(:x.like('a')).should == "(x LIKE 'a')"
    @d.literal(~:x.like('a')).should == "NOT (x LIKE 'a')"
    @d.literal(:x.ilike('a')).should == "(x LIKE 'a')"
    @d.literal(~:x.ilike('a')).should == "NOT (x LIKE 'a')"
  end

  specify "should raise errors if given a regexp pattern match" do
    proc{@d.literal(:x.like(/a/))}.should raise_error(Sequel::Error)
    proc{@d.literal(~:x.like(/a/))}.should raise_error(Sequel::Error)
    proc{@d.literal(:x.like(/a/i))}.should raise_error(Sequel::Error)
    proc{@d.literal(~:x.like(/a/i))}.should raise_error(Sequel::Error)
  end
end

describe "An SQLite numeric column" do
  specify "should handle and return BigDecimal values" do
    SQLITE_DB.create_table!(:d){numeric :d}
    d = SQLITE_DB[:d]
    d.insert(:d=>BigDecimal.new('80.0'))
    d.insert(:d=>BigDecimal.new('NaN'))
    d.insert(:d=>BigDecimal.new('Infinity'))
    d.insert(:d=>BigDecimal.new('-Infinity'))
    ds = d.all
    ds.shift.should == {:d=>BigDecimal.new('80.0')}
    ds.map{|x| x[:d].to_s}.should == %w'NaN Infinity -Infinity'
  end
end

describe "An SQLite dataset AS clause" do
  specify "should use a string literal for :col___alias" do
    SQLITE_DB.literal(:c___a).should == "c AS 'a'"
  end

  specify "should use a string literal for :table__col___alias" do
    SQLITE_DB.literal(:t__c___a).should == "t.c AS 'a'"
  end

  specify "should use a string literal for :column.as(:alias)" do
    SQLITE_DB.literal(:c.as(:a)).should == "c AS 'a'"
  end

  specify "should use a string literal in the SELECT clause" do
    SQLITE_DB[:t].select(:c___a).sql.should == "SELECT c AS 'a' FROM t"
  end

  specify "should use a string literal in the FROM clause" do
    SQLITE_DB[:t___a].sql.should == "SELECT * FROM t AS 'a'"
  end

  specify "should use a string literal in the JOIN clause" do
    SQLITE_DB[:t].join_table(:natural, :j, nil, :a).sql.should == "SELECT * FROM t NATURAL JOIN j AS 'a'"
  end
end

describe "SQLite::Dataset#delete" do
  before do
    SQLITE_DB.create_table! :items do
      primary_key :id
      String :name
      Float :value
    end
    @d = SQLITE_DB[:items]
    @d.delete # remove all records
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'def', :value => 4.56}
    @d << {:name => 'ghi', :value => 7.89}
  end
  
  specify "should return the number of records affected when filtered" do
    @d.count.should == 3
    @d.filter(:value.sql_number < 3).delete.should == 1
    @d.count.should == 2

    @d.filter(:value.sql_number < 3).delete.should == 0
    @d.count.should == 2
  end
  
  specify "should return the number of records affected when unfiltered" do
    @d.count.should == 3
    @d.delete.should == 3
    @d.count.should == 0

    @d.delete.should == 0
  end
end

describe "SQLite::Dataset#update" do
  before do
    SQLITE_DB.create_table! :items do
      primary_key :id
      String :name
      Float :value
    end
    @d = SQLITE_DB[:items]
    @d.delete # remove all records
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'def', :value => 4.56}
    @d << {:name => 'ghi', :value => 7.89}
  end
  
  specify "should return the number of records affected" do
    @d.filter(:name => 'abc').update(:value => 2).should == 1
    
    @d.update(:value => 10).should == 3
    
    @d.filter(:name => 'xxx').update(:value => 23).should == 0
  end
end

describe "SQLite dataset" do
  before do
    SQLITE_DB.create_table! :test do
      primary_key :id
      String :name
      Float :value
    end
    SQLITE_DB.create_table! :items do
      primary_key :id
      String :name
      Float :value
    end
    @d = SQLITE_DB[:items]
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'def', :value => 4.56}
    @d << {:name => 'ghi', :value => 7.89}
  end
  after do
    SQLITE_DB.drop_table(:test, :items)
  end
  
  specify "should be able to insert from a subquery" do
    SQLITE_DB[:test] << @d
    SQLITE_DB[:test].count.should == 3
    SQLITE_DB[:test].select(:name, :value).order(:value).to_a.should == \
      @d.select(:name, :value).order(:value).to_a
  end
    
  specify "should support #explain" do
    SQLITE_DB[:test].explain.should be_a_kind_of(Array)
  end
  
  specify "should have #explain work when identifier_output_method is modified" do
    ds = SQLITE_DB[:test]
    ds.identifier_output_method = :upcase
    ds.explain.should be_a_kind_of(Array)
  end
end

describe "A SQLite database" do
  before do
    @db = SQLITE_DB
    @db.create_table! :test2 do
      text :name
      integer :value
    end
  end

  specify "should support add_column operations" do
    @db.add_column :test2, :xyz, :text
    
    @db[:test2].columns.should == [:name, :value, :xyz]
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz=>'000'}
    @db[:test2].first.should == {:name => 'mmm', :value => 111, :xyz=>'000'}
  end
  
  specify "should support drop_column operations" do
    @db.drop_column :test2, :value
    @db[:test2].columns.should == [:name]
    @db[:test2] << {:name => 'mmm'}
    @db[:test2].first.should == {:name => 'mmm'}
  end
  
  specify "should support drop_column operations in a transaction" do
    @db.transaction{@db.drop_column :test2, :value}
    @db[:test2].columns.should == [:name]
    @db[:test2] << {:name => 'mmm'}
    @db[:test2].first.should == {:name => 'mmm'}
  end

  specify "should keep column attributes when dropping a column" do
    @db.create_table! :test3 do
      primary_key :id
      text :name
      integer :value
    end

    # This lame set of additions and deletions are to test that the primary keys
    # don't get messed up when we recreate the database.
    @db[:test3] << { :name => "foo", :value => 1}
    @db[:test3] << { :name => "foo", :value => 2}
    @db[:test3] << { :name => "foo", :value => 3}
    @db[:test3].filter(:id => 2).delete

    @db.drop_column :test3, :value

    @db['PRAGMA table_info(?)', :test3][:id][:pk].to_i.should == 1
    @db[:test3].select(:id).all.should == [{:id => 1}, {:id => 3}]
  end

  if SQLITE_DB.foreign_keys
    specify "should keep foreign keys when dropping a column" do
      @db.create_table! :test do
        primary_key :id
        String :name
        Integer :value
      end
      @db.create_table! :test3 do
        String :name
        Integer :value
        foreign_key :test_id, :test, :on_delete => :set_null, :on_update => :cascade
      end

      @db[:test3].insert(:name => "abc", :test_id => @db[:test].insert(:name => "foo", :value => 3))
      @db[:test3].insert(:name => "def", :test_id => @db[:test].insert(:name => "bar", :value => 4))

      @db.drop_column :test3, :value

      @db[:test].filter(:name => 'bar').delete
      @db[:test3][:name => 'def'][:test_id].should be_nil

      @db[:test].filter(:name => 'foo').update(:id=>100)
      @db[:test3][:name => 'abc'][:test_id].should == 100

      @db.drop_table :test, :test3
    end
  end

  specify "should support rename_column operations" do
    @db[:test2].delete
    @db.add_column :test2, :xyz, :text
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 'qqqq'}

    @db[:test2].columns.should == [:name, :value, :xyz]
    @db.rename_column :test2, :xyz, :zyx, :type => :text
    @db[:test2].columns.should == [:name, :value, :zyx]
    @db[:test2].first[:zyx].should == 'qqqq'
    @db[:test2].count.should eql(1)
  end
  
  specify "should preserve defaults when dropping or renaming columns" do
    @db.create_table! :test3 do
      String :s, :default=>'a'
      Integer :i
    end

    @db[:test3].insert
    @db[:test3].first[:s].should == 'a'
    @db[:test3].delete
    @db.drop_column :test3, :i
    @db[:test3].insert
    @db[:test3].first[:s].should == 'a'
    @db[:test3].delete
    @db.rename_column :test3, :s, :t
    @db[:test3].insert
    @db[:test3].first[:t].should == 'a'
    @db[:test3].delete
  end
  
  specify "should handle quoted tables when dropping or renaming columns" do
    @db.quote_identifiers = true
    table_name = "T T"
    @db.drop_table(table_name) rescue nil
    @db.create_table! table_name do
      Integer :"s s"
      Integer :"i i"
    end

    @db.from(table_name).insert(:"s s"=>1, :"i i"=>2)
    @db.from(table_name).all.should == [{:"s s"=>1, :"i i"=>2}]
    @db.drop_column table_name, :"i i"
    @db.from(table_name).all.should == [{:"s s"=>1}]
    @db.rename_column table_name, :"s s", :"t t"
    @db.from(table_name).all.should == [{:"t t"=>1}]
  end
  
  specify "should choose a temporary table name that isn't already used when dropping or renaming columns" do
    sqls = []
    @db.loggers << (l=Class.new{%w'info error'.each{|m| define_method(m){|sql| sqls << sql}}}.new)
    @db.create_table! :test3 do
      Integer :h
      Integer :i
    end
    @db.create_table! :test3_backup0 do
      Integer :j
    end
    @db.create_table! :test3_backup1 do
      Integer :k
    end

    @db[:test3].columns.should == [:h, :i]
    @db[:test3_backup0].columns.should == [:j]
    @db[:test3_backup1].columns.should == [:k]
    sqls.clear
    @db.drop_column(:test3, :i)
    sqls.any?{|x| x =~ /\ACREATE TABLE.*test3_backup2/}.should == true
    sqls.any?{|x| x =~ /\ACREATE TABLE.*test3_backup[01]/}.should == false
    @db[:test3].columns.should == [:h]
    @db[:test3_backup0].columns.should == [:j]
    @db[:test3_backup1].columns.should == [:k]

    @db.create_table! :test3_backup2 do
      Integer :l
    end

    sqls.clear
    @db.rename_column(:test3, :h, :i)
    sqls.any?{|x| x =~ /\ACREATE TABLE.*test3_backup3/}.should == true
    sqls.any?{|x| x =~ /\ACREATE TABLE.*test3_backup[012]/}.should == false
    @db[:test3].columns.should == [:i]
    @db[:test3_backup0].columns.should == [:j]
    @db[:test3_backup1].columns.should == [:k]
    @db[:test3_backup2].columns.should == [:l]
    @db.loggers.delete(l)
  end
  
  specify "should support add_index" do
    @db.add_index :test2, :value, :unique => true
    @db.add_index :test2, [:name, :value]
  end
  
  specify "should support drop_index" do
    @db.add_index :test2, :value, :unique => true
    @db.drop_index :test2, :value
  end

  specify "should keep applicable indexes when emulating schema methods" do
    @db.create_table!(:a){Integer :a; Integer :b}
    @db.add_index :a, :a
    @db.add_index :a, :b
    @db.add_index :a, [:b, :a]
    @db.drop_column :a, :b
    @db.indexes(:a).should == {:a_a_index=>{:unique=>false, :columns=>[:a]}}
  end
end  
