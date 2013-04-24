require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

unless defined?(POSTGRES_DB)
  POSTGRES_URL = 'postgres://postgres:postgres@localhost:5432/reality_spec' unless defined? POSTGRES_URL
  POSTGRES_DB = Sequel.connect(ENV['SEQUEL_PG_SPEC_DB']||POSTGRES_URL)
end
INTEGRATION_DB = POSTGRES_DB unless defined?(INTEGRATION_DB)

def POSTGRES_DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  POSTGRES_DB.sqls << msg
end
POSTGRES_DB.loggers << logger

#POSTGRES_DB.instance_variable_set(:@server_version, 80200)

describe "PostgreSQL", '#create_table' do
  before do
    @db = POSTGRES_DB
    POSTGRES_DB.sqls.clear
  end
  after do
    @db.drop_table?(:tmp_dolls)
    @db.drop_table?(:unlogged_dolls)
  end

  specify "should create a temporary table" do
    @db.create_table(:tmp_dolls, :temp => true){text :name}
    check_sqls do
      @db.sqls.should == ['CREATE TEMPORARY TABLE "tmp_dolls" ("name" text)']
    end
  end

  specify "should create an unlogged table" do
    @db.create_table(:unlogged_dolls, :unlogged => true){text :name}
    check_sqls do
      @db.sqls.should == ['CREATE UNLOGGED TABLE "unlogged_dolls" ("name" text)']
    end
  end

  specify "should not allow to pass both :temp and :unlogged" do
    proc do
      @db.create_table(:temp_unlogged_dolls, :temp => true, :unlogged => true){text :name}
    end.should raise_error(Sequel::Error, "can't provide both :temp and :unlogged to create_table")
  end
end

describe "PostgreSQL temporary views" do
  before do
    @db = POSTGRES_DB
    @db.drop_view(:items_view) rescue nil
    @db.create_table!(:items){Integer :number}
    @db[:items].insert(10)
    @db[:items].insert(20)
  end
  after do
    @db.drop_table?(:items)
  end

  specify "should be supported" do
    @db.create_view(:items_view, @db[:items].where(:number=>10), :temp=>true)
    @db[:items_view].map(:number).should == [10]
    @db.create_or_replace_view(:items_view, @db[:items].where(:number=>20),  :temp=>true)
    @db[:items_view].map(:number).should == [20]
    @db.disconnect
    lambda{@db[:items_view].map(:number)}.should raise_error(Sequel::DatabaseError)
  end
end unless POSTGRES_DB.adapter_scheme == :do # Causes freezing later
    
describe "A PostgreSQL database" do
  before(:all) do
    @db = POSTGRES_DB
    @db.create_table!(:public__testfk){primary_key :id; foreign_key :i, :public__testfk}
  end
  after(:all) do
    @db.drop_table?(:public__testfk)
  end

  specify "should provide the server version" do
    @db.server_version.should > 70000
  end

  specify "should support a :qualify option to tables and views" do
    @db.tables(:qualify=>true).should include(Sequel.qualify(:public, :testfk))
    begin
      @db.create_view(:testfkv, @db[:testfk])
      @db.views(:qualify=>true).should include(Sequel.qualify(:public, :testfkv))
    ensure
      @db.drop_view(:testfkv)
    end
  end

  specify "should not typecast the int2vector type incorrectly" do
    @db.get(Sequel.cast('10 20', :int2vector)).should_not == 10
  end

  cspecify "should not typecast the money type incorrectly", :do do
    @db.get(Sequel.cast('10.01', :money)).should_not == 0
  end

  specify "should correctly parse the schema" do
    @db.schema(:public__testfk, :reload=>true).should == [
      [:id, {:type=>:integer, :ruby_default=>nil, :db_type=>"integer", :default=>"nextval('testfk_id_seq'::regclass)", :oid=>23, :primary_key=>true, :allow_null=>false}],
      [:i, {:type=>:integer, :ruby_default=>nil, :db_type=>"integer", :default=>nil, :oid=>23, :primary_key=>false, :allow_null=>true}]]
  end

  specify "should parse foreign keys for tables in a schema" do
    @db.foreign_key_list(:public__testfk).should == [{:on_delete=>:no_action, :on_update=>:no_action, :columns=>[:i], :key=>[:id], :deferrable=>false, :table=>Sequel.qualify(:public, :testfk), :name=>:testfk_i_fkey}]
  end

  specify "should return uuid fields as strings" do
    @db.get(Sequel.cast('550e8400-e29b-41d4-a716-446655440000', :uuid)).should == '550e8400-e29b-41d4-a716-446655440000'
  end
end

describe "A PostgreSQL dataset" do
  before(:all) do
    @db = POSTGRES_DB
    @d = @db[:test]
    @db.create_table! :test do
      text :name
      integer :value, :index => true
    end
  end
  before do
    @d.delete
    @db.sqls.clear
  end
  after do
    @db.drop_table?(:atest)
  end
  after(:all) do
    @db.drop_table?(:test)
  end

  specify "should quote columns and tables using double quotes if quoting identifiers" do
    check_sqls do
      @d.select(:name).sql.should == 'SELECT "name" FROM "test"'
      @d.select(Sequel.lit('COUNT(*)')).sql.should == 'SELECT COUNT(*) FROM "test"'
      @d.select(Sequel.function(:max, :value)).sql.should == 'SELECT max("value") FROM "test"'
      @d.select(Sequel.function(:NOW)).sql.should == 'SELECT NOW() FROM "test"'
      @d.select(Sequel.function(:max, :items__value)).sql.should == 'SELECT max("items"."value") FROM "test"'
      @d.order(Sequel.desc(:name)).sql.should == 'SELECT * FROM "test" ORDER BY "name" DESC'
      @d.select(Sequel.lit('test.name AS item_name')).sql.should == 'SELECT test.name AS item_name FROM "test"'
      @d.select(Sequel.lit('"name"')).sql.should == 'SELECT "name" FROM "test"'
      @d.select(Sequel.lit('max(test."name") AS "max_name"')).sql.should == 'SELECT max(test."name") AS "max_name" FROM "test"'
      @d.insert_sql(:x => :y).should =~ /\AINSERT INTO "test" \("x"\) VALUES \("y"\)( RETURNING NULL)?\z/

      @d.select(Sequel.function(:test, :abc, 'hello')).sql.should == "SELECT test(\"abc\", 'hello') FROM \"test\""
      @d.select(Sequel.function(:test, :abc__def, 'hello')).sql.should == "SELECT test(\"abc\".\"def\", 'hello') FROM \"test\""
      @d.select(Sequel.function(:test, :abc__def, 'hello').as(:x2)).sql.should == "SELECT test(\"abc\".\"def\", 'hello') AS \"x2\" FROM \"test\""
      @d.insert_sql(:value => 333).should =~ /\AINSERT INTO "test" \("value"\) VALUES \(333\)( RETURNING NULL)?\z/
    end
  end

  specify "should quote fields correctly when reversing the order if quoting identifiers" do
    check_sqls do
      @d.reverse_order(:name).sql.should == 'SELECT * FROM "test" ORDER BY "name" DESC'
      @d.reverse_order(Sequel.desc(:name)).sql.should == 'SELECT * FROM "test" ORDER BY "name" ASC'
      @d.reverse_order(:name, Sequel.desc(:test)).sql.should == 'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC'
      @d.reverse_order(Sequel.desc(:name), :test).sql.should == 'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC'
    end
  end

  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}
    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end

  specify "should support NULLS FIRST and NULLS LAST" do
    @d << {:name => 'abc'}
    @d << {:name => 'bcd'}
    @d << {:name => 'bcd', :value => 2}
    @d.order(Sequel.asc(:value, :nulls=>:first), :name).select_map(:name).should == %w[abc bcd bcd]
    @d.order(Sequel.asc(:value, :nulls=>:last), :name).select_map(:name).should == %w[bcd abc bcd]
    @d.order(Sequel.asc(:value, :nulls=>:first), :name).reverse.select_map(:name).should == %w[bcd bcd abc]
  end

  specify "#lock should lock tables and yield if a block is given" do
    @d.lock('EXCLUSIVE'){@d.insert(:name=>'a')}
  end

  specify "should support exclusion constraints when creating or altering tables" do
    @db.create_table!(:atest){Integer :t; exclude [[Sequel.desc(:t, :nulls=>:last), '=']], :using=>:btree, :where=>proc{t > 0}}
    @db[:atest].insert(1)
    @db[:atest].insert(2)
    proc{@db[:atest].insert(2)}.should raise_error(Sequel::Postgres::ExclusionConstraintViolation)

    @db.create_table!(:atest){Integer :t}
    @db.alter_table(:atest){add_exclusion_constraint [[:t, '=']], :using=>:btree, :name=>'atest_ex'}
    @db[:atest].insert(1)
    @db[:atest].insert(2)
    proc{@db[:atest].insert(2)}.should raise_error(Sequel::Postgres::ExclusionConstraintViolation)
    @db.alter_table(:atest){drop_constraint 'atest_ex'}
  end if POSTGRES_DB.server_version >= 90000

  specify "should support Database#do for executing anonymous code blocks" do
    @db.drop_table?(:btest)
    @db.do "BEGIN EXECUTE 'CREATE TABLE btest (a INTEGER)'; EXECUTE 'INSERT INTO btest VALUES (1)'; END"
    @db[:btest].select_map(:a).should == [1]

    @db.do "BEGIN EXECUTE 'DROP TABLE btest; CREATE TABLE atest (a INTEGER)'; EXECUTE 'INSERT INTO atest VALUES (1)'; END", :language=>:plpgsql
    @db[:atest].select_map(:a).should == [1]
  end if POSTGRES_DB.server_version >= 90000

  specify "should support adding foreign key constarints that are not yet valid, and validating them later" do
    @db.create_table!(:atest){primary_key :id; Integer :fk}
    @db[:atest].insert(1, 5)
    @db.alter_table(:atest){add_foreign_key [:fk], :atest, :not_valid=>true, :name=>:atest_fk}
    @db[:atest].insert(2, 1)
    proc{@db[:atest].insert(3, 4)}.should raise_error(Sequel::DatabaseError)

    proc{@db.alter_table(:atest){validate_constraint :atest_fk}}.should raise_error(Sequel::DatabaseError)
    @db[:atest].where(:id=>1).update(:fk=>2)
    @db.alter_table(:atest){validate_constraint :atest_fk}
    proc{@db.alter_table(:atest){validate_constraint :atest_fk}}.should_not raise_error
  end if POSTGRES_DB.server_version >= 90200

  specify "should support :using when altering a column's type" do
    @db.create_table!(:atest){Integer :t}
    @db[:atest].insert(1262304000)
    @db.alter_table(:atest){set_column_type :t, Time, :using=>Sequel.cast('epoch', Time) + Sequel.cast('1 second', :interval) * :t}
    @db[:atest].get(Sequel.extract(:year, :t)).should == 2010
  end

  specify "should support :using with a string when altering a column's type" do
    @db.create_table!(:atest){Integer :t}
    @db[:atest].insert(1262304000)
    @db.alter_table(:atest){set_column_type :t, Time, :using=>"'epoch'::timestamp + '1 second'::interval * t"}
    @db[:atest].get(Sequel.extract(:year, :t)).should == 2010
  end

  specify "should be able to parse the default value for an interval type" do
    @db.create_table!(:atest){interval :t, :default=>'1 week'}
    @db.schema(:atest).first.last[:ruby_default].should == '7 days'
  end

  specify "should have #transaction support various types of synchronous options" do
    @db.transaction(:synchronous=>:on){}
    @db.transaction(:synchronous=>true){}
    @db.transaction(:synchronous=>:off){}
    @db.transaction(:synchronous=>false){}
    @db.sqls.grep(/synchronous/).should == ["SET LOCAL synchronous_commit = on", "SET LOCAL synchronous_commit = on", "SET LOCAL synchronous_commit = off", "SET LOCAL synchronous_commit = off"]

    @db.sqls.clear
    @db.transaction(:synchronous=>nil){}
    check_sqls do
      @db.sqls.should == ['BEGIN', 'COMMIT']
    end

    if @db.server_version >= 90100
      @db.sqls.clear
      @db.transaction(:synchronous=>:local){}
      check_sqls do
        @db.sqls.grep(/synchronous/).should == ["SET LOCAL synchronous_commit = local"]
      end

      if @db.server_version >= 90200
        @db.sqls.clear
        @db.transaction(:synchronous=>:remote_write){}
        check_sqls do
          @db.sqls.grep(/synchronous/).should == ["SET LOCAL synchronous_commit = remote_write"]
        end
      end
    end
  end

  specify "should have #transaction support read only transactions" do
    @db.transaction(:read_only=>true){}
    @db.transaction(:read_only=>false){}
    @db.transaction(:isolation=>:serializable, :read_only=>true){}
    @db.transaction(:isolation=>:serializable, :read_only=>false){}
    @db.sqls.grep(/READ/).should == ["SET TRANSACTION READ ONLY", "SET TRANSACTION READ WRITE", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE"]
  end

  specify "should have #transaction support deferrable transactions" do
    @db.transaction(:deferrable=>true){}
    @db.transaction(:deferrable=>false){}
    @db.transaction(:deferrable=>true, :read_only=>true){}
    @db.transaction(:deferrable=>false, :read_only=>false){}
    @db.transaction(:isolation=>:serializable, :deferrable=>true, :read_only=>true){}
    @db.transaction(:isolation=>:serializable, :deferrable=>false, :read_only=>false){}
    @db.sqls.grep(/DEF/).should == ["SET TRANSACTION DEFERRABLE", "SET TRANSACTION NOT DEFERRABLE", "SET TRANSACTION READ ONLY DEFERRABLE", "SET TRANSACTION READ WRITE NOT DEFERRABLE",  "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE NOT DEFERRABLE"]
  end if POSTGRES_DB.server_version >= 90100

  specify "should support creating indexes concurrently" do
    @db.add_index :test, [:name, :value], :concurrently=>true
    check_sqls do
      @db.sqls.should == ['CREATE INDEX CONCURRENTLY "test_name_value_index" ON "test" ("name", "value")']
    end
  end

  specify "should support dropping indexes only if they already exist" do
    @db.add_index :test, [:name, :value], :name=>'tnv1'
    @db.sqls.clear
    @db.drop_index :test, [:name, :value], :if_exists=>true, :name=>'tnv1'
    check_sqls do
      @db.sqls.should == ['DROP INDEX IF EXISTS "tnv1"']
    end
  end

  specify "should support CASCADE when dropping indexes" do
    @db.add_index :test, [:name, :value], :name=>'tnv2'
    @db.sqls.clear
    @db.drop_index :test, [:name, :value], :cascade=>true, :name=>'tnv2'
    check_sqls do
      @db.sqls.should == ['DROP INDEX "tnv2" CASCADE']
    end
  end

  specify "should support dropping indexes concurrently" do
    @db.add_index :test, [:name, :value], :name=>'tnv2'
    @db.sqls.clear
    @db.drop_index :test, [:name, :value], :concurrently=>true, :name=>'tnv2'
    check_sqls do
      @db.sqls.should == ['DROP INDEX CONCURRENTLY "tnv2"']
    end
  end if POSTGRES_DB.server_version >= 90200

  specify "#lock should lock table if inside a transaction" do
    @db.transaction{@d.lock('EXCLUSIVE'); @d.insert(:name=>'a')}
  end

  specify "#lock should return nil" do
    @d.lock('EXCLUSIVE'){@d.insert(:name=>'a')}.should == nil
    @db.transaction{@d.lock('EXCLUSIVE').should == nil; @d.insert(:name=>'a')}
  end

  specify "should raise an error if attempting to update a joined dataset with a single FROM table" do
    proc{@db[:test].join(:test, [:name]).update(:name=>'a')}.should raise_error(Sequel::Error, 'Need multiple FROM tables if updating/deleting a dataset with JOINs')
  end

  specify "should truncate with options" do
    @d << { :name => 'abc', :value => 1}
    @d.count.should == 1
    @d.truncate(:cascade => true)
    @d.count.should == 0
    if @d.db.server_version > 80400
      @d << { :name => 'abc', :value => 1}
      @d.truncate(:cascade => true, :only=>true, :restart=>true)
      @d.count.should == 0
    end
  end

  specify "should truncate multiple tables at once" do
    tables = [:test, :test]
    tables.each{|t| @d.from(t).insert}
    @d.from(:test, :test).truncate
    tables.each{|t| @d.from(t).count.should == 0}
  end
end

describe "Dataset#distinct" do
  before do
    @db = POSTGRES_DB
    @db.create_table!(:a) do
      Integer :a
      Integer :b
    end
    @ds = @db[:a]
  end
  after do
    @db.drop_table?(:a)
  end

  it "#distinct with arguments should return results distinct on those arguments" do
    @ds.insert(20, 10)
    @ds.insert(30, 10)
    @ds.order(:b, :a).distinct.map(:a).should == [20, 30]
    @ds.order(:b, Sequel.desc(:a)).distinct.map(:a).should == [30, 20]
    @ds.order(:b, :a).distinct(:b).map(:a).should == [20]
    @ds.order(:b, Sequel.desc(:a)).distinct(:b).map(:a).should == [30]
  end
end

if POSTGRES_DB.pool.respond_to?(:max_size) and POSTGRES_DB.pool.max_size > 1
  describe "Dataset#for_update support" do
    before do
      @db = POSTGRES_DB.create_table!(:items) do
        primary_key :id
        Integer :number
        String :name
      end
      @ds = POSTGRES_DB[:items]
    end
    after do
      POSTGRES_DB.drop_table?(:items)
      POSTGRES_DB.disconnect
    end

    specify "should handle FOR UPDATE" do
      @ds.insert(:number=>20)
      c, t = nil, nil
      q = Queue.new
      POSTGRES_DB.transaction do
        @ds.for_update.first(:id=>1)
        t = Thread.new do
          POSTGRES_DB.transaction do
            q.push nil
            @ds.filter(:id=>1).update(:name=>'Jim')
            c = @ds.first(:id=>1)
            q.push nil
          end
        end
        q.pop
        @ds.filter(:id=>1).update(:number=>30)
      end
      q.pop
      t.join
      c.should == {:id=>1, :number=>30, :name=>'Jim'}
    end

    specify "should handle FOR SHARE" do
      @ds.insert(:number=>20)
      c, t = nil
      q = Queue.new
      POSTGRES_DB.transaction do
        @ds.for_share.first(:id=>1)
        t = Thread.new do
          POSTGRES_DB.transaction do
            c = @ds.for_share.filter(:id=>1).first
            q.push nil
          end
        end
        q.pop
        @ds.filter(:id=>1).update(:name=>'Jim')
        c.should == {:id=>1, :number=>20, :name=>nil}
      end
      t.join
    end
  end
end

describe "A PostgreSQL dataset with a timestamp field" do
  before(:all) do
    @db = POSTGRES_DB
    @db.create_table! :test3 do
      Date :date
      DateTime :time
    end
    @d = @db[:test3]
  end
  before do
    @d.delete
  end
  after do
    @db.convert_infinite_timestamps = false if @db.adapter_scheme == :postgres
  end
  after(:all) do
    @db.drop_table?(:test3)
  end

  cspecify "should store milliseconds in time fields for Time objects", :do, :swift do
    t = Time.now
    @d << {:time=>t}
    t2 = @d.get(:time)
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    (t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).should == t.usec
  end

  cspecify "should store milliseconds in time fields for DateTime objects", :do, :swift do
    t = DateTime.now
    @d << {:time=>t}
    t2 = @d.get(:time)
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    (t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).should == t.strftime('%N').to_i/1000
  end

  if POSTGRES_DB.adapter_scheme == :postgres
    specify "should handle infinite timestamps if convert_infinite_timestamps is set" do
      @d << {:time=>Sequel.cast('infinity', DateTime)}
      @db.convert_infinite_timestamps = :nil
      @db[:test3].get(:time).should == nil
      @db.convert_infinite_timestamps = :string
      @db[:test3].get(:time).should == 'infinity'
      @db.convert_infinite_timestamps = :float
      @db[:test3].get(:time).should == 1.0/0.0

      @d.update(:time=>Sequel.cast('-infinity', DateTime))
      @db.convert_infinite_timestamps = :nil
      @db[:test3].get(:time).should == nil
      @db.convert_infinite_timestamps = :string
      @db[:test3].get(:time).should == '-infinity'
      @db.convert_infinite_timestamps = :float
      @db[:test3].get(:time).should == -1.0/0.0
    end

    specify "should handle conversions from infinite strings/floats in models" do
      c = Class.new(Sequel::Model(:test3))
      @db.convert_infinite_timestamps = :float
      c.new(:time=>'infinity').time.should == 'infinity'
      c.new(:time=>'-infinity').time.should == '-infinity'
      c.new(:time=>1.0/0.0).time.should == 1.0/0.0
      c.new(:time=>-1.0/0.0).time.should == -1.0/0.0
    end

    specify "should handle infinite dates if convert_infinite_timestamps is set" do
      @d << {:date=>Sequel.cast('infinity', Date)}
      @db.convert_infinite_timestamps = :nil
      @db[:test3].get(:date).should == nil
      @db.convert_infinite_timestamps = :string
      @db[:test3].get(:date).should == 'infinity'
      @db.convert_infinite_timestamps = :float
      @db[:test3].get(:date).should == 1.0/0.0

      @d.update(:date=>Sequel.cast('-infinity', :timestamp))
      @db.convert_infinite_timestamps = :nil
      @db[:test3].get(:date).should == nil
      @db.convert_infinite_timestamps = :string
      @db[:test3].get(:date).should == '-infinity'
      @db.convert_infinite_timestamps = :float
      @db[:test3].get(:date).should == -1.0/0.0
    end

    specify "should handle conversions from infinite strings/floats in models" do
      c = Class.new(Sequel::Model(:test3))
      @db.convert_infinite_timestamps = :float
      c.new(:date=>'infinity').date.should == 'infinity'
      c.new(:date=>'-infinity').date.should == '-infinity'
      c.new(:date=>1.0/0.0).date.should == 1.0/0.0
      c.new(:date=>-1.0/0.0).date.should == -1.0/0.0
    end
  end

  specify "explain and analyze should not raise errors" do
    @d = POSTGRES_DB[:test3]
    proc{@d.explain}.should_not raise_error
    proc{@d.analyze}.should_not raise_error
  end

  specify "#locks should be a dataset returning database locks " do
    @db.locks.should be_a_kind_of(Sequel::Dataset)
    @db.locks.all.should be_a_kind_of(Array)
  end
end

describe "A PostgreSQL database" do
  before do
    @db = POSTGRES_DB
    @db.create_table! :test2 do
      text :name
      integer :value
    end
  end
  after do
    @db.drop_table?(:test2)
  end

  specify "should support column operations" do
    @db.create_table!(:test2){text :name; integer :value}
    @db[:test2] << {}
    @db[:test2].columns.should == [:name, :value]

    @db.add_column :test2, :xyz, :text, :default => '000'
    @db[:test2].columns.should == [:name, :value, :xyz]
    @db[:test2] << {:name => 'mmm', :value => 111}
    @db[:test2].first[:xyz].should == '000'

    @db[:test2].columns.should == [:name, :value, :xyz]
    @db.drop_column :test2, :xyz

    @db[:test2].columns.should == [:name, :value]

    @db[:test2].delete
    @db.add_column :test2, :xyz, :text, :default => '000'
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 'qqqq'}

    @db[:test2].columns.should == [:name, :value, :xyz]
    @db.rename_column :test2, :xyz, :zyx
    @db[:test2].columns.should == [:name, :value, :zyx]
    @db[:test2].first[:zyx].should == 'qqqq'

    @db.add_column :test2, :xyz, :float
    @db[:test2].delete
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 56.78}
    @db.set_column_type :test2, :xyz, :integer

    @db[:test2].first[:xyz].should == 57
  end
end

describe "A PostgreSQL database" do
  before do
    @db = POSTGRES_DB
    @db.drop_table?(:posts)
    @db.sqls.clear
  end
  after do
    @db.drop_table?(:posts)
  end

  specify "should support resetting the primary key sequence" do
    @db.create_table(:posts){primary_key :a}
    @db[:posts].insert(:a=>20).should == 20
    @db[:posts].insert.should == 1
    @db[:posts].insert.should == 2
    @db[:posts].insert(:a=>10).should == 10
    @db.reset_primary_key_sequence(:posts).should == 21
    @db[:posts].insert.should == 21
    @db[:posts].order(:a).map(:a).should == [1, 2, 10, 20, 21]
  end
    
  specify "should support resetting the primary key sequence with default_schema" do
    begin
      @db.run("DROP SCHEMA p") rescue nil
      @db.run("CREATE SCHEMA p")
      @db.default_schema = :p
      @db.create_table(:posts){primary_key :a}
      @db[:p__posts].insert(:a=>20).should == 20
      @db[:p__posts].insert.should == 1
      @db[:p__posts].insert.should == 2
      @db[:p__posts].insert(:a=>10).should == 10
      @db.reset_primary_key_sequence(:posts).should == 21
      @db[:p__posts].insert.should == 21
      @db[:p__posts].order(:a).map(:a).should == [1, 2, 10, 20, 21]
    ensure
      @db.default_schema = nil
      @db.run("DROP SCHEMA p CASCADE")
    end
  end

  specify "should support specifying Integer/Bignum/Fixnum types in primary keys and have them be auto incrementing" do
    @db.create_table(:posts){primary_key :a, :type=>Integer}
    @db[:posts].insert.should == 1
    @db[:posts].insert.should == 2
    @db.create_table!(:posts){primary_key :a, :type=>Fixnum}
    @db[:posts].insert.should == 1
    @db[:posts].insert.should == 2
    @db.create_table!(:posts){primary_key :a, :type=>Bignum}
    @db[:posts].insert.should == 1
    @db[:posts].insert.should == 2
  end

  specify "should not raise an error if attempting to resetting the primary key sequence for a table without a primary key" do
    @db.create_table(:posts){Integer :a}
    @db.reset_primary_key_sequence(:posts).should == nil
  end

  specify "should support opclass specification" do
    @db.create_table(:posts){text :title; text :body; integer :user_id; index(:user_id, :opclass => :int4_ops, :type => :btree)}
    check_sqls do
      @db.sqls.should == [
      'CREATE TABLE "posts" ("title" text, "body" text, "user_id" integer)',
      'CREATE INDEX "posts_user_id_index" ON "posts" USING btree ("user_id" int4_ops)'
      ]
    end
  end

  specify "should support fulltext indexes and searching" do
    @db.create_table(:posts){text :title; text :body; full_text_index [:title, :body]; full_text_index :title, :language => 'french'}
    check_sqls do
      @db.sqls.should == [
        %{CREATE TABLE "posts" ("title" text, "body" text)},
        %{CREATE INDEX "posts_title_body_index" ON "posts" USING gin (to_tsvector('simple'::regconfig, (COALESCE("title", '') || ' ' || COALESCE("body", ''))))},
        %{CREATE INDEX "posts_title_index" ON "posts" USING gin (to_tsvector('french'::regconfig, (COALESCE("title", ''))))}
      ]
    end

    @db[:posts].insert(:title=>'ruby rails', :body=>'yowsa')
    @db[:posts].insert(:title=>'sequel', :body=>'ruby')
    @db[:posts].insert(:title=>'ruby scooby', :body=>'x')
    @db.sqls.clear

    @db[:posts].full_text_search(:title, 'rails').all.should == [{:title=>'ruby rails', :body=>'yowsa'}]
    @db[:posts].full_text_search([:title, :body], ['yowsa', 'rails']).all.should == [:title=>'ruby rails', :body=>'yowsa']
    @db[:posts].full_text_search(:title, 'scooby', :language => 'french').all.should == [{:title=>'ruby scooby', :body=>'x'}]
    check_sqls do
      @db.sqls.should == [
        %{SELECT * FROM "posts" WHERE (to_tsvector('simple'::regconfig, (COALESCE("title", ''))) @@ to_tsquery('simple'::regconfig, 'rails'))},
        %{SELECT * FROM "posts" WHERE (to_tsvector('simple'::regconfig, (COALESCE("title", '') || ' ' || COALESCE("body", ''))) @@ to_tsquery('simple'::regconfig, 'yowsa | rails'))},
        %{SELECT * FROM "posts" WHERE (to_tsvector('french'::regconfig, (COALESCE("title", ''))) @@ to_tsquery('french'::regconfig, 'scooby'))}]
    end

    @db[:posts].full_text_search(:title, :$n).call(:select, :n=>'rails').should == [{:title=>'ruby rails', :body=>'yowsa'}]
    @db[:posts].full_text_search(:title, :$n).prepare(:select, :fts_select).call(:n=>'rails').should == [{:title=>'ruby rails', :body=>'yowsa'}]
  end

  specify "should support spatial indexes" do
    @db.create_table(:posts){box :geom; spatial_index [:geom]}
    check_sqls do
      @db.sqls.should == [
        'CREATE TABLE "posts" ("geom" box)',
        'CREATE INDEX "posts_geom_index" ON "posts" USING gist ("geom")'
      ]
    end
  end

  specify "should support indexes with index type" do
    @db.create_table(:posts){varchar :title, :size => 5; index :title, :type => 'hash'}
    check_sqls do
      @db.sqls.should == [
        'CREATE TABLE "posts" ("title" varchar(5))',
        'CREATE INDEX "posts_title_index" ON "posts" USING hash ("title")'
      ]
    end
  end

  specify "should support unique indexes with index type" do
    @db.create_table(:posts){varchar :title, :size => 5; index :title, :type => 'btree', :unique => true}
    check_sqls do
      @db.sqls.should == [
        'CREATE TABLE "posts" ("title" varchar(5))',
        'CREATE UNIQUE INDEX "posts_title_index" ON "posts" USING btree ("title")'
      ]
    end
  end

  specify "should support partial indexes" do
    @db.create_table(:posts){varchar :title, :size => 5; index :title, :where => {:title => '5'}}
    check_sqls do
      @db.sqls.should == [
        'CREATE TABLE "posts" ("title" varchar(5))',
        'CREATE INDEX "posts_title_index" ON "posts" ("title") WHERE ("title" = \'5\')'
      ]
    end
  end

  specify "should support identifiers for table names in indicies" do
    @db.create_table(Sequel::SQL::Identifier.new(:posts)){varchar :title, :size => 5; index :title, :where => {:title => '5'}}
    check_sqls do
      @db.sqls.should == [
        'CREATE TABLE "posts" ("title" varchar(5))',
        'CREATE INDEX "posts_title_index" ON "posts" ("title") WHERE ("title" = \'5\')'
      ]
    end
  end

  specify "should support renaming tables" do
    @db.create_table!(:posts1){primary_key :a}
    @db.rename_table(:posts1, :posts)
  end
end

describe "Postgres::Dataset#import" do
  before do
    @db = POSTGRES_DB
    @db.create_table!(:test){primary_key :x; Integer :y}
    @db.sqls.clear
    @ds = @db[:test]
  end
  after do
    @db.drop_table?(:test)
  end


  specify "#import should a single insert statement" do
    @ds.import([:x, :y], [[1, 2], [3, 4]])
    check_sqls do
      @db.sqls.should == ['BEGIN', 'INSERT INTO "test" ("x", "y") VALUES (1, 2), (3, 4)', 'COMMIT']
    end
    @ds.all.should == [{:x=>1, :y=>2}, {:x=>3, :y=>4}]
  end

  specify "#import should work correctly when returning primary keys" do
    @ds.import([:x, :y], [[1, 2], [3, 4]], :return=>:primary_key).should == [1, 3]
    @ds.all.should == [{:x=>1, :y=>2}, {:x=>3, :y=>4}]
  end

  specify "#import should work correctly when returning primary keys with :slice option" do
    @ds.import([:x, :y], [[1, 2], [3, 4]], :return=>:primary_key, :slice=>1).should == [1, 3]
    @ds.all.should == [{:x=>1, :y=>2}, {:x=>3, :y=>4}]
  end

  specify "#import should work correctly with an arbitrary returning value" do
    @ds.returning(:y, :x).import([:x, :y], [[1, 2], [3, 4]]).should == [{:y=>2, :x=>1}, {:y=>4, :x=>3}]
    @ds.all.should == [{:x=>1, :y=>2}, {:x=>3, :y=>4}]
  end
end

describe "Postgres::Dataset#insert" do
  before do
    @db = POSTGRES_DB
    @db.create_table!(:test5){primary_key :xid; Integer :value}
    @db.sqls.clear
    @ds = @db[:test5]
  end
  after do
    @db.drop_table?(:test5)
  end

  specify "should work with static SQL" do
    @ds.with_sql('INSERT INTO test5 (value) VALUES (10)').insert.should == nil
    @db['INSERT INTO test5 (value) VALUES (20)'].insert.should == nil
    @ds.all.should == [{:xid=>1, :value=>10}, {:xid=>2, :value=>20}]
  end

  specify "should insert correctly if using a column array and a value array" do
    @ds.insert([:value], [10]).should == 1
    @ds.all.should == [{:xid=>1, :value=>10}]
  end

  specify "should use INSERT RETURNING" do
    @ds.insert(:value=>10).should == 1
    check_sqls do
      @db.sqls.last.should == 'INSERT INTO "test5" ("value") VALUES (10) RETURNING "xid"'
    end
  end

  specify "should have insert_select insert the record and return the inserted record" do
    h = @ds.insert_select(:value=>10)
    h[:value].should == 10
    @ds.first(:xid=>h[:xid])[:value].should == 10
  end

  specify "should correctly return the inserted record's primary key value" do
    value1 = 10
    id1 = @ds.insert(:value=>value1)
    @ds.first(:xid=>id1)[:value].should == value1
    value2 = 20
    id2 = @ds.insert(:value=>value2)
    @ds.first(:xid=>id2)[:value].should == value2
  end

  specify "should return nil if the table has no primary key" do
    @db.create_table!(:test5){String :name; Integer :value}
    @ds.delete
    @ds.insert(:name=>'a').should == nil
  end
end

describe "Postgres::Database schema qualified tables" do
  before do
    @db = POSTGRES_DB
    @db << "CREATE SCHEMA schema_test"
    @db.instance_variable_set(:@primary_keys, {})
    @db.instance_variable_set(:@primary_key_sequences, {})
  end
  after do
    @db << "DROP SCHEMA schema_test CASCADE"
    @db.default_schema = nil
  end

  specify "should be able to create, drop, select and insert into tables in a given schema" do
    @db.create_table(:schema_test__schema_test){primary_key :i}
    @db[:schema_test__schema_test].first.should == nil
    @db[:schema_test__schema_test].insert(:i=>1).should == 1
    @db[:schema_test__schema_test].first.should == {:i=>1}
    @db.from(Sequel.lit('schema_test.schema_test')).first.should == {:i=>1}
    @db.drop_table(:schema_test__schema_test)
    @db.create_table(Sequel.qualify(:schema_test, :schema_test)){integer :i}
    @db[:schema_test__schema_test].first.should == nil
    @db.from(Sequel.lit('schema_test.schema_test')).first.should == nil
    @db.drop_table(Sequel.qualify(:schema_test, :schema_test))
  end

  specify "#tables should not include tables in a default non-public schema" do
    @db.create_table(:schema_test__schema_test){integer :i}
    @db.tables.should include(:schema_test)
    @db.tables.should_not include(:pg_am)
    @db.tables.should_not include(:domain_udt_usage)
  end

  specify "#tables should return tables in the schema provided by the :schema argument" do
    @db.create_table(:schema_test__schema_test){integer :i}
    @db.tables(:schema=>:schema_test).should == [:schema_test]
  end

  specify "#schema should not include columns from tables in a default non-public schema" do
    @db.create_table(:schema_test__domains){integer :i}
    sch = @db.schema(:schema_test__domains)
    cs = sch.map{|x| x.first}
    cs.should include(:i)
    cs.should_not include(:data_type)
  end

  specify "#schema should only include columns from the table in the given :schema argument" do
    @db.create_table!(:domains){integer :d}
    @db.create_table(:schema_test__domains){integer :i}
    sch = @db.schema(:domains, :schema=>:schema_test)
    cs = sch.map{|x| x.first}
    cs.should include(:i)
    cs.should_not include(:d)
    @db.drop_table(:domains)
  end

  specify "#schema should not include columns in tables from other domains by default" do
    @db.create_table!(:public__domains){integer :d}
    @db.create_table(:schema_test__domains){integer :i}
    begin
      @db.schema(:domains).map{|x| x.first}.should == [:d]
      @db.schema(:schema_test__domains).map{|x| x.first}.should == [:i]
    ensure
      @db.drop_table?(:public__domains)
    end
  end

  specify "#table_exists? should see if the table is in a given schema" do
    @db.create_table(:schema_test__schema_test){integer :i}
    @db.table_exists?(:schema_test__schema_test).should == true
  end

  specify "should be able to get primary keys for tables in a given schema" do
    @db.create_table(:schema_test__schema_test){primary_key :i}
    @db.primary_key(:schema_test__schema_test).should == 'i'
  end

  specify "should be able to get serial sequences for tables in a given schema" do
    @db.create_table(:schema_test__schema_test){primary_key :i}
    @db.primary_key_sequence(:schema_test__schema_test).should == '"schema_test"."schema_test_i_seq"'
  end

  specify "should be able to get serial sequences for tables that have spaces in the name in a given schema" do
    @db.create_table(:"schema_test__schema test"){primary_key :i}
    @db.primary_key_sequence(:"schema_test__schema test").should == '"schema_test"."schema test_i_seq"'
  end

  specify "should be able to get custom sequences for tables in a given schema" do
    @db << "CREATE SEQUENCE schema_test.kseq"
    @db.create_table(:schema_test__schema_test){integer :j; primary_key :k, :type=>:integer, :default=>Sequel.lit("nextval('schema_test.kseq'::regclass)")}
    @db.primary_key_sequence(:schema_test__schema_test).should == '"schema_test".kseq'
  end

  specify "should be able to get custom sequences for tables that have spaces in the name in a given schema" do
    @db << "CREATE SEQUENCE schema_test.\"ks eq\""
    @db.create_table(:"schema_test__schema test"){integer :j; primary_key :k, :type=>:integer, :default=>Sequel.lit("nextval('schema_test.\"ks eq\"'::regclass)")}
    @db.primary_key_sequence(:"schema_test__schema test").should == '"schema_test"."ks eq"'
  end

  specify "#default_schema= should change the default schema used from public" do
    @db.create_table(:schema_test__schema_test){primary_key :i}
    @db.default_schema = :schema_test
    @db.table_exists?(:schema_test).should == true
    @db.tables.should == [:schema_test]
    @db.primary_key(:schema_test__schema_test).should == 'i'
    @db.primary_key_sequence(:schema_test__schema_test).should == '"schema_test"."schema_test_i_seq"'
  end

  specify "should handle schema introspection cases with tables with same name in multiple schemas" do
    begin
      @db.create_table(:schema_test__schema_test) do
        primary_key :id
        foreign_key :i, :schema_test__schema_test, :index=>{:name=>:schema_test_sti}
      end
      @db.create_table!(:public__schema_test) do
        primary_key :id
        foreign_key :j, :public__schema_test, :index=>{:name=>:public_test_sti}
      end

      h = @db.schema(:schema_test)
      h.length.should == 2
      h.last.first.should == :j

      @db.indexes(:schema_test).should == {:public_test_sti=>{:unique=>false, :columns=>[:j], :deferrable=>nil}}
      @db.foreign_key_list(:schema_test).should == [{:on_update=>:no_action, :columns=>[:j], :deferrable=>false, :key=>[:id], :table=>:schema_test, :on_delete=>:no_action, :name=>:schema_test_j_fkey}]
    ensure
      @db.drop_table?(:public__schema_test)
    end
  end
end

describe "Postgres::Database schema qualified tables and eager graphing" do
  before(:all) do
    @db = POSTGRES_DB
    @db.run "DROP SCHEMA s CASCADE" rescue nil
    @db.run "CREATE SCHEMA s"

    @db.create_table(:s__bands){primary_key :id; String :name}
    @db.create_table(:s__albums){primary_key :id; String :name; foreign_key :band_id, :s__bands}
    @db.create_table(:s__tracks){primary_key :id; String :name; foreign_key :album_id, :s__albums}
    @db.create_table(:s__members){primary_key :id; String :name; foreign_key :band_id, :s__bands}

    @Band = Class.new(Sequel::Model(:s__bands))
    @Album = Class.new(Sequel::Model(:s__albums))
    @Track = Class.new(Sequel::Model(:s__tracks))
    @Member = Class.new(Sequel::Model(:s__members))
    def @Band.name; :Band; end
    def @Album.name; :Album; end
    def @Track.name; :Track; end
    def @Member.name; :Member; end

    @Band.one_to_many :albums, :class=>@Album, :order=>:name
    @Band.one_to_many :members, :class=>@Member, :order=>:name
    @Album.many_to_one :band, :class=>@Band, :order=>:name
    @Album.one_to_many :tracks, :class=>@Track, :order=>:name
    @Track.many_to_one :album, :class=>@Album, :order=>:name
    @Member.many_to_one :band, :class=>@Band, :order=>:name

    @Member.many_to_many :members, :class=>@Member, :join_table=>:s__bands, :right_key=>:id, :left_key=>:id, :left_primary_key=>:band_id, :right_primary_key=>:band_id, :order=>:name
    @Band.many_to_many :tracks, :class=>@Track, :join_table=>:s__albums, :right_key=>:id, :right_primary_key=>:album_id, :order=>:name

    @b1 = @Band.create(:name=>"BM")
    @b2 = @Band.create(:name=>"J")
    @a1 = @Album.create(:name=>"BM1", :band=>@b1)
    @a2 = @Album.create(:name=>"BM2", :band=>@b1)
    @a3 = @Album.create(:name=>"GH", :band=>@b2)
    @a4 = @Album.create(:name=>"GHL", :band=>@b2)
    @t1 = @Track.create(:name=>"BM1-1", :album=>@a1)
    @t2 = @Track.create(:name=>"BM1-2", :album=>@a1)
    @t3 = @Track.create(:name=>"BM2-1", :album=>@a2)
    @t4 = @Track.create(:name=>"BM2-2", :album=>@a2)
    @m1 = @Member.create(:name=>"NU", :band=>@b1)
    @m2 = @Member.create(:name=>"TS", :band=>@b1)
    @m3 = @Member.create(:name=>"NS", :band=>@b2)
    @m4 = @Member.create(:name=>"JC", :band=>@b2)
  end
  after(:all) do
    @db.run "DROP SCHEMA s CASCADE"
  end

  specify "should return all eager graphs correctly" do
    bands = @Band.order(:bands__name).eager_graph(:albums).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]

    bands = @Band.order(:bands__name).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.albums.map{|y| y.tracks}}.should == [[[@t1, @t2], [@t3, @t4]], [[], []]]

    bands = @Band.order(:bands__name).eager_graph({:albums=>:tracks}, :members).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.albums.map{|y| y.tracks}}.should == [[[@t1, @t2], [@t3, @t4]], [[], []]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]
  end

  specify "should have eager graphs work with previous joins" do
    bands = @Band.order(:bands__name).select_all(:s__bands).join(:s__members, :band_id=>:id).from_self(:alias=>:bands0).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.albums.map{|y| y.tracks}}.should == [[[@t1, @t2], [@t3, @t4]], [[], []]]
  end

  specify "should have eager graphs work with joins with the same tables" do
    bands = @Band.order(:bands__name).select_all(:s__bands).join(:s__members, :band_id=>:id).eager_graph({:albums=>:tracks}, :members).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.albums.map{|y| y.tracks}}.should == [[[@t1, @t2], [@t3, @t4]], [[], []]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]
  end

  specify "should have eager graphs work with self referential associations" do
    bands = @Band.order(:bands__name).eager_graph(:tracks=>{:album=>:band}).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]
    bands.map{|x| x.tracks.map{|y| y.album}}.should == [[@a1, @a1, @a2, @a2], []]
    bands.map{|x| x.tracks.map{|y| y.album.band}}.should == [[@b1, @b1, @b1, @b1], []]

    members = @Member.order(:members__name).eager_graph(:members).all
    members.should == [@m4, @m3, @m1, @m2]
    members.map{|x| x.members}.should == [[@m4, @m3], [@m4, @m3], [@m1, @m2], [@m1, @m2]]

    members = @Member.order(:members__name).eager_graph(:band, :members=>:band).all
    members.should == [@m4, @m3, @m1, @m2]
    members.map{|x| x.band}.should == [@b2, @b2, @b1, @b1]
    members.map{|x| x.members}.should == [[@m4, @m3], [@m4, @m3], [@m1, @m2], [@m1, @m2]]
    members.map{|x| x.members.map{|y| y.band}}.should == [[@b2, @b2], [@b2, @b2], [@b1, @b1], [@b1, @b1]]
  end

  specify "should have eager graphs work with a from_self dataset" do
    bands = @Band.order(:bands__name).from_self.eager_graph(:tracks=>{:album=>:band}).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]
    bands.map{|x| x.tracks.map{|y| y.album}}.should == [[@a1, @a1, @a2, @a2], []]
    bands.map{|x| x.tracks.map{|y| y.album.band}}.should == [[@b1, @b1, @b1, @b1], []]
  end

  specify "should have eager graphs work with different types of aliased from tables" do
    bands = @Band.order(:tracks__name).from(:s__bands___tracks).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:tracks__name).from(Sequel.expr(:s__bands).as(:tracks)).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:tracks__name).from(Sequel.expr(:s__bands).as(Sequel.identifier(:tracks))).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:tracks__name).from(Sequel.expr(:s__bands).as('tracks')).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]
  end

  specify "should have eager graphs work with join tables with aliases" do
    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums___tracks, :band_id=>Sequel.qualify(:s__bands, :id)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(Sequel.as(:s__albums, :tracks), :band_id=>Sequel.qualify(:s__bands, :id)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(Sequel.as(:s__albums, 'tracks'), :band_id=>Sequel.qualify(:s__bands, :id)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(Sequel.as(:s__albums, Sequel.identifier(:tracks)), :band_id=>Sequel.qualify(:s__bands, :id)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums, {:band_id=>Sequel.qualify(:s__bands, :id)}, :table_alias=>:tracks).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums, {:band_id=>Sequel.qualify(:s__bands, :id)}, :table_alias=>'tracks').eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums, {:band_id=>Sequel.qualify(:s__bands, :id)}, :table_alias=>Sequel.identifier(:tracks)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]
  end

  specify "should have eager graphs work with different types of qualified from tables" do
    bands = @Band.order(:bands__name).from(Sequel.qualify(:s, :bands)).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:bands__name).from(Sequel.identifier(:bands).qualify(:s)).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:bands__name).from(Sequel::SQL::QualifiedIdentifier.new(:s, 'bands')).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]
  end

end

if POSTGRES_DB.server_version >= 80300
  describe "PostgreSQL tsearch2" do
    before(:all) do
      POSTGRES_DB.create_table! :test6 do
        text :title
        text :body
        full_text_index [:title, :body]
      end
      @ds = POSTGRES_DB[:test6]
    end
    after do
      POSTGRES_DB[:test6].delete
    end
    after(:all) do
      POSTGRES_DB.drop_table?(:test6)
    end

    specify "should search by indexed column" do
      record =  {:title => "oopsla conference", :body => "test"}
      @ds << record
      @ds.full_text_search(:title, "oopsla").all.should include(record)
    end

    specify "should join multiple coumns with spaces to search by last words in row" do
      record = {:title => "multiple words", :body => "are easy to search"}
      @ds << record
      @ds.full_text_search([:title, :body], "words").all.should include(record)
    end

    specify "should return rows with a NULL in one column if a match in another column" do
      record = {:title => "multiple words", :body =>nil}
      @ds << record
      @ds.full_text_search([:title, :body], "words").all.should include(record)
    end
  end
end

if POSTGRES_DB.dataset.supports_window_functions?
  describe "Postgres::Dataset named windows" do
    before do
      @db = POSTGRES_DB
      @db.create_table!(:i1){Integer :id; Integer :group_id; Integer :amount}
      @ds = @db[:i1].order(:id)
      @ds.insert(:id=>1, :group_id=>1, :amount=>1)
      @ds.insert(:id=>2, :group_id=>1, :amount=>10)
      @ds.insert(:id=>3, :group_id=>1, :amount=>100)
      @ds.insert(:id=>4, :group_id=>2, :amount=>1000)
      @ds.insert(:id=>5, :group_id=>2, :amount=>10000)
      @ds.insert(:id=>6, :group_id=>2, :amount=>100000)
    end
    after do
      @db.drop_table?(:i1)
    end

    specify "should give correct results for window functions" do
      @ds.window(:win, :partition=>:group_id, :order=>:id).select(:id){sum(:over, :args=>amount, :window=>win){}}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1000, :id=>4}, {:sum=>11000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.window(:win, :partition=>:group_id).select(:id){sum(:over, :args=>amount, :window=>win, :order=>id){}}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1000, :id=>4}, {:sum=>11000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.window(:win, {}).select(:id){sum(:over, :args=>amount, :window=>:win, :order=>id){}}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1111, :id=>4}, {:sum=>11111, :id=>5}, {:sum=>111111, :id=>6}]
      @ds.window(:win, :partition=>:group_id).select(:id){sum(:over, :args=>amount, :window=>:win, :order=>id, :frame=>:all){}}.all.should ==
        [{:sum=>111, :id=>1}, {:sum=>111, :id=>2}, {:sum=>111, :id=>3}, {:sum=>111000, :id=>4}, {:sum=>111000, :id=>5}, {:sum=>111000, :id=>6}]
    end
  end
end

describe "Postgres::Database functions, languages, schemas, and triggers" do
  before do
    @d = POSTGRES_DB
  end
  after do
    @d.drop_function('tf', :if_exists=>true, :cascade=>true)
    @d.drop_function('tf', :if_exists=>true, :cascade=>true, :args=>%w'integer integer')
    @d.drop_language(:plpgsql, :if_exists=>true, :cascade=>true) if @d.server_version < 90000
    @d.drop_schema(:sequel, :if_exists=>true, :cascade=>true)
    @d.drop_table?(:test)
  end

  specify "#create_function and #drop_function should create and drop functions" do
    proc{@d['SELECT tf()'].all}.should raise_error(Sequel::DatabaseError)
    args = ['tf', 'SELECT 1', {:returns=>:integer}]
    @d.send(:create_function_sql, *args).should =~ /\A\s*CREATE FUNCTION tf\(\)\s+RETURNS integer\s+LANGUAGE SQL\s+AS 'SELECT 1'\s*\z/
    @d.create_function(*args)
    @d['SELECT tf()'].all.should == [{:tf=>1}]
    @d.send(:drop_function_sql, 'tf').should == 'DROP FUNCTION tf()'
    @d.drop_function('tf')
    proc{@d['SELECT tf()'].all}.should raise_error(Sequel::DatabaseError)
  end

  specify "#create_function and #drop_function should support options" do
    args = ['tf', 'SELECT $1 + $2', {:args=>[[:integer, :a], :integer], :replace=>true, :returns=>:integer, :language=>'SQL', :behavior=>:immutable, :strict=>true, :security_definer=>true, :cost=>2, :set=>{:search_path => 'public'}}]
    @d.send(:create_function_sql,*args).should =~ /\A\s*CREATE OR REPLACE FUNCTION tf\(a integer, integer\)\s+RETURNS integer\s+LANGUAGE SQL\s+IMMUTABLE\s+STRICT\s+SECURITY DEFINER\s+COST 2\s+SET search_path = public\s+AS 'SELECT \$1 \+ \$2'\s*\z/
    @d.create_function(*args)
    # Make sure replace works
    @d.create_function(*args)
    @d['SELECT tf(1, 2)'].all.should == [{:tf=>3}]
    args = ['tf', {:if_exists=>true, :cascade=>true, :args=>[[:integer, :a], :integer]}]
    @d.send(:drop_function_sql,*args).should == 'DROP FUNCTION IF EXISTS tf(a integer, integer) CASCADE'
    @d.drop_function(*args)
    # Make sure if exists works
    @d.drop_function(*args)
  end

  specify "#create_language and #drop_language should create and drop languages" do
    @d.send(:create_language_sql, :plpgsql).should == 'CREATE LANGUAGE plpgsql'
    @d.create_language(:plpgsql, :replace=>true) if @d.server_version < 90000
    proc{@d.create_language(:plpgsql)}.should raise_error(Sequel::DatabaseError)
    @d.send(:drop_language_sql, :plpgsql).should == 'DROP LANGUAGE plpgsql'
    @d.drop_language(:plpgsql) if @d.server_version < 90000
    proc{@d.drop_language(:plpgsql)}.should raise_error(Sequel::DatabaseError) if @d.server_version < 90000
    @d.send(:create_language_sql, :plpgsql, :replace=>true, :trusted=>true, :handler=>:a, :validator=>:b).should == (@d.server_version >= 90000 ? 'CREATE OR REPLACE TRUSTED LANGUAGE plpgsql HANDLER a VALIDATOR b' : 'CREATE TRUSTED LANGUAGE plpgsql HANDLER a VALIDATOR b')
    @d.send(:drop_language_sql, :plpgsql, :if_exists=>true, :cascade=>true).should == 'DROP LANGUAGE IF EXISTS plpgsql CASCADE'
    # Make sure if exists works
    @d.drop_language(:plpgsql, :if_exists=>true, :cascade=>true) if @d.server_version < 90000
  end

  specify "#create_schema and #drop_schema should create and drop schemas" do
    @d.send(:create_schema_sql, :sequel).should == 'CREATE SCHEMA "sequel"'
    @d.send(:drop_schema_sql, :sequel).should == 'DROP SCHEMA "sequel"'
    @d.send(:drop_schema_sql, :sequel, :if_exists=>true, :cascade=>true).should == 'DROP SCHEMA IF EXISTS "sequel" CASCADE'
    @d.create_schema(:sequel)
    @d.create_table(:sequel__test){Integer :a}
    @d.drop_schema(:sequel, :if_exists=>true, :cascade=>true)
  end

  specify "#create_trigger and #drop_trigger should create and drop triggers" do
    @d.create_language(:plpgsql) if @d.server_version < 90000
    @d.create_function(:tf, 'BEGIN IF NEW.value IS NULL THEN RAISE EXCEPTION \'Blah\'; END IF; RETURN NEW; END;', :language=>:plpgsql, :returns=>:trigger)
    @d.send(:create_trigger_sql, :test, :identity, :tf, :each_row=>true).should == 'CREATE TRIGGER identity BEFORE INSERT OR UPDATE OR DELETE ON "test" FOR EACH ROW EXECUTE PROCEDURE tf()'
    @d.create_table(:test){String :name; Integer :value}
    @d.create_trigger(:test, :identity, :tf, :each_row=>true)
    @d[:test].insert(:name=>'a', :value=>1)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>1}]
    proc{@d[:test].filter(:name=>'a').update(:value=>nil)}.should raise_error(Sequel::DatabaseError)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>1}]
    @d[:test].filter(:name=>'a').update(:value=>3)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>3}]
    @d.send(:drop_trigger_sql, :test, :identity).should == 'DROP TRIGGER identity ON "test"'
    @d.drop_trigger(:test, :identity)
    @d.send(:create_trigger_sql, :test, :identity, :tf, :after=>true, :events=>:insert, :args=>[1, 'a']).should == 'CREATE TRIGGER identity AFTER INSERT ON "test" EXECUTE PROCEDURE tf(1, \'a\')'
    @d.send(:drop_trigger_sql, :test, :identity, :if_exists=>true, :cascade=>true).should == 'DROP TRIGGER IF EXISTS identity ON "test" CASCADE'
    # Make sure if exists works
    @d.drop_trigger(:test, :identity, :if_exists=>true, :cascade=>true)
  end
end

if POSTGRES_DB.adapter_scheme == :postgres
  describe "Postgres::Dataset #use_cursor" do
    before(:all) do
      @db = POSTGRES_DB
      @db.create_table!(:test_cursor){Integer :x}
      @db.sqls.clear
      @ds = @db[:test_cursor]
      @db.transaction{1001.times{|i| @ds.insert(i)}}
    end
    after(:all) do
      @db.drop_table?(:test_cursor)
    end

    specify "should return the same results as the non-cursor use" do
      @ds.all.should == @ds.use_cursor.all
    end

    specify "should respect the :rows_per_fetch option" do
      @db.sqls.clear
      @ds.use_cursor.all
      check_sqls do
        @db.sqls.length.should == 6
        @db.sqls.clear
      end
      @ds.use_cursor(:rows_per_fetch=>100).all
      check_sqls do
        @db.sqls.length.should == 15
      end
    end

    specify "should handle returning inside block" do
      def @ds.check_return
        use_cursor.each{|r| return}
      end
      @ds.check_return
      @ds.all.should == @ds.use_cursor.all
    end
  end

  describe "Postgres::PG_NAMED_TYPES" do
    before do
      @db = POSTGRES_DB
      Sequel::Postgres::PG_NAMED_TYPES[:interval] = lambda{|v| v.reverse}
      @db.reset_conversion_procs
    end
    after do
      Sequel::Postgres::PG_NAMED_TYPES.delete(:interval)
      @db.reset_conversion_procs
      @db.drop_table?(:foo)
    end

    specify "should look up conversion procs by name" do
      @db.create_table!(:foo){interval :bar}
      @db[:foo].insert(Sequel.cast('21 days', :interval))
      @db[:foo].get(:bar).should == 'syad 12'
    end
  end
end

if ((POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG) || POSTGRES_DB.adapter_scheme == :jdbc) && POSTGRES_DB.server_version >= 90000
  describe "Postgres::Database#copy_into" do
    before(:all) do
      @db = POSTGRES_DB
      @db.create_table!(:test_copy){Integer :x; Integer :y}
      @ds = @db[:test_copy].order(:x, :y)
    end
    before do
      @db[:test_copy].delete
    end
    after(:all) do
      @db.drop_table?(:test_copy)
    end

    specify "should work with a :data option containing data in PostgreSQL text format" do
      @db.copy_into(:test_copy, :data=>"1\t2\n3\t4\n")
      @ds.select_map([:x, :y]).should == [[1, 2], [3, 4]]
    end

    specify "should work with :format=>:csv option and :data option containing data in CSV format" do
      @db.copy_into(:test_copy, :format=>:csv, :data=>"1,2\n3,4\n")
      @ds.select_map([:x, :y]).should == [[1, 2], [3, 4]]
    end

    specify "should respect given :options" do
      @db.copy_into(:test_copy, :options=>"FORMAT csv, HEADER TRUE", :data=>"x,y\n1,2\n3,4\n")
      @ds.select_map([:x, :y]).should == [[1, 2], [3, 4]]
    end

    specify "should respect given :options options when :format is used" do
      @db.copy_into(:test_copy, :options=>"QUOTE '''', DELIMITER '|'", :format=>:csv, :data=>"'1'|'2'\n'3'|'4'\n")
      @ds.select_map([:x, :y]).should == [[1, 2], [3, 4]]
    end

    specify "should accept :columns option to online copy the given columns" do
      @db.copy_into(:test_copy, :data=>"1\t2\n3\t4\n", :columns=>[:y, :x])
      @ds.select_map([:x, :y]).should == [[2, 1], [4, 3]]
    end

    specify "should accept a block and use returned values for the copy in data stream" do
      buf = ["1\t2\n", "3\t4\n"]
      @db.copy_into(:test_copy){buf.shift}
      @ds.select_map([:x, :y]).should == [[1, 2], [3, 4]]
    end

    specify "should work correctly with a block and :format=>:csv" do
      buf = ["1,2\n", "3,4\n"]
      @db.copy_into(:test_copy, :format=>:csv){buf.shift}
      @ds.select_map([:x, :y]).should == [[1, 2], [3, 4]]
    end

    specify "should accept an enumerable as the :data option" do
      @db.copy_into(:test_copy, :data=>["1\t2\n", "3\t4\n"])
      @ds.select_map([:x, :y]).should == [[1, 2], [3, 4]]
    end

    specify "should have an exception, cause a rollback of copied data and still have a usable connection" do
      2.times do
        sent = false
        proc{@db.copy_into(:test_copy){raise ArgumentError if sent; sent = true; "1\t2\n"}}.should raise_error(ArgumentError)
        @ds.select_map([:x, :y]).should == []
      end
    end

    specify "should handle database errors with a rollback of copied data and still have a usable connection" do
      2.times do
        proc{@db.copy_into(:test_copy, :data=>["1\t2\n", "3\ta\n"])}.should raise_error(Sequel::DatabaseError)
        @ds.select_map([:x, :y]).should == []
      end
    end

    specify "should raise an Error if both :data and a block are provided" do
      proc{@db.copy_into(:test_copy, :data=>["1\t2\n", "3\t4\n"]){}}.should raise_error(Sequel::Error)
    end

    specify "should raise an Error if neither :data or a block are provided" do
      proc{@db.copy_into(:test_copy)}.should raise_error(Sequel::Error)
    end
  end

  describe "Postgres::Database#copy_table" do
    before(:all) do
      @db = POSTGRES_DB
      @db.create_table!(:test_copy){Integer :x; Integer :y}
      ds = @db[:test_copy]
      ds.insert(1, 2)
      ds.insert(3, 4)
    end
    after(:all) do
      @db.drop_table?(:test_copy)
    end

    specify "without a block or options should return a text version of the table as a single string" do
      @db.copy_table(:test_copy).should == "1\t2\n3\t4\n"
    end

    specify "without a block and with :format=>:csv should return a csv version of the table as a single string" do
      @db.copy_table(:test_copy, :format=>:csv).should == "1,2\n3,4\n"
    end

    specify "should treat string as SQL code" do
      @db.copy_table('COPY "test_copy" TO STDOUT').should == "1\t2\n3\t4\n"
    end

    specify "should respect given :options options" do
      @db.copy_table(:test_copy, :options=>"FORMAT csv, HEADER TRUE").should == "x,y\n1,2\n3,4\n"
    end

    specify "should respect given :options options when :format is used" do
      @db.copy_table(:test_copy, :format=>:csv, :options=>"QUOTE '''', FORCE_QUOTE *").should == "'1','2'\n'3','4'\n"
    end

    specify "should accept dataset as first argument" do
      @db.copy_table(@db[:test_copy].cross_join(:test_copy___tc).order(:test_copy__x, :test_copy__y, :tc__x, :tc__y)).should == "1\t2\t1\t2\n1\t2\t3\t4\n3\t4\t1\t2\n3\t4\t3\t4\n"
    end

    specify "with a block and no options should yield each row as a string in text format" do
      buf = []
      @db.copy_table(:test_copy){|b| buf << b}
      buf.should == ["1\t2\n", "3\t4\n"]
    end

    specify "with a block and :format=>:csv should yield each row as a string in csv format" do
      buf = []
      @db.copy_table(:test_copy, :format=>:csv){|b| buf << b}
      buf.should == ["1,2\n", "3,4\n"]
    end

    specify "should work fine when using a block that is terminated early with a following copy_table" do
      buf = []
      proc{@db.copy_table(:test_copy, :format=>:csv){|b| buf << b; break}}.should raise_error(Sequel::DatabaseDisconnectError)
      buf.should == ["1,2\n"]
      buf.clear
      proc{@db.copy_table(:test_copy, :format=>:csv){|b| buf << b; raise ArgumentError}}.should raise_error(Sequel::DatabaseDisconnectError)
      buf.should == ["1,2\n"]
      buf.clear
      @db.copy_table(:test_copy){|b| buf << b}
      buf.should == ["1\t2\n", "3\t4\n"]
    end

    specify "should work fine when using a block that is terminated early with a following regular query" do
      buf = []
      proc{@db.copy_table(:test_copy, :format=>:csv){|b| buf << b; break}}.should raise_error(Sequel::DatabaseDisconnectError)
      buf.should == ["1,2\n"]
      buf.clear
      proc{@db.copy_table(:test_copy, :format=>:csv){|b| buf << b; raise ArgumentError}}.should raise_error(Sequel::DatabaseDisconnectError)
      buf.should == ["1,2\n"]
      @db[:test_copy].select_order_map(:x).should == [1, 3]
    end
  end
end

if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG && POSTGRES_DB.server_version >= 90000
  describe "Postgres::Database LISTEN/NOTIFY" do
    before(:all) do
      @db = POSTGRES_DB
    end

    specify "should support listen and notify" do
      notify_pid = @db.synchronize{|conn| conn.backend_pid}

      called = false
      @db.listen('foo', :after_listen=>proc{@db.notify('foo')}) do |ev, pid, payload|
        ev.should == 'foo'
        pid.should == notify_pid
        ['', nil].should include(payload)
        called = true
      end.should == 'foo'
      called.should be_true

      # Check weird identifier names
      called = false
      @db.listen('FOO bar', :after_listen=>proc{@db.notify('FOO bar')}) do |ev, pid, payload|
        ev.should == 'FOO bar'
        pid.should == notify_pid
        ['', nil].should include(payload)
        called = true
      end.should == 'FOO bar'
      called.should be_true

      # Check identifier symbols
      called = false
      @db.listen(:foo, :after_listen=>proc{@db.notify(:foo)}) do |ev, pid, payload|
        ev.should == 'foo'
        pid.should == notify_pid
        ['', nil].should include(payload)
        called = true
      end.should == 'foo'
      called.should be_true

      called = false
      @db.listen('foo', :after_listen=>proc{@db.notify('foo', :payload=>'bar')}) do |ev, pid, payload|
        ev.should == 'foo'
        pid.should == notify_pid
        payload.should == 'bar'
        called = true
      end.should == 'foo'
      called.should be_true

      @db.listen('foo', :after_listen=>proc{@db.notify('foo')}).should == 'foo'

      called = false
      called2 = false
      i = 0
      @db.listen(['foo', 'bar'], :after_listen=>proc{@db.notify('foo', :payload=>'bar'); @db.notify('bar', :payload=>'foo')}, :loop=>proc{i+=1}) do |ev, pid, payload|
        if !called
          ev.should == 'foo'
          pid.should == notify_pid
          payload.should == 'bar'
          called = true
        else
          ev.should == 'bar'
          pid.should == notify_pid
          payload.should == 'foo'
          called2 = true
          break
        end
      end.should be_nil
      called.should be_true
      called2.should be_true
      i.should == 1
    end

    specify "should accept a :timeout option in listen" do
      @db.listen('foo2', :timeout=>0.001).should == nil
      called = false
      @db.listen('foo2', :timeout=>0.001){|ev, pid, payload| called = true}.should == nil
      called.should be_false
      i = 0
      @db.listen('foo2', :timeout=>0.001, :loop=>proc{i+=1; throw :stop if i > 3}){|ev, pid, payload| called = true}.should == nil
      i.should == 4
    end unless RUBY_PLATFORM =~ /mingw/ # Ruby freezes on this spec on this platform/version
  end
end

describe 'PostgreSQL special float handling' do
  before do
    @db = POSTGRES_DB
    @db.create_table!(:test5){Float :value}
    @db.sqls.clear
    @ds = @db[:test5]
  end
  after do
    @db.drop_table?(:test5)
  end

  check_sqls do
    specify 'should quote NaN' do
      nan = 0.0/0.0
      @ds.insert_sql(:value => nan).should == %q{INSERT INTO "test5" ("value") VALUES ('NaN')}
    end

    specify 'should quote +Infinity' do
      inf = 1.0/0.0
      @ds.insert_sql(:value => inf).should == %q{INSERT INTO "test5" ("value") VALUES ('Infinity')}
    end

    specify 'should quote -Infinity' do
      inf = -1.0/0.0
      @ds.insert_sql(:value => inf).should == %q{INSERT INTO "test5" ("value") VALUES ('-Infinity')}
    end
  end

  if POSTGRES_DB.adapter_scheme == :postgres
    specify 'inserts NaN' do
      nan = 0.0/0.0
      @ds.insert(:value=>nan)
      @ds.all[0][:value].nan?.should be_true
    end

    specify 'inserts +Infinity' do
      inf = 1.0/0.0
      @ds.insert(:value=>inf)
      @ds.all[0][:value].infinite?.should > 0
    end

    specify 'inserts -Infinity' do
      inf = -1.0/0.0
      @ds.insert(:value=>inf)
      @ds.all[0][:value].infinite?.should < 0
    end
  end
end

describe 'PostgreSQL array handling' do
  before(:all) do
    @db = POSTGRES_DB
    @db.extension :pg_array
    @ds = @db[:items]
    @native = POSTGRES_DB.adapter_scheme == :postgres
    @jdbc = POSTGRES_DB.adapter_scheme == :jdbc
    @tp = lambda{@db.schema(:items).map{|a| a.last[:type]}}
  end
  after do
    @db.drop_table?(:items)
  end

  specify 'insert and retrieve integer and float arrays of various sizes' do
    @db.create_table!(:items) do
      column :i2, 'int2[]'
      column :i4, 'int4[]'
      column :i8, 'int8[]'
      column :r, 'real[]'
      column :dp, 'double precision[]'
    end
    @tp.call.should == [:integer_array, :integer_array, :bigint_array, :float_array, :float_array]
    @ds.insert(Sequel.pg_array([1], :int2), Sequel.pg_array([nil, 2], :int4), Sequel.pg_array([3, nil], :int8), Sequel.pg_array([4, nil, 4.5], :real), Sequel.pg_array([5, nil, 5.5], "double precision"))
    @ds.count.should == 1
    rs = @ds.all
    if @jdbc || @native
      rs.should == [{:i2=>[1], :i4=>[nil, 2], :i8=>[3, nil], :r=>[4.0, nil, 4.5], :dp=>[5.0, nil, 5.5]}]
    end
    if @native
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end

    @ds.delete
    @ds.insert(Sequel.pg_array([[1], [2]], :int2), Sequel.pg_array([[nil, 2], [3, 4]], :int4), Sequel.pg_array([[3, nil], [nil, nil]], :int8), Sequel.pg_array([[4, nil], [nil, 4.5]], :real), Sequel.pg_array([[5, nil], [nil, 5.5]], "double precision"))

    rs = @ds.all
    if @jdbc || @native
      rs.should == [{:i2=>[[1], [2]], :i4=>[[nil, 2], [3, 4]], :i8=>[[3, nil], [nil, nil]], :r=>[[4, nil], [nil, 4.5]], :dp=>[[5, nil], [nil, 5.5]]}]
    end
    if @native
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'insert and retrieve decimal arrays' do
    @db.create_table!(:items) do
      column :n, 'numeric[]'
    end
    @tp.call.should == [:decimal_array]
    @ds.insert(Sequel.pg_array([BigDecimal.new('1.000000000000000000001'), nil, BigDecimal.new('1')], :numeric))
    @ds.count.should == 1
    rs = @ds.all
    if @jdbc || @native
      rs.should == [{:n=>[BigDecimal.new('1.000000000000000000001'), nil, BigDecimal.new('1')]}]
    end
    if @native
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end

    @ds.delete
    @ds.insert(Sequel.pg_array([[BigDecimal.new('1.0000000000000000000000000000001'), nil], [nil, BigDecimal.new('1')]], :numeric))
    rs = @ds.all
    if @jdbc || @native
      rs.should == [{:n=>[[BigDecimal.new('1.0000000000000000000000000000001'), nil], [nil, BigDecimal.new('1')]]}]
    end
    if @native
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'insert and retrieve string arrays' do
    @db.create_table!(:items) do
      column :c, 'char(4)[]'
      column :vc, 'varchar[]'
      column :t, 'text[]'
    end
    @tp.call.should == [:string_array, :string_array, :string_array]
    @ds.insert(Sequel.pg_array(['a', nil, 'NULL', 'b"\'c'], 'char(4)'), Sequel.pg_array(['a', nil, 'NULL', 'b"\'c'], :varchar), Sequel.pg_array(['a', nil, 'NULL', 'b"\'c'], :text))
    @ds.count.should == 1
    rs = @ds.all
    if @jdbc || @native
      rs.should == [{:c=>['a   ', nil, 'NULL', 'b"\'c'], :vc=>['a', nil, 'NULL', 'b"\'c'], :t=>['a', nil, 'NULL', 'b"\'c']}]
    end
    if @native
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end

    @ds.delete
    @ds.insert(Sequel.pg_array([[['a'], [nil]], [['NULL'], ['b"\'c']]], 'char(4)'), Sequel.pg_array([[['a'], ['']], [['NULL'], ['b"\'c']]], :varchar), Sequel.pg_array([[['a'], [nil]], [['NULL'], ['b"\'c']]], :text))
    rs = @ds.all
    if @jdbc || @native
      rs.should == [{:c=>[[['a   '], [nil]], [['NULL'], ['b"\'c']]], :vc=>[[['a'], ['']], [['NULL'], ['b"\'c']]], :t=>[[['a'], [nil]], [['NULL'], ['b"\'c']]]}]
    end
    if @native
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'insert and retrieve arrays of other types' do
    @db.create_table!(:items) do
      column :b, 'bool[]'
      column :d, 'date[]'
      column :t, 'time[]'
      column :ts, 'timestamp[]'
      column :tstz, 'timestamptz[]'
    end
    @tp.call.should == [:boolean_array, :date_array, :time_array, :datetime_array, :datetime_timezone_array]

    d = Date.today
    t = Sequel::SQLTime.create(10, 20, 30)
    ts = Time.local(2011, 1, 2, 3, 4, 5)

    @ds.insert(Sequel.pg_array([true, false], :bool), Sequel.pg_array([d, nil], :date), Sequel.pg_array([t, nil], :time), Sequel.pg_array([ts, nil], :timestamp), Sequel.pg_array([ts, nil], :timestamptz))
    @ds.count.should == 1
    rs = @ds.all
    if @jdbc || @native
      rs.should == [{:b=>[true, false], :d=>[d, nil], :t=>[t, nil], :ts=>[ts, nil], :tstz=>[ts, nil]}]
    end
    if @native
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end

    @db.create_table!(:items) do
      column :ba, 'bytea[]'
      column :tz, 'timetz[]'
      column :o, 'oid[]'
    end
    @tp.call.should == [:blob_array, :time_timezone_array, :integer_array]
    @ds.insert(Sequel.pg_array([Sequel.blob("a\0"), nil], :bytea), Sequel.pg_array([t, nil], :timetz), Sequel.pg_array([1, 2, 3], :oid))
    @ds.count.should == 1
    if @native
      rs = @ds.all
      rs.should == [{:ba=>[Sequel.blob("a\0"), nil], :tz=>[t, nil], :o=>[1, 2, 3]}]
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'insert and retrieve custom array types' do
    int2vector = Class.new do
      attr_reader :array
      def initialize(array)
        @array = array
      end
      def sql_literal_append(ds, sql)
        sql << "'#{array.join(' ')}'"
      end
      def ==(other)
        if other.is_a?(self.class)
          array == other.array
        else
          super
        end
      end
    end
    @db.register_array_type(:int2vector){|s| int2vector.new(s.split.map{|i| i.to_i})}
    @db.create_table!(:items) do
      column :b, 'int2vector[]'
    end
    @tp.call.should == [:int2vector_array]
    int2v = int2vector.new([1, 2])
    @ds.insert(Sequel.pg_array([int2v], :int2vector))
    @ds.count.should == 1
    rs = @ds.all
    if @native
      rs.should == [{:b=>[int2v]}]
      rs.first.values.each{|v| v.should_not be_a_kind_of(Array)}
      rs.first.values.each{|v| v.to_a.should be_a_kind_of(Array)}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end unless POSTGRES_DB.adapter_scheme == :jdbc

  specify 'use arrays in bound variables' do
    @db.create_table!(:items) do
      column :i, 'int4[]'
    end
    @ds.call(:insert, {:i=>[1,2]}, {:i=>:$i})
    @ds.get(:i).should == [1, 2]
    @ds.filter(:i=>:$i).call(:first, :i=>[1,2]).should == {:i=>[1,2]}
    @ds.filter(:i=>:$i).call(:first, :i=>[1,3]).should == nil

    # NULL values
    @ds.delete
    @ds.call(:insert, {:i=>[nil,nil]}, {:i=>:$i})
    @ds.first.should == {:i=>[nil, nil]}

    @db.create_table!(:items) do
      column :i, 'text[]'
    end
    a = ["\"\\\\\"{}\n\t\r \v\b123afP", 'NULL', nil, '']
    @ds.call(:insert, {:i=>:$i}, :i=>Sequel.pg_array(a))
    @ds.get(:i).should == a
    @ds.filter(:i=>:$i).call(:first, :i=>a).should == {:i=>a}
    @ds.filter(:i=>:$i).call(:first, :i=>['', nil, nil, 'a']).should == nil

    @db.create_table!(:items) do
      column :i, 'date[]'
    end
    a = [Date.today]
    @ds.call(:insert, {:i=>:$i}, :i=>Sequel.pg_array(a, 'date'))
    @ds.get(:i).should == a
    @ds.filter(:i=>:$i).call(:first, :i=>a).should == {:i=>a}
    @ds.filter(:i=>:$i).call(:first, :i=>Sequel.pg_array([Date.today-1], 'date')).should == nil

    @db.create_table!(:items) do
      column :i, 'timestamp[]'
    end
    a = [Time.local(2011, 1, 2, 3, 4, 5)]
    @ds.call(:insert, {:i=>:$i}, :i=>Sequel.pg_array(a, 'timestamp'))
    @ds.get(:i).should == a
    @ds.filter(:i=>:$i).call(:first, :i=>a).should == {:i=>a}
    @ds.filter(:i=>:$i).call(:first, :i=>Sequel.pg_array([a.first-1], 'timestamp')).should == nil

    @db.create_table!(:items) do
      column :i, 'boolean[]'
    end
    a = [true, false]
    @ds.call(:insert, {:i=>:$i}, :i=>Sequel.pg_array(a, 'boolean'))
    @ds.get(:i).should == a
    @ds.filter(:i=>:$i).call(:first, :i=>a).should == {:i=>a}
    @ds.filter(:i=>:$i).call(:first, :i=>Sequel.pg_array([false, true], 'boolean')).should == nil

    @db.create_table!(:items) do
      column :i, 'bytea[]'
    end
    a = [Sequel.blob("a\0'\"")]
    @ds.call(:insert, {:i=>:$i}, :i=>Sequel.pg_array(a, 'bytea'))
    @ds.get(:i).should == a
    @ds.filter(:i=>:$i).call(:first, :i=>a).should == {:i=>a}
    @ds.filter(:i=>:$i).call(:first, :i=>Sequel.pg_array([Sequel.blob("b\0")], 'bytea')).should == nil
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'with models' do
    @db.create_table!(:items) do
      primary_key :id
      column :i, 'integer[]'
      column :f, 'double precision[]'
      column :d, 'numeric[]'
      column :t, 'text[]'
    end
    c = Class.new(Sequel::Model(@db[:items]))
    c.plugin :pg_typecast_on_load, :i, :f, :d, :t unless @native
    o = c.create(:i=>[1,2, nil], :f=>[[1, 2.5], [3, 4.5]], :d=>[1, BigDecimal.new('1.000000000000000000001')], :t=>[%w'a b c', ['NULL', nil, '1']])
    o.i.should == [1, 2, nil]
    o.f.should == [[1, 2.5], [3, 4.5]]
    o.d.should == [BigDecimal.new('1'), BigDecimal.new('1.000000000000000000001')]
    o.t.should == [%w'a b c', ['NULL', nil, '1']]
  end

  specify 'operations/functions with pg_array_ops' do
    Sequel.extension :pg_array_ops
    @db.create_table!(:items){column :i, 'integer[]'; column :i2, 'integer[]'; column :i3, 'integer[]'; column :i4, 'integer[]'; column :i5, 'integer[]'}
    @ds.insert(Sequel.pg_array([1, 2, 3]), Sequel.pg_array([2, 1]), Sequel.pg_array([4, 4]), Sequel.pg_array([[5, 5], [4, 3]]), Sequel.pg_array([1, nil, 5]))

    @ds.get(Sequel.pg_array(:i) > :i3).should be_false
    @ds.get(Sequel.pg_array(:i3) > :i).should be_true

    @ds.get(Sequel.pg_array(:i) >= :i3).should be_false
    @ds.get(Sequel.pg_array(:i) >= :i).should be_true

    @ds.get(Sequel.pg_array(:i3) < :i).should be_false
    @ds.get(Sequel.pg_array(:i) < :i3).should be_true

    @ds.get(Sequel.pg_array(:i3) <= :i).should be_false
    @ds.get(Sequel.pg_array(:i) <= :i).should be_true

    @ds.get(Sequel.expr(5=>Sequel.pg_array(:i).any)).should be_false
    @ds.get(Sequel.expr(1=>Sequel.pg_array(:i).any)).should be_true

    @ds.get(Sequel.expr(1=>Sequel.pg_array(:i3).all)).should be_false
    @ds.get(Sequel.expr(4=>Sequel.pg_array(:i3).all)).should be_true

    @ds.get(Sequel.pg_array(:i2)[1]).should == 2
    @ds.get(Sequel.pg_array(:i2)[2]).should == 1

    @ds.get(Sequel.pg_array(:i4)[2][1]).should == 4
    @ds.get(Sequel.pg_array(:i4)[2][2]).should == 3

    @ds.get(Sequel.pg_array(:i).contains(:i2)).should be_true
    @ds.get(Sequel.pg_array(:i).contains(:i3)).should be_false

    @ds.get(Sequel.pg_array(:i2).contained_by(:i)).should be_true
    @ds.get(Sequel.pg_array(:i).contained_by(:i2)).should be_false

    @ds.get(Sequel.pg_array(:i).overlaps(:i2)).should be_true
    @ds.get(Sequel.pg_array(:i2).overlaps(:i3)).should be_false

    @ds.get(Sequel.pg_array(:i).dims).should == '[1:3]'
    @ds.get(Sequel.pg_array(:i).length).should == 3
    @ds.get(Sequel.pg_array(:i).lower).should == 1

    if @db.server_version >= 90000
      @ds.get(Sequel.pg_array(:i5).join).should == '15'
      @ds.get(Sequel.pg_array(:i5).join(':')).should == '1:5'
      @ds.get(Sequel.pg_array(:i5).join(':', '*')).should == '1:*:5'
    end
    @ds.select(Sequel.pg_array(:i).unnest).from_self.count.should == 3 if @db.server_version >= 80400

    if @native
      @ds.get(Sequel.pg_array(:i).push(4)).should == [1, 2, 3, 4]
      @ds.get(Sequel.pg_array(:i).unshift(4)).should == [4, 1, 2, 3]
      @ds.get(Sequel.pg_array(:i).concat(:i2)).should == [1, 2, 3, 2, 1]
    end
  end
end

describe 'PostgreSQL hstore handling' do
  before(:all) do
    @db = POSTGRES_DB
    @db.extension :pg_hstore
    @ds = @db[:items]
    @h = {'a'=>'b', 'c'=>nil, 'd'=>'NULL', 'e'=>'\\\\" \\\' ,=>'}
    @native = POSTGRES_DB.adapter_scheme == :postgres
  end
  after do
    @db.drop_table?(:items)
  end

  specify 'insert and retrieve hstore values' do
    @db.create_table!(:items) do
      column :h, :hstore
    end
    @ds.insert(Sequel.hstore(@h))
    @ds.count.should == 1
    if @native
      rs = @ds.all
      v = rs.first[:h]
      v.should_not be_a_kind_of(Hash)
      v.to_hash.should be_a_kind_of(Hash)
      v.to_hash.should == @h
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'use hstore in bound variables' do
    @db.create_table!(:items) do
      column :i, :hstore
    end
    @ds.call(:insert, {:i=>Sequel.hstore(@h)}, {:i=>:$i})
    @ds.get(:i).should == @h
    @ds.filter(:i=>:$i).call(:first, :i=>Sequel.hstore(@h)).should == {:i=>@h}
    @ds.filter(:i=>:$i).call(:first, :i=>Sequel.hstore({})).should == nil

    @ds.delete
    @ds.call(:insert, {:i=>Sequel.hstore('a'=>nil)}, {:i=>:$i})
    @ds.get(:i).should == Sequel.hstore('a'=>nil)

    @ds.delete
    @ds.call(:insert, {:i=>@h}, {:i=>:$i})
    @ds.get(:i).should == @h
    @ds.filter(:i=>:$i).call(:first, :i=>@h).should == {:i=>@h}
    @ds.filter(:i=>:$i).call(:first, :i=>{}).should == nil
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'with models and associations' do
    @db.create_table!(:items) do
      primary_key :id
      column :h, :hstore
    end
    c = Class.new(Sequel::Model(@db[:items])) do
      def self.name
        'Item'
      end
      unrestrict_primary_key
      def item_id
        h['item_id'].to_i if h
      end
      def left_item_id
        h['left_item_id'].to_i if h
      end
    end
    Sequel.extension :pg_hstore_ops
    c.plugin :many_through_many
    c.plugin :pg_typecast_on_load, :h unless @native

    h = {'item_id'=>"2", 'left_item_id'=>"1"}
    o2 = c.create(:id=>2)
    o = c.create(:id=>1, :h=>h)
    o.h.should == h

    c.many_to_one :item, :class=>c, :key_column=>Sequel.cast(Sequel.hstore(:h)['item_id'], Integer)
    c.one_to_many :items, :class=>c, :key=>Sequel.cast(Sequel.hstore(:h)['item_id'], Integer), :key_method=>:item_id
    c.many_to_many :related_items, :class=>c, :join_table=>:items___i, :left_key=>Sequel.cast(Sequel.hstore(:h)['left_item_id'], Integer), :right_key=>Sequel.cast(Sequel.hstore(:h)['item_id'], Integer)

    c.many_to_one :other_item, :class=>c, :key=>:id, :primary_key_method=>:item_id, :primary_key=>Sequel.cast(Sequel.hstore(:h)['item_id'], Integer)
    c.one_to_many :other_items, :class=>c, :primary_key=>:item_id, :key=>:id, :primary_key_column=>Sequel.cast(Sequel.hstore(:h)['item_id'], Integer)
    c.many_to_many :other_related_items, :class=>c, :join_table=>:items___i, :left_key=>:id, :right_key=>:id,
      :left_primary_key_column=>Sequel.cast(Sequel.hstore(:h)['left_item_id'], Integer),
      :left_primary_key=>:left_item_id,
      :right_primary_key=>Sequel.cast(Sequel.hstore(:h)['left_item_id'], Integer),
      :right_primary_key_method=>:left_item_id

    c.many_through_many :mtm_items, [
        [:items, Sequel.cast(Sequel.hstore(:h)['item_id'], Integer), Sequel.cast(Sequel.hstore(:h)['left_item_id'], Integer)],
        [:items, Sequel.cast(Sequel.hstore(:h)['left_item_id'], Integer), Sequel.cast(Sequel.hstore(:h)['left_item_id'], Integer)]
      ],
      :class=>c,
      :left_primary_key_column=>Sequel.cast(Sequel.hstore(:h)['item_id'], Integer),
      :left_primary_key=>:item_id,
      :right_primary_key=>Sequel.cast(Sequel.hstore(:h)['left_item_id'], Integer),
      :right_primary_key_method=>:left_item_id

    # Lazily Loading
    o.item.should == o2
    o2.items.should == [o]
    o.related_items.should == [o2]
    o2.other_item.should == o
    o.other_items.should == [o2]
    o.other_related_items.should == [o]
    o.mtm_items.should == [o]

    # Eager Loading via eager
    os = c.eager(:item, :related_items, :other_items, :other_related_items, :mtm_items).where(:id=>1).all.first
    os.item.should == o2
    os.related_items.should == [o2]
    os.other_items.should == [o2]
    os.other_related_items.should == [o]
    os.mtm_items.should == [o]
    os = c.eager(:items, :other_item).where(:id=>2).all.first
    os.items.should == [o]
    os.other_item.should == o

    # Eager Loading via eager_graph
    c.eager_graph(:item).where(:items__id=>1).all.first.item.should == o2
    c.eager_graph(:items).where(:items__id=>2).all.first.items.should == [o]
    c.eager_graph(:related_items).where(:items__id=>1).all.first.related_items.should == [o2]
    c.eager_graph(:other_item).where(:items__id=>2).all.first.other_item.should == o
    c.eager_graph(:other_items).where(:items__id=>1).all.first.other_items.should == [o2]
    c.eager_graph(:other_related_items).where(:items__id=>1).all.first.other_related_items.should == [o]
    c.eager_graph(:mtm_items).where(:items__id=>1).all.first.mtm_items.should == [o]

    # Filter By Associations - Model Instances
    c.filter(:item=>o2).all.should == [o]
    c.filter(:items=>o).all.should == [o2]
    c.filter(:related_items=>o2).all.should == [o]
    c.filter(:other_item=>o).all.should == [o2]
    c.filter(:other_items=>o2).all.should == [o]
    c.filter(:other_related_items=>o).all.should == [o]
    c.filter(:mtm_items=>o).all.should == [o]
   
    # Filter By Associations - Model Datasets
    c.filter(:item=>c.filter(:id=>o2.id)).all.should == [o]
    c.filter(:items=>c.filter(:id=>o.id)).all.should == [o2]
    c.filter(:related_items=>c.filter(:id=>o2.id)).all.should == [o]
    c.filter(:other_item=>c.filter(:id=>o.id)).all.should == [o2]
    c.filter(:other_items=>c.filter(:id=>o2.id)).all.should == [o]
    c.filter(:other_related_items=>c.filter(:id=>o.id)).all.should == [o]
    c.filter(:mtm_items=>c.filter(:id=>o.id)).all.should == [o]
  end

  specify 'operations/functions with pg_hstore_ops' do
    Sequel.extension :pg_hstore_ops, :pg_array, :pg_array_ops
    @db.create_table!(:items){hstore :h1; hstore :h2; hstore :h3; String :t}
    @ds.insert(Sequel.hstore('a'=>'b', 'c'=>nil), Sequel.hstore('a'=>'b'), Sequel.hstore('d'=>'e'))
    h1 = Sequel.hstore(:h1)
    h2 = Sequel.hstore(:h2)
    h3 = Sequel.hstore(:h3)
    
    @ds.get(h1['a']).should == 'b'
    @ds.get(h1['d']).should == nil

    @ds.get(h2.concat(h3).keys.pg_array.length).should == 2
    @ds.get(h1.concat(h3).keys.pg_array.length).should == 3
    @ds.get(h2.merge(h3).keys.pg_array.length).should == 2
    @ds.get(h1.merge(h3).keys.pg_array.length).should == 3

    unless [:do].include?(@db.adapter_scheme)
      # Broken DataObjects thinks operators with ? represent placeholders
      @ds.get(h1.contain_all(Sequel.pg_array(%w'a c'))).should == true
      @ds.get(h1.contain_all(Sequel.pg_array(%w'a d'))).should == false

      @ds.get(h1.contain_any(Sequel.pg_array(%w'a d'))).should == true
      @ds.get(h1.contain_any(Sequel.pg_array(%w'e d'))).should == false
    end

    @ds.get(h1.contains(h2)).should == true
    @ds.get(h1.contains(h3)).should == false

    @ds.get(h2.contained_by(h1)).should == true
    @ds.get(h2.contained_by(h3)).should == false

    @ds.get(h1.defined('a')).should == true
    @ds.get(h1.defined('c')).should == false
    @ds.get(h1.defined('d')).should == false

    @ds.get(h1.delete('a')['c']).should == nil
    @ds.get(h1.delete(Sequel.pg_array(%w'a d'))['c']).should == nil
    @ds.get(h1.delete(h2)['c']).should == nil

    @ds.from(Sequel.hstore('a'=>'b', 'c'=>nil).op.each).order(:key).all.should == [{:key=>'a', :value=>'b'}, {:key=>'c', :value=>nil}]

    unless [:do].include?(@db.adapter_scheme)
      @ds.get(h1.has_key?('c')).should == true
      @ds.get(h1.include?('c')).should == true
      @ds.get(h1.key?('c')).should == true
      @ds.get(h1.member?('c')).should == true
      @ds.get(h1.exist?('c')).should == true
      @ds.get(h1.has_key?('d')).should == false
      @ds.get(h1.include?('d')).should == false
      @ds.get(h1.key?('d')).should == false
      @ds.get(h1.member?('d')).should == false
      @ds.get(h1.exist?('d')).should == false
    end

    @ds.get(h1.hstore.hstore.hstore.keys.pg_array.length).should == 2
    @ds.get(h1.keys.pg_array.length).should == 2
    @ds.get(h2.keys.pg_array.length).should == 1
    @ds.get(h1.akeys.pg_array.length).should == 2
    @ds.get(h2.akeys.pg_array.length).should == 1

    @ds.from(Sequel.hstore('t'=>'s').op.populate(Sequel::SQL::Cast.new(nil, :items))).select_map(:t).should == ['s']
    @ds.from(:items___i).select(Sequel.hstore('t'=>'s').op.record_set(:i).as(:r)).from_self(:alias=>:s).select(Sequel.lit('(r).*')).from_self.select_map(:t).should == ['s']

    @ds.from(Sequel.hstore('t'=>'s', 'a'=>'b').op.skeys.as(:s)).select_order_map(:s).should == %w'a t'
    @ds.from((Sequel.hstore('t'=>'s', 'a'=>'b').op - 'a').skeys.as(:s)).select_order_map(:s).should == %w't'

    @ds.get(h1.slice(Sequel.pg_array(%w'a c')).keys.pg_array.length).should == 2
    @ds.get(h1.slice(Sequel.pg_array(%w'd c')).keys.pg_array.length).should == 1
    @ds.get(h1.slice(Sequel.pg_array(%w'd e')).keys.pg_array.length).should == nil

    @ds.from(Sequel.hstore('t'=>'s', 'a'=>'b').op.svals.as(:s)).select_order_map(:s).should == %w'b s'

    @ds.get(h1.to_array.pg_array.length).should == 4
    @ds.get(h2.to_array.pg_array.length).should == 2

    @ds.get(h1.to_matrix.pg_array.length).should == 2
    @ds.get(h2.to_matrix.pg_array.length).should == 1

    @ds.get(h1.values.pg_array.length).should == 2
    @ds.get(h2.values.pg_array.length).should == 1
    @ds.get(h1.avals.pg_array.length).should == 2
    @ds.get(h2.avals.pg_array.length).should == 1
  end
end if POSTGRES_DB.type_supported?(:hstore)

describe 'PostgreSQL json type' do
  before(:all) do
    @db = POSTGRES_DB
    @db.extension :pg_array, :pg_json
    @ds = @db[:items]
    @a = [1, 2, {'a'=>'b'}, 3.0]
    @h = {'a'=>'b', '1'=>[3, 4, 5]}
    @native = POSTGRES_DB.adapter_scheme == :postgres
  end
  after do
    @db.drop_table?(:items)
  end

  specify 'insert and retrieve json values' do
    @db.create_table!(:items){json :j}
    @ds.insert(Sequel.pg_json(@h))
    @ds.count.should == 1
    if @native
      rs = @ds.all
      v = rs.first[:j]
      v.should_not be_a_kind_of(Hash)
      v.to_hash.should be_a_kind_of(Hash)
      v.should == @h
      v.to_hash.should == @h
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end

    @ds.delete
    @ds.insert(Sequel.pg_json(@a))
    @ds.count.should == 1
    if @native
      rs = @ds.all
      v = rs.first[:j]
      v.should_not be_a_kind_of(Array)
      v.to_a.should be_a_kind_of(Array)
      v.should == @a
      v.to_a.should == @a
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'insert and retrieve json[] values' do
    @db.create_table!(:items){column :j, 'json[]'}
    j = Sequel.pg_array([Sequel.pg_json('a'=>1), Sequel.pg_json(['b', 2])])
    @ds.insert(j)
    @ds.count.should == 1
    if @native
      rs = @ds.all
      v = rs.first[:j]
      v.should_not be_a_kind_of(Array)
      v.to_a.should be_a_kind_of(Array)
      v.should == j
      v.to_a.should == j
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'use json in bound variables' do
    @db.create_table!(:items){json :i}
    @ds.call(:insert, {:i=>Sequel.pg_json(@h)}, {:i=>:$i})
    @ds.get(:i).should == @h
    @ds.filter(Sequel.cast(:i, String)=>:$i).call(:first, :i=>Sequel.pg_json(@h)).should == {:i=>@h}
    @ds.filter(Sequel.cast(:i, String)=>:$i).call(:first, :i=>Sequel.pg_json({})).should == nil
    @ds.filter(Sequel.cast(:i, String)=>:$i).call(:delete, :i=>Sequel.pg_json(@h)).should == 1

    @ds.call(:insert, {:i=>Sequel.pg_json(@a)}, {:i=>:$i})
    @ds.get(:i).should == @a
    @ds.filter(Sequel.cast(:i, String)=>:$i).call(:first, :i=>Sequel.pg_json(@a)).should == {:i=>@a}
    @ds.filter(Sequel.cast(:i, String)=>:$i).call(:first, :i=>Sequel.pg_json([])).should == nil

    @ds.delete
    @ds.call(:insert, {:i=>Sequel.pg_json('a'=>nil)}, {:i=>:$i})
    @ds.get(:i).should == Sequel.pg_json('a'=>nil)

    @db.create_table!(:items){column :i, 'json[]'}
    j = Sequel.pg_array([Sequel.pg_json('a'=>1), Sequel.pg_json(['b', 2])], :text)
    @ds.call(:insert, {:i=>j}, {:i=>:$i})
    @ds.get(:i).should == j
    @ds.filter(Sequel.cast(:i, 'text[]')=>:$i).call(:first, :i=>j).should == {:i=>j}
    @ds.filter(Sequel.cast(:i, 'text[]')=>:$i).call(:first, :i=>Sequel.pg_array([])).should == nil
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'with models' do
    @db.create_table!(:items) do
      primary_key :id
      json :h
    end
    c = Class.new(Sequel::Model(@db[:items]))
    c.plugin :pg_typecast_on_load, :h unless @native
    c.create(:h=>Sequel.pg_json(@h)).h.should == @h
    c.create(:h=>Sequel.pg_json(@a)).h.should == @a
  end
end if POSTGRES_DB.server_version >= 90200

describe 'PostgreSQL inet/cidr types' do
  ipv6_broken = (IPAddr.new('::1'); false) rescue true

  before(:all) do
    @db = POSTGRES_DB
    @db.extension :pg_array, :pg_inet
    @ds = @db[:items]
    @v4 = '127.0.0.1'
    @v4nm = '127.0.0.0/8'
    @v6 = '2001:4f8:3:ba:2e0:81ff:fe22:d1f1'
    @v6nm = '2001:4f8:3:ba::/64'
    @ipv4 = IPAddr.new(@v4)
    @ipv4nm = IPAddr.new(@v4nm)
    unless ipv6_broken
      @ipv6 = IPAddr.new(@v6)
      @ipv6nm = IPAddr.new(@v6nm)
    end
    @native = POSTGRES_DB.adapter_scheme == :postgres
  end
  after do
    @db.drop_table?(:items)
  end

  specify 'insert and retrieve inet/cidr values' do
    @db.create_table!(:items){inet :i; cidr :c}
    @ds.insert(@ipv4, @ipv4nm)
    @ds.count.should == 1
    if @native
      rs = @ds.all
      rs.first[:i].should == @ipv4
      rs.first[:c].should == @ipv4nm
      rs.first[:i].should be_a_kind_of(IPAddr)
      rs.first[:c].should be_a_kind_of(IPAddr)
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end

    unless ipv6_broken
      @ds.delete
      @ds.insert(@ipv6, @ipv6nm)
      @ds.count.should == 1
      if @native
        rs = @ds.all
        rs.first[:j]
        rs.first[:i].should == @ipv6
        rs.first[:c].should == @ipv6nm
        rs.first[:i].should be_a_kind_of(IPAddr)
        rs.first[:c].should be_a_kind_of(IPAddr)
        @ds.delete
        @ds.insert(rs.first)
        @ds.all.should == rs
      end
    end
  end

  specify 'insert and retrieve inet/cidr/macaddr array values' do
    @db.create_table!(:items){column :i, 'inet[]'; column :c, 'cidr[]'; column :m, 'macaddr[]'}
    @ds.insert(Sequel.pg_array([@ipv4], 'inet'), Sequel.pg_array([@ipv4nm], 'cidr'), Sequel.pg_array(['12:34:56:78:90:ab'], 'macaddr'))
    @ds.count.should == 1
    if @native
      rs = @ds.all
      rs.first.values.all?{|c| c.is_a?(Sequel::Postgres::PGArray)}.should be_true
      rs.first[:i].first.should == @ipv4
      rs.first[:c].first.should == @ipv4nm
      rs.first[:m].first.should == '12:34:56:78:90:ab'
      rs.first[:i].first.should be_a_kind_of(IPAddr)
      rs.first[:c].first.should be_a_kind_of(IPAddr)
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'use ipaddr in bound variables' do
    @db.create_table!(:items){inet :i; cidr :c}

    @ds.call(:insert, {:i=>@ipv4, :c=>@ipv4nm}, {:i=>:$i, :c=>:$c})
    @ds.get(:i).should == @ipv4
    @ds.get(:c).should == @ipv4nm
    @ds.filter(:i=>:$i, :c=>:$c).call(:first, :i=>@ipv4, :c=>@ipv4nm).should == {:i=>@ipv4, :c=>@ipv4nm}
    @ds.filter(:i=>:$i, :c=>:$c).call(:first, :i=>@ipv6, :c=>@ipv6nm).should == nil
    @ds.filter(:i=>:$i, :c=>:$c).call(:delete, :i=>@ipv4, :c=>@ipv4nm).should == 1

    unless ipv6_broken
      @ds.call(:insert, {:i=>@ipv6, :c=>@ipv6nm}, {:i=>:$i, :c=>:$c})
      @ds.get(:i).should == @ipv6
      @ds.get(:c).should == @ipv6nm
      @ds.filter(:i=>:$i, :c=>:$c).call(:first, :i=>@ipv6, :c=>@ipv6nm).should == {:i=>@ipv6, :c=>@ipv6nm}
      @ds.filter(:i=>:$i, :c=>:$c).call(:first, :i=>@ipv4, :c=>@ipv4nm).should == nil
      @ds.filter(:i=>:$i, :c=>:$c).call(:delete, :i=>@ipv6, :c=>@ipv6nm).should == 1
    end

    @db.create_table!(:items){column :i, 'inet[]'; column :c, 'cidr[]'; column :m, 'macaddr[]'}
    @ds.call(:insert, {:i=>[@ipv4], :c=>[@ipv4nm], :m=>['12:34:56:78:90:ab']}, {:i=>:$i, :c=>:$c, :m=>:$m})
    @ds.filter(:i=>:$i, :c=>:$c, :m=>:$m).call(:first, :i=>[@ipv4], :c=>[@ipv4nm], :m=>['12:34:56:78:90:ab']).should == {:i=>[@ipv4], :c=>[@ipv4nm], :m=>['12:34:56:78:90:ab']}
    @ds.filter(:i=>:$i, :c=>:$c, :m=>:$m).call(:first, :i=>[], :c=>[], :m=>[]).should == nil
    @ds.filter(:i=>:$i, :c=>:$c, :m=>:$m).call(:delete, :i=>[@ipv4], :c=>[@ipv4nm], :m=>['12:34:56:78:90:ab']).should == 1
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'with models' do
    @db.create_table!(:items) do
      primary_key :id
      inet :i
      cidr :c
    end
    c = Class.new(Sequel::Model(@db[:items]))
    c.plugin :pg_typecast_on_load, :i, :c unless @native
    c.create(:i=>@v4, :c=>@v4nm).values.values_at(:i, :c).should == [@ipv4, @ipv4nm]
    unless ipv6_broken
      c.create(:i=>@ipv6, :c=>@ipv6nm).values.values_at(:i, :c).should == [@ipv6, @ipv6nm]
    end
  end
end

describe 'PostgreSQL range types' do
  before(:all) do
    @db = POSTGRES_DB
    @db.extension :pg_array, :pg_range
    @ds = @db[:items]
    @map = {:i4=>'int4range', :i8=>'int8range', :n=>'numrange', :d=>'daterange', :t=>'tsrange', :tz=>'tstzrange'}
    @r = {:i4=>1...2, :i8=>2...3, :n=>BigDecimal.new('1.0')..BigDecimal.new('2.0'), :d=>Date.today...(Date.today+1), :t=>Time.local(2011, 1)..Time.local(2011, 2), :tz=>Time.local(2011, 1)..Time.local(2011, 2)}
    @ra = {}
    @pgr = {}
    @pgra = {}
    @r.each{|k, v| @ra[k] = Sequel.pg_array([v], @map[k])}
    @r.each{|k, v| @pgr[k] = Sequel.pg_range(v)}
    @r.each{|k, v| @pgra[k] = Sequel.pg_array([Sequel.pg_range(v)], @map[k])}
    @native = POSTGRES_DB.adapter_scheme == :postgres
  end
  after do
    @db.drop_table?(:items)
  end

  specify 'insert and retrieve range type values' do
    @db.create_table!(:items){int4range :i4; int8range :i8; numrange :n; daterange :d; tsrange :t; tstzrange :tz}
    [@r, @pgr].each do |input|
      h = {}
      input.each{|k, v| h[k] = Sequel.cast(v, @map[k])}
      @ds.insert(h)
      @ds.count.should == 1
      if @native
        rs = @ds.all
        rs.first.each do |k, v|
          v.should_not be_a_kind_of(Range)
          v.to_range.should be_a_kind_of(Range)
          v.should == @r[k]
          v.to_range.should == @r[k]
        end
        @ds.delete
        @ds.insert(rs.first)
        @ds.all.should == rs
      end
      @ds.delete
    end
  end

  specify 'insert and retrieve arrays of range type values' do
    @db.create_table!(:items){column :i4, 'int4range[]'; column :i8, 'int8range[]'; column :n, 'numrange[]'; column :d, 'daterange[]'; column :t, 'tsrange[]'; column :tz, 'tstzrange[]'}
    [@ra, @pgra].each do |input|
      @ds.insert(input)
      @ds.count.should == 1
      if @native
        rs = @ds.all
        rs.first.each do |k, v|
          v.should_not be_a_kind_of(Array)
          v.to_a.should be_a_kind_of(Array)
          v.first.should_not be_a_kind_of(Range)
          v.first.to_range.should be_a_kind_of(Range)
          v.should == @ra[k].to_a
          v.first.should == @r[k]
        end
        @ds.delete
        @ds.insert(rs.first)
        @ds.all.should == rs
      end
      @ds.delete
    end
  end

  specify 'use range types in bound variables' do
    @db.create_table!(:items){int4range :i4; int8range :i8; numrange :n; daterange :d; tsrange :t; tstzrange :tz}
    h = {}
    @r.keys.each{|k| h[k] = :"$#{k}"}
    r2 = {}
    @r.each{|k, v| r2[k] = Range.new(v.begin, v.end+2)}
    @ds.call(:insert, @r, h)
    @ds.first.should == @r
    @ds.filter(h).call(:first, @r).should == @r
    @ds.filter(h).call(:first, @pgr).should == @r
    @ds.filter(h).call(:first, r2).should == nil
    @ds.filter(h).call(:delete, @r).should == 1

    @db.create_table!(:items){column :i4, 'int4range[]'; column :i8, 'int8range[]'; column :n, 'numrange[]'; column :d, 'daterange[]'; column :t, 'tsrange[]'; column :tz, 'tstzrange[]'}
    @r.each{|k, v| r2[k] = [Range.new(v.begin, v.end+2)]}
    @ds.call(:insert, @ra, h)
    @ds.filter(h).call(:first, @ra).each{|k, v| v.should == @ra[k].to_a}
    @ds.filter(h).call(:first, @pgra).each{|k, v| v.should == @ra[k].to_a}
    @ds.filter(h).call(:first, r2).should == nil
    @ds.filter(h).call(:delete, @ra).should == 1
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'with models' do
    @db.create_table!(:items){primary_key :id; int4range :i4; int8range :i8; numrange :n; daterange :d; tsrange :t; tstzrange :tz}
    c = Class.new(Sequel::Model(@db[:items]))
    c.plugin :pg_typecast_on_load, :i4, :i8, :n, :d, :t, :tz unless @native
    v = c.create(@r).values
    v.delete(:id)
    v.should == @r

    unless @db.adapter_scheme == :jdbc
      @db.create_table!(:items){primary_key :id; column :i4, 'int4range[]'; column :i8, 'int8range[]'; column :n, 'numrange[]'; column :d, 'daterange[]'; column :t, 'tsrange[]'; column :tz, 'tstzrange[]'}
      c = Class.new(Sequel::Model(@db[:items]))
      c.plugin :pg_typecast_on_load, :i4, :i8, :n, :d, :t, :tz unless @native
      v = c.create(@ra).values
      v.delete(:id)
      v.each{|k,v1| v1.should == @ra[k].to_a}
    end
  end

  specify 'operations/functions with pg_range_ops' do
    Sequel.extension :pg_range_ops

    @db.get(Sequel.pg_range(1..5, :int4range).op.contains(2..4)).should be_true
    @db.get(Sequel.pg_range(1..5, :int4range).op.contains(3..6)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.contains(0..6)).should be_false

    @db.get(Sequel.pg_range(1..5, :int4range).op.contained_by(0..6)).should be_true
    @db.get(Sequel.pg_range(1..5, :int4range).op.contained_by(3..6)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.contained_by(2..4)).should be_false

    @db.get(Sequel.pg_range(1..5, :int4range).op.overlaps(5..6)).should be_true
    @db.get(Sequel.pg_range(1...5, :int4range).op.overlaps(5..6)).should be_false
    
    @db.get(Sequel.pg_range(1..5, :int4range).op.left_of(6..10)).should be_true
    @db.get(Sequel.pg_range(1..5, :int4range).op.left_of(5..10)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.left_of(-1..0)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.left_of(-1..3)).should be_false

    @db.get(Sequel.pg_range(1..5, :int4range).op.right_of(6..10)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.right_of(5..10)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.right_of(-1..0)).should be_true
    @db.get(Sequel.pg_range(1..5, :int4range).op.right_of(-1..3)).should be_false

    @db.get(Sequel.pg_range(1..5, :int4range).op.starts_before(6..10)).should be_true
    @db.get(Sequel.pg_range(1..5, :int4range).op.starts_before(5..10)).should be_true
    @db.get(Sequel.pg_range(1..5, :int4range).op.starts_before(-1..0)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.starts_before(-1..3)).should be_false

    @db.get(Sequel.pg_range(1..5, :int4range).op.ends_after(6..10)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.ends_after(5..10)).should be_false
    @db.get(Sequel.pg_range(1..5, :int4range).op.ends_after(-1..0)).should be_true
    @db.get(Sequel.pg_range(1..5, :int4range).op.ends_after(-1..3)).should be_true

    @db.get(Sequel.pg_range(1..5, :int4range).op.adjacent_to(6..10)).should be_true
    @db.get(Sequel.pg_range(1...5, :int4range).op.adjacent_to(6..10)).should be_false

    @db.get((Sequel.pg_range(1..5, :int4range).op + (6..10)).adjacent_to(6..10)).should be_false
    @db.get((Sequel.pg_range(1..5, :int4range).op + (6..10)).adjacent_to(11..20)).should be_true

    @db.get((Sequel.pg_range(1..5, :int4range).op * (2..6)).adjacent_to(6..10)).should be_true
    @db.get((Sequel.pg_range(1..4, :int4range).op * (2..6)).adjacent_to(6..10)).should be_false

    @db.get((Sequel.pg_range(1..5, :int4range).op - (2..6)).adjacent_to(2..10)).should be_true
    @db.get((Sequel.pg_range(0..4, :int4range).op - (3..6)).adjacent_to(4..10)).should be_false

    @db.get(Sequel.pg_range(0..4, :int4range).op.lower).should == 0
    @db.get(Sequel.pg_range(0..4, :int4range).op.upper).should == 5

    @db.get(Sequel.pg_range(0..4, :int4range).op.isempty).should be_false
    @db.get(Sequel::Postgres::PGRange.empty(:int4range).op.isempty).should be_true

    @db.get(Sequel.pg_range(1..5, :numrange).op.lower_inc).should be_true
    @db.get(Sequel::Postgres::PGRange.new(1, 5, :exclude_begin=>true, :db_type=>:numrange).op.lower_inc).should be_false

    @db.get(Sequel.pg_range(1..5, :numrange).op.upper_inc).should be_true
    @db.get(Sequel.pg_range(1...5, :numrange).op.upper_inc).should be_false

    @db.get(Sequel::Postgres::PGRange.new(1, 5, :db_type=>:int4range).op.lower_inf).should be_false
    @db.get(Sequel::Postgres::PGRange.new(nil, 5, :db_type=>:int4range).op.lower_inf).should be_true

    @db.get(Sequel::Postgres::PGRange.new(1, 5, :db_type=>:int4range).op.upper_inf).should be_false
    @db.get(Sequel::Postgres::PGRange.new(1, nil, :db_type=>:int4range).op.upper_inf).should be_true
  end
end if POSTGRES_DB.server_version >= 90200

describe 'PostgreSQL interval types' do
  before(:all) do
    @db = POSTGRES_DB
    @db.extension :pg_array, :pg_interval
    @ds = @db[:items]
    @native = POSTGRES_DB.adapter_scheme == :postgres
  end
  after(:all) do
    Sequel::Postgres::PG_TYPES.delete(1186)
  end
  after do
    @db.drop_table?(:items)
  end

  specify 'insert and retrieve interval values' do
    @db.create_table!(:items){interval :i}
    [
      ['0', '00:00:00',  0, [[:seconds, 0]]],
      ['1 microsecond', '00:00:00.000001',  0.000001, [[:seconds, 0.000001]]],
      ['1 millisecond', '00:00:00.001',  0.001, [[:seconds, 0.001]]],
      ['1 second', '00:00:01', 1, [[:seconds, 1]]],
      ['1 minute', '00:01:00', 60, [[:seconds, 60]]],
      ['1 hour', '01:00:00', 3600, [[:seconds, 3600]]],
      ['1 day', '1 day', 86400, [[:days, 1]]],
      ['1 week', '7 days', 86400*7, [[:days, 7]]],
      ['1 month', '1 mon', 86400*30, [[:months, 1]]],
      ['1 year', '1 year', 31557600, [[:years, 1]]],
      ['1 decade', '10 years', 31557600*10, [[:years, 10]]],
      ['1 century', '100 years', 31557600*100, [[:years, 100]]],
      ['1 millennium', '1000 years', 31557600*1000, [[:years, 1000]]],
      ['1 year 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds', '1 year 2 mons 25 days 05:06:07', 31557600 + 2*86400*30 + 3*86400*7 + 4*86400 + 5*3600 + 6*60 + 7, [[:years, 1], [:months, 2], [:days, 25], [:seconds, 18367]]],
      ['-1 year +2 months -3 weeks +4 days -5 hours +6 minutes -7 seconds', '-10 mons -17 days -04:54:07', -10*86400*30 - 3*86400*7 + 4*86400 - 5*3600 + 6*60 - 7, [[:months, -10], [:days, -17], [:seconds, -17647]]],
      ['+2 years -1 months +3 weeks -4 days +5 hours -6 minutes +7 seconds', '1 year 11 mons 17 days 04:54:07', 31557600 + 11*86400*30 + 3*86400*7 - 4*86400 + 5*3600 - 6*60 + 7, [[:years, 1], [:months, 11], [:days, 17], [:seconds, 17647]]],
    ].each do |instr, outstr, value, parts|
      @ds.insert(instr)
      @ds.count.should == 1
      if @native
        @ds.get(Sequel.cast(:i, String)).should == outstr
        rs = @ds.all
        rs.first[:i].is_a?(ActiveSupport::Duration).should be_true
        rs.first[:i].should == ActiveSupport::Duration.new(value, parts)
        rs.first[:i].parts.sort_by{|k,v| k.to_s}.should == parts.sort_by{|k,v| k.to_s}
        @ds.delete
        @ds.insert(rs.first)
        @ds.all.should == rs
      end
      @ds.delete
    end
  end

  specify 'insert and retrieve interval array values' do
    @db.create_table!(:items){column :i, 'interval[]'}
    @ds.insert(Sequel.pg_array(['1 year 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds'], 'interval'))
    @ds.count.should == 1
    if @native
      rs = @ds.all
      rs.first[:i].is_a?(Sequel::Postgres::PGArray).should be_true
      rs.first[:i].first.is_a?(ActiveSupport::Duration).should be_true
      rs.first[:i].first.should == ActiveSupport::Duration.new(31557600 + 2*86400*30 + 3*86400*7 + 4*86400 + 5*3600 + 6*60 + 7, [[:years, 1], [:months, 2], [:days, 25], [:seconds, 18367]])
      rs.first[:i].first.parts.sort_by{|k,v| k.to_s}.should == [[:years, 1], [:months, 2], [:days, 25], [:seconds, 18367]].sort_by{|k,v| k.to_s}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs
    end
  end

  specify 'use intervals in bound variables' do
    @db.create_table!(:items){interval :i}
    @ds.insert('1 year 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds')
    d = @ds.get(:i)
    @ds.delete

    @ds.call(:insert, {:i=>d}, {:i=>:$i})
    @ds.get(:i).should == d
    @ds.filter(:i=>:$i).call(:first, :i=>d).should == {:i=>d}
    @ds.filter(:i=>:$i).call(:first, :i=>'0').should == nil
    @ds.filter(:i=>:$i).call(:delete, :i=>d).should == 1

    @db.create_table!(:items){column :i, 'interval[]'}
    @ds.call(:insert, {:i=>[d]}, {:i=>:$i})
    @ds.filter(:i=>:$i).call(:first, :i=>[d]).should == {:i=>[d]}
    @ds.filter(:i=>:$i).call(:first, :i=>[]).should == nil
    @ds.filter(:i=>:$i).call(:delete, :i=>[d]).should == 1
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'with models' do
    @db.create_table!(:items) do
      primary_key :id
      interval :i
    end
    c = Class.new(Sequel::Model(@db[:items]))
    c.plugin :pg_typecast_on_load, :i, :c unless @native
    v = c.create(:i=>'1 year 2 mons 25 days 05:06:07').i
    v.is_a?(ActiveSupport::Duration).should be_true
    v.should == ActiveSupport::Duration.new(31557600 + 2*86400*30 + 3*86400*7 + 4*86400 + 5*3600 + 6*60 + 7, [[:years, 1], [:months, 2], [:days, 25], [:seconds, 18367]])
    v.parts.sort_by{|k,_| k.to_s}.should == [[:years, 1], [:months, 2], [:days, 25], [:seconds, 18367]].sort_by{|k,_| k.to_s}
  end
end if (begin require 'active_support/duration'; require 'active_support/inflector'; require 'active_support/core_ext/string/inflections'; true; rescue LoadError; false end)

describe 'PostgreSQL row-valued/composite types' do
  before(:all) do
    @db = POSTGRES_DB
    Sequel.extension :pg_array_ops, :pg_row_ops
    @db.extension :pg_array, :pg_row
    @ds = @db[:person]

    @db.create_table!(:address) do
      String :street
      String :city
      String :zip
    end
    @db.create_table!(:person) do
      Integer :id
      address :address
    end
    @db.create_table!(:company) do
      Integer :id
      column :employees, 'person[]'
    end
    @db.register_row_type(:address)
    @db.register_row_type(Sequel.qualify(:public, :person))
    @db.register_row_type(:public__company)

    @native = POSTGRES_DB.adapter_scheme == :postgres
  end
  after(:all) do
    @db.drop_table?(:company, :person, :address)
    @db.row_types.clear
    @db.reset_conversion_procs if @native
  end
  after do
    [:company, :person, :address].each{|t| @db[t].delete}
  end

  specify 'insert and retrieve row types' do
    @ds.insert(:id=>1, :address=>Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345']))
    @ds.count.should == 1
    if @native
      # Single row valued type
      rs = @ds.all
      v = rs.first[:address]
      v.should_not be_a_kind_of(Hash)
      v.to_hash.should be_a_kind_of(Hash)
      v.to_hash.should == {:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}
      @ds.delete
      @ds.insert(rs.first)
      @ds.all.should == rs

      # Nested row value type
      p = @ds.get(:person)
      p[:id].should == 1
      p[:address].should == v
    end
  end

  specify 'insert and retrieve arrays of row types' do
    @ds = @db[:company]
    @ds.insert(:id=>1, :employees=>Sequel.pg_array([@db.row_type(:person, [1, Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345'])])]))
    @ds.count.should == 1
    if @native
      v = @ds.get(:company)
      v.should_not be_a_kind_of(Hash)
      v.to_hash.should be_a_kind_of(Hash)
      v[:id].should == 1
      employees = v[:employees]
      employees.should_not be_a_kind_of(Array)
      employees.to_a.should be_a_kind_of(Array)
      employees.should == [{:id=>1, :address=>{:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}}]
      @ds.delete
      @ds.insert(v[:id], v[:employees])
      @ds.get(:company).should == v
    end
  end

  specify 'use row types in bound variables' do
    @ds.call(:insert, {:address=>Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345'])}, {:address=>:$address, :id=>1})
    @ds.get(:address).should == {:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}
    @ds.filter(:address=>Sequel.cast(:$address, :address)).call(:first, :address=>Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345']))[:id].should == 1
    @ds.filter(:address=>Sequel.cast(:$address, :address)).call(:first, :address=>Sequel.pg_row(['123 Sesame St', 'Somewhere', '12356'])).should == nil

    @ds.delete
    @ds.call(:insert, {:address=>Sequel.pg_row([nil, nil, nil])}, {:address=>:$address, :id=>1})
    @ds.get(:address).should == {:street=>nil, :city=>nil, :zip=>nil}
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'use arrays of row types in bound variables' do
    @ds = @db[:company]
    @ds.call(:insert, {:employees=>Sequel.pg_array([@db.row_type(:person, [1, Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345'])])])}, {:employees=>:$employees, :id=>1})
    @ds.get(:company).should == {:id=>1, :employees=>[{:id=>1, :address=>{:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}}]}
    @ds.filter(:employees=>Sequel.cast(:$employees, 'person[]')).call(:first, :employees=>Sequel.pg_array([@db.row_type(:person, [1, Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345'])])]))[:id].should == 1
    @ds.filter(:employees=>Sequel.cast(:$employees, 'person[]')).call(:first, :employees=>Sequel.pg_array([@db.row_type(:person, [1, Sequel.pg_row(['123 Sesame St', 'Somewhere', '12356'])])])).should == nil

    @ds.delete
    @ds.call(:insert, {:employees=>Sequel.pg_array([@db.row_type(:person, [1, Sequel.pg_row([nil, nil, nil])])])}, {:employees=>:$employees, :id=>1})
    @ds.get(:employees).should == [{:address=>{:city=>nil, :zip=>nil, :street=>nil}, :id=>1}]
  end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

  specify 'operations/functions with pg_row_ops' do
    @ds.insert(:id=>1, :address=>Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345']))
    @ds.get(Sequel.pg_row(:address)[:street]).should == '123 Sesame St'
    @ds.get(Sequel.pg_row(:address)[:city]).should == 'Somewhere'
    @ds.get(Sequel.pg_row(:address)[:zip]).should == '12345'

    @ds = @db[:company]
    @ds.insert(:id=>1, :employees=>Sequel.pg_array([@db.row_type(:person, [1, Sequel.pg_row(['123 Sesame St', 'Somewhere', '12345'])])]))
    @ds.get(Sequel.pg_row(:company)[:id]).should == 1
    if @native
      @ds.get(Sequel.pg_row(:company)[:employees]).should == [{:id=>1, :address=>{:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}}]
      @ds.get(Sequel.pg_row(:company)[:employees][1]).should == {:id=>1, :address=>{:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}}
      @ds.get(Sequel.pg_row(:company)[:employees][1][:address]).should == {:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}
    end
    @ds.get(Sequel.pg_row(:company)[:employees][1][:id]).should == 1
    @ds.get(Sequel.pg_row(:company)[:employees][1][:address][:street]).should == '123 Sesame St'
    @ds.get(Sequel.pg_row(:company)[:employees][1][:address][:city]).should == 'Somewhere'
    @ds.get(Sequel.pg_row(:company)[:employees][1][:address][:zip]).should == '12345'
  end

  context "#splat and #*" do
    before(:all) do
      @db.create_table!(:a){Integer :a}
      @db.create_table!(:b){a :b; Integer :a}
      @db.register_row_type(:a)
      @db.register_row_type(:b)
      @db[:b].insert(:a=>1, :b=>@db.row_type(:a, [2]))
    end
    after(:all) do
      @db.drop_table?(:b, :a)
    end

    specify "splat should reference the table type" do
      @db[:b].select(:a).first.should == {:a=>1}
      @db[:b].select(:b__a).first.should == {:a=>1}
      @db[:b].select(Sequel.pg_row(:b)[:a]).first.should == {:a=>2}
      @db[:b].select(Sequel.pg_row(:b).splat[:a]).first.should == {:a=>1}

      if @native
        @db[:b].select(:b).first.should == {:b=>{:a=>2}}
        @db[:b].select(Sequel.pg_row(:b).splat).first.should == {:a=>1, :b=>{:a=>2}}
        @db[:b].select(Sequel.pg_row(:b).splat(:b)).first.should == {:b=>{:a=>1, :b=>{:a=>2}}}
      end
    end

    specify "* should expand the table type into separate columns" do
      ds = @db[:b].select(Sequel.pg_row(:b).splat(:b)).from_self(:alias=>:t)
      if @native
        ds.first.should == {:b=>{:a=>1, :b=>{:a=>2}}}
        ds.select(Sequel.pg_row(:b).*).first.should == {:a=>1, :b=>{:a=>2}}
        ds.select(Sequel.pg_row(:b)[:b]).first.should == {:b=>{:a=>2}}
        ds.select(Sequel.pg_row(:t__b).*).first.should == {:a=>1, :b=>{:a=>2}}
        ds.select(Sequel.pg_row(:t__b)[:b]).first.should == {:b=>{:a=>2}}
      end
      ds.select(Sequel.pg_row(:b)[:a]).first.should == {:a=>1}
      ds.select(Sequel.pg_row(:t__b)[:a]).first.should == {:a=>1}
    end
  end

  context "with models" do
    before(:all) do
      class Address < Sequel::Model(:address)
        plugin :pg_row
      end
      class Person < Sequel::Model(:person)
        plugin :pg_row
      end
      class Company < Sequel::Model(:company)
        plugin :pg_row
      end
      @a = Address.new(:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345')
      @es = Sequel.pg_array([Person.new(:id=>1, :address=>@a)])
    end
    after(:all) do
      Object.send(:remove_const, :Address) rescue nil
      Object.send(:remove_const, :Person) rescue nil
      Object.send(:remove_const, :Company) rescue nil
    end

    specify 'insert and retrieve row types as model objects' do
      @ds.insert(:id=>1, :address=>@a)
      @ds.count.should == 1
      if @native
        # Single row valued type
        rs = @ds.all
        v = rs.first[:address]
        v.should be_a_kind_of(Address)
        v.should == @a
        @ds.delete
        @ds.insert(rs.first)
        @ds.all.should == rs

        # Nested row value type
        p = @ds.get(:person)
        p.should be_a_kind_of(Person)
        p.id.should == 1
        p.address.should be_a_kind_of(Address)
        p.address.should == @a
      end
    end

    specify 'insert and retrieve arrays of row types as model objects' do
      @ds = @db[:company]
      @ds.insert(:id=>1, :employees=>@es)
      @ds.count.should == 1
      if @native
        v = @ds.get(:company)
        v.should be_a_kind_of(Company)
        v.id.should == 1
        employees = v[:employees]
        employees.should_not be_a_kind_of(Array)
        employees.to_a.should be_a_kind_of(Array)
        employees.should == @es
        @ds.delete
        @ds.insert(v.id, v.employees)
        @ds.get(:company).should == v
      end
    end

    specify 'use model objects in bound variables' do
      @ds.call(:insert, {:address=>@a}, {:address=>:$address, :id=>1})
      @ds.get(:address).should == @a
      @ds.filter(:address=>Sequel.cast(:$address, :address)).call(:first, :address=>@a)[:id].should == 1
      @ds.filter(:address=>Sequel.cast(:$address, :address)).call(:first, :address=>Address.new(:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12356')).should == nil
    end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

    specify 'use arrays of model objects in bound variables' do
      @ds = @db[:company]
      @ds.call(:insert, {:employees=>@es}, {:employees=>:$employees, :id=>1})
      @ds.get(:company).should == Company.new(:id=>1, :employees=>@es)
      @ds.filter(:employees=>Sequel.cast(:$employees, 'person[]')).call(:first, :employees=>@es)[:id].should == 1
      @ds.filter(:employees=>Sequel.cast(:$employees, 'person[]')).call(:first, :employees=>Sequel.pg_array([@db.row_type(:person, [1, Sequel.pg_row(['123 Sesame St', 'Somewhere', '12356'])])])).should == nil
    end if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG

    specify 'model typecasting' do
      Person.plugin :pg_typecast_on_load, :address unless @native
      a = Address.new(:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345')
      o = Person.create(:id=>1, :address=>['123 Sesame St', 'Somewhere', '12345'])
      o.address.should == a
      o = Person.create(:id=>1, :address=>{:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'})
      o.address.should == a
      o = Person.create(:id=>1, :address=>a)
      o.address.should == a

      Company.plugin :pg_typecast_on_load, :employees unless @native
      e = Person.new(:id=>1, :address=>a)
      unless @db.adapter_scheme == :jdbc
        o = Company.create(:id=>1, :employees=>[{:id=>1, :address=>{:street=>'123 Sesame St', :city=>'Somewhere', :zip=>'12345'}}])
        o.employees.should == [e]
        o = Company.create(:id=>1, :employees=>[e])
        o.employees.should == [e]
      end
    end
  end
end
