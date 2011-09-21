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
POSTGRES_DB.loggers = [logger]

#POSTGRES_DB.instance_variable_set(:@server_version, 80100)
POSTGRES_DB.create_table! :test do
  text :name
  integer :value, :index => true
end
POSTGRES_DB.create_table! :test2 do
  text :name
  integer :value
end
POSTGRES_DB.create_table! :test3 do
  integer :value
  timestamp :time
end
POSTGRES_DB.create_table! :test4 do
  varchar :name, :size => 20
  bytea :value
end

describe "A PostgreSQL database" do
  before do
    @db = POSTGRES_DB
  end
  
  specify "should provide the server version" do
    @db.server_version.should > 70000
  end

  specify "should correctly parse the schema" do
    @db.schema(:test3, :reload=>true).should == [
      [:value, {:type=>:integer, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"integer", :primary_key=>false}],
      [:time, {:type=>:datetime, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"timestamp without time zone", :primary_key=>false}]
    ]
    @db.schema(:test4, :reload=>true).should == [
      [:name, {:type=>:string, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"character varying(20)", :primary_key=>false}],
      [:value, {:type=>:blob, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"bytea", :primary_key=>false}]
    ]
  end
end

describe "A PostgreSQL dataset" do
  before do
    @d = POSTGRES_DB[:test]
    @d.delete # remove all records
  end
  
  specify "should quote columns and tables using double quotes if quoting identifiers" do
    @d.quote_identifiers = true
    @d.select(:name).sql.should == \
      'SELECT "name" FROM "test"'
      
    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM "test"'

    @d.select(:max.sql_function(:value)).sql.should == \
      'SELECT max("value") FROM "test"'
      
    @d.select(:NOW.sql_function).sql.should == \
    'SELECT NOW() FROM "test"'

    @d.select(:max.sql_function(:items__value)).sql.should == \
      'SELECT max("items"."value") FROM "test"'

    @d.order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.select('test.name AS item_name'.lit).sql.should == \
      'SELECT test.name AS item_name FROM "test"'
      
    @d.select('"name"'.lit).sql.should == \
      'SELECT "name" FROM "test"'

    @d.select('max(test."name") AS "max_name"'.lit).sql.should == \
      'SELECT max(test."name") AS "max_name" FROM "test"'
      
    @d.select(:test.sql_function(:abc, 'hello')).sql.should == \
      "SELECT test(\"abc\", 'hello') FROM \"test\""

    @d.select(:test.sql_function(:abc__def, 'hello')).sql.should == \
      "SELECT test(\"abc\".\"def\", 'hello') FROM \"test\""

    @d.select(:test.sql_function(:abc__def, 'hello').as(:x2)).sql.should == \
      "SELECT test(\"abc\".\"def\", 'hello') AS \"x2\" FROM \"test\""

    @d.insert_sql(:value => 333).should =~ \
      /\AINSERT INTO "test" \("value"\) VALUES \(333\)( RETURNING NULL)?\z/

    @d.insert_sql(:x => :y).should =~ \
      /\AINSERT INTO "test" \("x"\) VALUES \("y"\)( RETURNING NULL)?\z/

    @d.disable_insert_returning.insert_sql(:value => 333).should =~ \
      /\AINSERT INTO "test" \("value"\) VALUES \(333\)\z/
  end
  
  specify "should quote fields correctly when reversing the order if quoting identifiers" do
    @d.quote_identifiers = true
    @d.reverse_order(:name).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.reverse_order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" ASC'

    @d.reverse_order(:name, :test.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC'

    @d.reverse_order(:name.desc, :test).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC'
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
    @d.order(:value.asc(:nulls=>:first), :name).select_map(:name).should == %w[abc bcd bcd]
    @d.order(:value.asc(:nulls=>:last), :name).select_map(:name).should == %w[bcd abc bcd]
    @d.order(:value.asc(:nulls=>:first), :name).reverse.select_map(:name).should == %w[bcd bcd abc]
  end
  
  specify "#lock should lock tables and yield if a block is given" do
    @d.lock('EXCLUSIVE'){@d.insert(:name=>'a')}
  end
  
  specify "#lock should lock table if inside a transaction" do
    POSTGRES_DB.transaction{@d.lock('EXCLUSIVE'); @d.insert(:name=>'a')}
  end
  
  specify "#lock should return nil" do
    @d.lock('EXCLUSIVE'){@d.insert(:name=>'a')}.should == nil
    POSTGRES_DB.transaction{@d.lock('EXCLUSIVE').should == nil; @d.insert(:name=>'a')}
  end
  
  specify "should raise an error if attempting to update a joined dataset with a single FROM table" do
    proc{POSTGRES_DB[:test].join(:test2, [:name]).update(:name=>'a')}.should raise_error(Sequel::Error, 'Need multiple FROM tables if updating/deleting a dataset with JOINs')
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
    @db.drop_table(:a)
  end
  
  it "#distinct with arguments should return results distinct on those arguments" do
    @ds.insert(20, 10)
    @ds.insert(30, 10)
    @ds.order(:b, :a).distinct.map(:a).should == [20, 30]
    @ds.order(:b, :a.desc).distinct.map(:a).should == [30, 20]
    @ds.order(:b, :a).distinct(:b).map(:a).should == [20]
    @ds.order(:b, :a.desc).distinct(:b).map(:a).should == [30]
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
      POSTGRES_DB.drop_table(:items)
      POSTGRES_DB.disconnect
    end
    
    specify "should handle FOR UPDATE" do
      @ds.insert(:number=>20)
      c = nil
      t = nil
      POSTGRES_DB.transaction do
        @ds.for_update.first(:id=>1)
        t = Thread.new do
          POSTGRES_DB.transaction do
            @ds.filter(:id=>1).update(:name=>'Jim')
            c = @ds.first(:id=>1)
          end
        end
        sleep 0.01
        @ds.filter(:id=>1).update(:number=>30)
      end
      t.join
      c.should == {:id=>1, :number=>30, :name=>'Jim'}
    end
  
    specify "should handle FOR SHARE" do
      @ds.insert(:number=>20)
      c = nil
      t = nil
      POSTGRES_DB.transaction do
        @ds.for_share.first(:id=>1)
        t = Thread.new do
          POSTGRES_DB.transaction do
            c = @ds.for_share.filter(:id=>1).first
          end
        end
        sleep 0.1
        @ds.filter(:id=>1).update(:name=>'Jim')
        c.should == {:id=>1, :number=>20, :name=>nil}
      end
      t.join
    end
  end
end

describe "A PostgreSQL dataset with a timestamp field" do
  before do
    @d = POSTGRES_DB[:test3]
    @d.delete
  end

  cspecify "should store milliseconds in time fields for Time objects", :do do
    t = Time.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>'1'][:time]
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000 == t.usec
  end

  cspecify "should store milliseconds in time fields for DateTime objects", :do do
    t = DateTime.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>'1'][:time]
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000 == t.strftime('%N').to_i/1000
  end
end

describe "PostgreSQL's EXPLAIN and ANALYZE" do
  specify "should not raise errors" do
    @d = POSTGRES_DB[:test3]
    proc{@d.explain}.should_not raise_error
    proc{@d.analyze}.should_not raise_error
  end
end

describe "A PostgreSQL database" do
  before do
    @db = POSTGRES_DB
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
  
  specify "#locks should be a dataset returning database locks " do
    @db.locks.should be_a_kind_of(Sequel::Dataset)
    @db.locks.all.should be_a_kind_of(Array)
  end
end  

describe "A PostgreSQL database" do
  before do
    @db = POSTGRES_DB
    @db.drop_table(:posts) rescue nil
    @db.sqls.clear
  end
  after do
    @db.drop_table(:posts) rescue nil
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
    @db.sqls.should == [
    "CREATE TABLE posts (title text, body text, user_id integer)",
    "CREATE INDEX posts_user_id_index ON posts USING btree (user_id int4_ops)"
    ]
  end

  specify "should support fulltext indexes and searching" do
    @db.create_table(:posts){text :title; text :body; full_text_index [:title, :body]; full_text_index :title, :language => 'french'}
    @db.sqls.should == [
      "CREATE TABLE posts (title text, body text)",
      "CREATE INDEX posts_title_body_index ON posts USING gin (to_tsvector('simple', (COALESCE(title, '') || ' ' || COALESCE(body, ''))))",
      "CREATE INDEX posts_title_index ON posts USING gin (to_tsvector('french', (COALESCE(title, ''))))"
    ]

    @db[:posts].insert(:title=>'ruby rails', :body=>'yowsa')
    @db[:posts].insert(:title=>'sequel', :body=>'ruby')
    @db[:posts].insert(:title=>'ruby scooby', :body=>'x')
    @db.sqls.clear

    @db[:posts].full_text_search(:title, 'rails').all.should == [{:title=>'ruby rails', :body=>'yowsa'}]
    @db[:posts].full_text_search([:title, :body], ['yowsa', 'rails']).all.should == [:title=>'ruby rails', :body=>'yowsa']
    @db[:posts].full_text_search(:title, 'scooby', :language => 'french').all.should == [{:title=>'ruby scooby', :body=>'x'}]
    @db.sqls.should == [
      "SELECT * FROM posts WHERE (to_tsvector('simple', (COALESCE(title, ''))) @@ to_tsquery('simple', 'rails'))",
      "SELECT * FROM posts WHERE (to_tsvector('simple', (COALESCE(title, '') || ' ' || COALESCE(body, ''))) @@ to_tsquery('simple', 'yowsa | rails'))",
      "SELECT * FROM posts WHERE (to_tsvector('french', (COALESCE(title, ''))) @@ to_tsquery('french', 'scooby'))"]
  end

  specify "should support spatial indexes" do
    @db.create_table(:posts){box :geom; spatial_index [:geom]}
    @db.sqls.should == [
      "CREATE TABLE posts (geom box)",
      "CREATE INDEX posts_geom_index ON posts USING gist (geom)"
    ]
  end
  
  specify "should support indexes with index type" do
    @db.create_table(:posts){varchar :title, :size => 5; index :title, :type => 'hash'}
    @db.sqls.should == [
      "CREATE TABLE posts (title varchar(5))",
      "CREATE INDEX posts_title_index ON posts USING hash (title)"
    ]
  end
  
  specify "should support unique indexes with index type" do
    @db.create_table(:posts){varchar :title, :size => 5; index :title, :type => 'btree', :unique => true}
    @db.sqls.should == [
      "CREATE TABLE posts (title varchar(5))",
      "CREATE UNIQUE INDEX posts_title_index ON posts USING btree (title)"
    ]
  end
  
  specify "should support partial indexes" do
    @db.create_table(:posts){varchar :title, :size => 5; index :title, :where => {:title => '5'}}
    @db.sqls.should == [
      "CREATE TABLE posts (title varchar(5))",
      "CREATE INDEX posts_title_index ON posts (title) WHERE (title = '5')"
    ]
  end
  
  specify "should support identifiers for table names in indicies" do
    @db.create_table(Sequel::SQL::Identifier.new(:posts)){varchar :title, :size => 5; index :title, :where => {:title => '5'}}
    @db.sqls.should == [
      "CREATE TABLE posts (title varchar(5))",
      "CREATE INDEX posts_title_index ON posts (title) WHERE (title = '5')"
    ]
  end
  
  specify "should support renaming tables" do
    @db.create_table!(:posts1){primary_key :a}
    @db.rename_table(:posts1, :posts)
  end
end

describe "Postgres::Dataset#import" do
  before do
    @db = POSTGRES_DB
    @db.create_table!(:test){Integer :x; Integer :y}
    @db.sqls.clear
    @ds = @db[:test]
  end
  after do
    @db.drop_table(:test) rescue nil
  end
  
  specify "#import should return separate insert statements if server_version < 80200" do
    @ds.meta_def(:server_version){80199}
    
    @ds.import([:x, :y], [[1, 2], [3, 4]])
    
    @db.sqls.should == [
      'BEGIN',
      'INSERT INTO test (x, y) VALUES (1, 2)',
      'INSERT INTO test (x, y) VALUES (3, 4)',
      'COMMIT'
    ]
    @ds.all.should == [{:x=>1, :y=>2}, {:x=>3, :y=>4}]
  end
  
  specify "#import should a single insert statement if server_version >= 80200" do
    @ds.meta_def(:server_version){80200}
    
    @ds.import([:x, :y], [[1, 2], [3, 4]])
    
    @db.sqls.should == [
      'BEGIN',
      'INSERT INTO test (x, y) VALUES (1, 2), (3, 4)',
      'COMMIT'
    ]
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
    @db.drop_table(:test5) rescue nil
  end

  specify "should work with static SQL" do
    @ds.with_sql('INSERT INTO test5 (value) VALUES (10)').insert.should == nil
    @db['INSERT INTO test5 (value) VALUES (20)'].insert.should == nil
    @ds.all.should == [{:xid=>1, :value=>10}, {:xid=>2, :value=>20}]
  end

  specify "should work regardless of how it is used" do
    @ds.insert(:value=>10).should == 1
    @ds.disable_insert_returning.insert(:value=>20).should == 2
    @ds.meta_def(:server_version){80100}
    @ds.insert(:value=>13).should == 3
    
    @db.sqls.reject{|x| x =~ /pg_class/}.should == [
      'INSERT INTO test5 (value) VALUES (10) RETURNING xid',
      'INSERT INTO test5 (value) VALUES (20)',
      "SELECT currval('\"public\".test5_xid_seq')",
      'INSERT INTO test5 (value) VALUES (13)',
      "SELECT currval('\"public\".test5_xid_seq')"
    ]
    @ds.all.should == [{:xid=>1, :value=>10}, {:xid=>2, :value=>20}, {:xid=>3, :value=>13}]
  end
  
  specify "should call execute_insert if server_version < 80200" do
    @ds.meta_def(:server_version){80100}
    @ds.should_receive(:execute_insert).once.with('INSERT INTO test5 (value) VALUES (10)', :table=>:test5, :values=>{:value=>10})
    @ds.insert(:value=>10)
  end

  specify "should call execute_insert if disabling insert returning" do
    @ds.disable_insert_returning!
    @ds.should_receive(:execute_insert).once.with('INSERT INTO test5 (value) VALUES (10)', :table=>:test5, :values=>{:value=>10})
    @ds.insert(:value=>10)
  end

  specify "should use INSERT RETURNING if server_version >= 80200" do
    @ds.meta_def(:server_version){80201}
    @ds.insert(:value=>10)
    @db.sqls.last.should == 'INSERT INTO test5 (value) VALUES (10) RETURNING xid'
  end

  specify "should have insert_select return nil if server_version < 80200" do
    @ds.meta_def(:server_version){80100}
    @ds.insert_select(:value=>10).should == nil
  end

  specify "should have insert_select return nil if disable_insert_returning is used" do
    @ds.disable_insert_returning.insert_select(:value=>10).should == nil
  end

  specify "should have insert_select insert the record and return the inserted record if server_version >= 80200" do
    @ds.meta_def(:server_version){80201}
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
    ds = POSTGRES_DB[:test4]
    ds.delete
    ds.insert(:name=>'a').should == nil
  end
end

describe "Postgres::Database schema qualified tables" do
  before do
    POSTGRES_DB << "CREATE SCHEMA schema_test"
    POSTGRES_DB.instance_variable_set(:@primary_keys, {})
    POSTGRES_DB.instance_variable_set(:@primary_key_sequences, {})
  end
  after do
    POSTGRES_DB.quote_identifiers = false
    POSTGRES_DB << "DROP SCHEMA schema_test CASCADE"
    POSTGRES_DB.default_schema = nil
  end
  
  specify "should be able to create, drop, select and insert into tables in a given schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){primary_key :i}
    POSTGRES_DB[:schema_test__schema_test].first.should == nil
    POSTGRES_DB[:schema_test__schema_test].insert(:i=>1).should == 1
    POSTGRES_DB[:schema_test__schema_test].first.should == {:i=>1}
    POSTGRES_DB.from('schema_test.schema_test'.lit).first.should == {:i=>1}
    POSTGRES_DB.drop_table(:schema_test__schema_test)
    POSTGRES_DB.create_table(:schema_test.qualify(:schema_test)){integer :i}
    POSTGRES_DB[:schema_test__schema_test].first.should == nil
    POSTGRES_DB.from('schema_test.schema_test'.lit).first.should == nil
    POSTGRES_DB.drop_table(:schema_test.qualify(:schema_test))
  end
  
  specify "#tables should not include tables in a default non-public schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :i}
    POSTGRES_DB.tables.should include(:schema_test)
    POSTGRES_DB.tables.should_not include(:pg_am)
    POSTGRES_DB.tables.should_not include(:domain_udt_usage)
  end
  
  specify "#tables should return tables in the schema provided by the :schema argument" do
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :i}
    POSTGRES_DB.tables(:schema=>:schema_test).should == [:schema_test]
  end
  
  specify "#schema should not include columns from tables in a default non-public schema" do
    POSTGRES_DB.create_table(:schema_test__domains){integer :i}
    sch = POSTGRES_DB.schema(:domains)
    cs = sch.map{|x| x.first}
    cs.should include(:i)
    cs.should_not include(:data_type)
  end
  
  specify "#schema should only include columns from the table in the given :schema argument" do
    POSTGRES_DB.create_table!(:domains){integer :d}
    POSTGRES_DB.create_table(:schema_test__domains){integer :i}
    sch = POSTGRES_DB.schema(:domains, :schema=>:schema_test)
    cs = sch.map{|x| x.first}
    cs.should include(:i)
    cs.should_not include(:d)
    POSTGRES_DB.drop_table(:domains)
  end
  
  specify "#table_exists? should see if the table is in a given schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :i}
    POSTGRES_DB.table_exists?(:schema_test__schema_test).should == true
  end
  
  specify "should be able to get primary keys for tables in a given schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){primary_key :i}
    POSTGRES_DB.primary_key(:schema_test__schema_test).should == 'i'
  end
  
  specify "should be able to get serial sequences for tables in a given schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){primary_key :i}
    POSTGRES_DB.primary_key_sequence(:schema_test__schema_test).should == '"schema_test".schema_test_i_seq'
  end
  
  specify "should be able to get serial sequences for tables that have spaces in the name in a given schema" do
    POSTGRES_DB.quote_identifiers = true
    POSTGRES_DB.create_table(:"schema_test__schema test"){primary_key :i}
    POSTGRES_DB.primary_key_sequence(:"schema_test__schema test").should == '"schema_test"."schema test_i_seq"'
  end
  
  specify "should be able to get custom sequences for tables in a given schema" do
    POSTGRES_DB << "CREATE SEQUENCE schema_test.kseq"
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :j; primary_key :k, :type=>:integer, :default=>"nextval('schema_test.kseq'::regclass)".lit}
    POSTGRES_DB.primary_key_sequence(:schema_test__schema_test).should == '"schema_test".kseq'
  end
  
  specify "should be able to get custom sequences for tables that have spaces in the name in a given schema" do
    POSTGRES_DB.quote_identifiers = true
    POSTGRES_DB << "CREATE SEQUENCE schema_test.\"ks eq\""
    POSTGRES_DB.create_table(:"schema_test__schema test"){integer :j; primary_key :k, :type=>:integer, :default=>"nextval('schema_test.\"ks eq\"'::regclass)".lit}
    POSTGRES_DB.primary_key_sequence(:"schema_test__schema test").should == '"schema_test"."ks eq"'
  end
  
  specify "#default_schema= should change the default schema used from public" do
    POSTGRES_DB.create_table(:schema_test__schema_test){primary_key :i}
    POSTGRES_DB.default_schema = :schema_test
    POSTGRES_DB.table_exists?(:schema_test).should == true
    POSTGRES_DB.tables.should == [:schema_test]
    POSTGRES_DB.primary_key(:schema_test__schema_test).should == 'i'
    POSTGRES_DB.primary_key_sequence(:schema_test__schema_test).should == '"schema_test".schema_test_i_seq'
  end
end

describe "Postgres::Database schema qualified tables and eager graphing" do
  before(:all) do
    @db = POSTGRES_DB
    @db.run "DROP SCHEMA s CASCADE" rescue nil
    @db.run "CREATE SCHEMA s"
    @db.quote_identifiers = true
    
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
    @db.quote_identifiers = false
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
    bands = @Band.order(:bands__name).select(:s__bands.*).join(:s__members, :band_id=>:id).from_self(:alias=>:bands0).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.albums.map{|y| y.tracks}}.should == [[[@t1, @t2], [@t3, @t4]], [[], []]]
  end

  specify "should have eager graphs work with joins with the same tables" do
    bands = @Band.order(:bands__name).select(:s__bands.*).join(:s__members, :band_id=>:id).eager_graph({:albums=>:tracks}, :members).all
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

    bands = @Band.order(:tracks__name).from(:s__bands.as(:tracks)).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:tracks__name).from(:s__bands.as(:tracks.identifier)).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:tracks__name).from(:s__bands.as('tracks')).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]
  end

  specify "should have eager graphs work with join tables with aliases" do
    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums___tracks, :band_id=>:id.qualify(:s__bands)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums.as(:tracks), :band_id=>:id.qualify(:s__bands)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums.as('tracks'), :band_id=>:id.qualify(:s__bands)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums.as(:tracks.identifier), :band_id=>:id.qualify(:s__bands)).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums, {:band_id=>:id.qualify(:s__bands)}, :table_alias=>:tracks).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums, {:band_id=>:id.qualify(:s__bands)}, :table_alias=>'tracks').eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]

    bands = @Band.order(:bands__name).eager_graph(:members).join(:s__albums, {:band_id=>:id.qualify(:s__bands)}, :table_alias=>:tracks.identifier).eager_graph(:albums=>:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.albums}.should == [[@a1, @a2], [@a3, @a4]]
    bands.map{|x| x.members}.should == [[@m1, @m2], [@m4, @m3]]
  end

  specify "should have eager graphs work with different types of qualified from tables" do
    bands = @Band.order(:bands__name).from(:bands.qualify(:s)).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:bands__name).from(:bands.identifier.qualify(:s)).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]

    bands = @Band.order(:bands__name).from(Sequel::SQL::QualifiedIdentifier.new(:s, 'bands')).eager_graph(:tracks).all
    bands.should == [@b1, @b2]
    bands.map{|x| x.tracks}.should == [[@t1, @t2, @t3, @t4], []]
  end

end

if POSTGRES_DB.server_version >= 80300

  POSTGRES_DB.create_table! :test6 do
    text :title
    text :body
    full_text_index [:title, :body]
  end

  describe "PostgreSQL tsearch2" do
    before do
      @ds = POSTGRES_DB[:test6]
    end
    after do
      POSTGRES_DB[:test6].delete
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
      @db.drop_table(:i1)
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

describe "Postgres::Database functions, languages, and triggers" do
  before do
    @d = POSTGRES_DB
  end
  after do
    @d.drop_function('tf', :if_exists=>true, :cascade=>true)
    @d.drop_function('tf', :if_exists=>true, :cascade=>true, :args=>%w'integer integer')
    @d.drop_language(:plpgsql, :if_exists=>true, :cascade=>true) if @d.server_version < 90000
    @d.drop_table(:test) rescue nil
  end

  specify "#create_function and #drop_function should create and drop functions" do
    proc{@d['SELECT tf()'].all}.should raise_error(Sequel::DatabaseError)
    args = ['tf', 'SELECT 1', {:returns=>:integer}]
    @d.send(:create_function_sql, *args).should =~ /\A\s*CREATE FUNCTION tf\(\)\s+RETURNS integer\s+LANGUAGE SQL\s+AS 'SELECT 1'\s*\z/
    @d.create_function(*args)
    rows = @d['SELECT tf()'].all.should == [{:tf=>1}]
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
    rows = @d['SELECT tf(1, 2)'].all.should == [{:tf=>3}]
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
  
  specify "#create_trigger and #drop_trigger should create and drop triggers" do
    @d.create_language(:plpgsql) if @d.server_version < 90000
    @d.create_function(:tf, 'BEGIN IF NEW.value IS NULL THEN RAISE EXCEPTION \'Blah\'; END IF; RETURN NEW; END;', :language=>:plpgsql, :returns=>:trigger)
    @d.send(:create_trigger_sql, :test, :identity, :tf, :each_row=>true).should == 'CREATE TRIGGER identity BEFORE INSERT OR UPDATE OR DELETE ON test FOR EACH ROW EXECUTE PROCEDURE tf()'
    @d.create_table(:test){String :name; Integer :value}
    @d.create_trigger(:test, :identity, :tf, :each_row=>true)
    @d[:test].insert(:name=>'a', :value=>1)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>1}]
    proc{@d[:test].filter(:name=>'a').update(:value=>nil)}.should raise_error(Sequel::DatabaseError)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>1}]
    @d[:test].filter(:name=>'a').update(:value=>3)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>3}]
    @d.send(:drop_trigger_sql, :test, :identity).should == 'DROP TRIGGER identity ON test'
    @d.drop_trigger(:test, :identity)
    @d.send(:create_trigger_sql, :test, :identity, :tf, :after=>true, :events=>:insert, :args=>[1, 'a']).should == 'CREATE TRIGGER identity AFTER INSERT ON test EXECUTE PROCEDURE tf(1, \'a\')'
    @d.send(:drop_trigger_sql, :test, :identity, :if_exists=>true, :cascade=>true).should == 'DROP TRIGGER IF EXISTS identity ON test CASCADE'
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
      @db.drop_table(:test_cursor) rescue nil
    end
  
    specify "should return the same results as the non-cursor use" do
      @ds.all.should == @ds.use_cursor.all
    end
    
    specify "should respect the :rows_per_fetch option" do
      @db.sqls.clear
      @ds.use_cursor.all
      @db.sqls.length.should == 6
      @db.sqls.clear
      @ds.use_cursor(:rows_per_fetch=>100).all
      @db.sqls.length.should == 15
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
      @db.instance_eval do
        disconnect
        @conversion_procs = nil
      end 
    end
    after do
      Sequel::Postgres::PG_NAMED_TYPES.delete(:interval)
      @db.instance_eval do
        disconnect
        @conversion_procs = nil
      end
      @db.drop_table(:foo)
    end

    specify "should look up conversion procs by name" do
      @db.create_table!(:foo){interval :bar}
      @db[:foo].insert('21 days')
      @db[:foo].get(:bar).should == 'syad 12'
    end
  end
end

if POSTGRES_DB.adapter_scheme == :postgres && SEQUEL_POSTGRES_USES_PG && POSTGRES_DB.server_version >= 90000
  describe "Postgres::Database#copy_table" do
    before(:all) do
      @db = POSTGRES_DB
      @db.create_table!(:test_copy){Integer :x; Integer :y}
      ds = @db[:test_copy]
      ds.insert(1, 2)
      ds.insert(3, 4)
    end
    after(:all) do
      @db.drop_table(:test_copy) rescue nil
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
      @db.copy_table(@db[:test_copy].cross_join(:test_copy___tc).order(1, 2, 3, 4)).should == "1\t2\t1\t2\n1\t2\t3\t4\n3\t4\t1\t2\n3\t4\t3\t4\n"
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
