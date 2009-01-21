require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(POSTGRES_DB)
  POSTGRES_URL = 'postgres://postgres:postgres@localhost:5432/reality_spec' unless defined? POSTGRES_URL
  POSTGRES_DB = Sequel.connect(ENV['SEQUEL_PG_SPEC_DB']||POSTGRES_URL)
end

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
POSTGRES_DB.create_table! :test5 do
  primary_key :xid
  integer :value
end

context "A PostgreSQL database" do
  setup do
    @db = POSTGRES_DB
  end
  
  specify "should provide disconnect functionality" do
    @db.tables
    @db.pool.size.should == 1
    @db.disconnect
    @db.pool.size.should == 0
  end
  
  specify "should provide the server version" do
    @db.server_version.should > 70000
  end

  specify "should raise Sequel::Error on error" do
    proc{@db << "SELECT 1 + 'a'"}.should raise_error(Sequel::Error)
  end

  specify "should correctly parse the schema" do
    require 'logger'
    @db.schema(:test3, :reload=>true).should == [
      [:value, {:type=>:integer, :allow_null=>true, :default=>nil, :db_type=>"integer", :primary_key=>false}],
      [:time, {:type=>:datetime, :allow_null=>true, :default=>nil, :db_type=>"timestamp without time zone", :primary_key=>false}]
    ]
    @db.schema(:test4, :reload=>true).should == [
      [:name, {:type=>:string, :allow_null=>true, :default=>nil, :db_type=>"character varying(20)", :primary_key=>false}],
      [:value, {:type=>:blob, :allow_null=>true, :default=>nil, :db_type=>"bytea", :primary_key=>false}]
    ]
  end

  specify "should get the schema all database tables if no table name is used" do
    @db.schema(:test3, :reload=>true).should == @db.schema(nil, :reload=>true)[:test3]
  end
end

context "A PostgreSQL dataset" do
  setup do
    @d = POSTGRES_DB[:test]
    @d.delete # remove all records
  end
  
  specify "should return the correct record count" do
    @d.count.should == 0
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.count.should == 3
  end
  
  specify "should return the correct records" do
    @d.to_a.should == []
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}

    @d.order(:value).to_a.should == [
      {:name => 'abc', :value => 123},
      {:name => 'abc', :value => 456},
      {:name => 'def', :value => 789}
    ]
  end
  
  specify "should update records correctly" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter(:name => 'abc').update(:value => 530)
    
    # the third record should stay the same
    # floating-point precision bullshit
    @d[:name => 'def'][:value].should == 789
    @d.filter(:value => 530).count.should == 2
  end
  
  specify "should delete records correctly" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter(:name => 'abc').delete
    
    @d.count.should == 1
    @d.first[:name].should == 'def'
  end
  
  specify "should be able to literalize booleans" do
    proc {@d.literal(true)}.should_not raise_error
    proc {@d.literal(false)}.should_not raise_error
  end
  
  specify "should quote columns and tables using double quotes if quoting identifiers" do
    @d.quote_identifiers = true
    @d.select(:name).sql.should == \
      'SELECT "name" FROM "test"'
      
    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM "test"'

    @d.select(:max[:value]).sql.should == \
      'SELECT max("value") FROM "test"'
      
    @d.select(:NOW[]).sql.should == \
    'SELECT NOW() FROM "test"'

    @d.select(:max[:items__value]).sql.should == \
      'SELECT max("items"."value") FROM "test"'

    @d.order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.select('test.name AS item_name'.lit).sql.should == \
      'SELECT test.name AS item_name FROM "test"'
      
    @d.select('"name"'.lit).sql.should == \
      'SELECT "name" FROM "test"'

    @d.select('max(test."name") AS "max_name"'.lit).sql.should == \
      'SELECT max(test."name") AS "max_name" FROM "test"'
      
    @d.select(:test[:abc, 'hello']).sql.should == \
      "SELECT test(\"abc\", 'hello') FROM \"test\""

    @d.select(:test[:abc__def, 'hello']).sql.should == \
      "SELECT test(\"abc\".\"def\", 'hello') FROM \"test\""

    @d.select(:test[:abc__def, 'hello'].as(:x2)).sql.should == \
      "SELECT test(\"abc\".\"def\", 'hello') AS \"x2\" FROM \"test\""

    @d.insert_sql(:value => 333).should =~ \
      /\AINSERT INTO "test" \("value"\) VALUES \(333\)( RETURNING NULL)?\z/

    @d.insert_sql(:x => :y).should =~ \
      /\AINSERT INTO "test" \("x"\) VALUES \("y"\)( RETURNING NULL)?\z/
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
  
  specify "should support transactions" do
    POSTGRES_DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.should == 1
  end
  
  specify "should have #transaction yield the connection" do
    POSTGRES_DB.transaction do |conn|
      conn.should_not == nil
    end
  end
  
  specify "should correctly rollback transactions" do
    proc do
      POSTGRES_DB.transaction do
        @d << {:name => 'abc', :value => 1}
        raise Interrupt, 'asdf'
      end
    end.should raise_error(Interrupt)

    @d.count.should == 0
  end
  
  specify "should handle returning inside of the block by committing" do
    def POSTGRES_DB.ret_commit
      transaction do
        self[:test] << {:name => 'abc'}
        return
        self[:test] << {:name => 'd'}
      end
    end
    @d.count.should == 0
    POSTGRES_DB.ret_commit
    @d.count.should == 1
    POSTGRES_DB.ret_commit
    @d.count.should == 2
    proc do
      POSTGRES_DB.transaction do
        raise Interrupt, 'asdf'
      end
    end.should raise_error(Interrupt)

    @d.count.should == 2
  end

  specify "should support nested transactions through savepoints" do
    POSTGRES_DB.transaction do
      @d << {:name => '1'}
      POSTGRES_DB.transaction do
        @d << {:name => '2'}
        POSTGRES_DB.transaction do
          @d << {:name => '3'}
          raise Sequel::Error::Rollback
        end
        @d << {:name => '4'}
        POSTGRES_DB.transaction do
          @d << {:name => '6'}
          POSTGRES_DB.transaction do
            @d << {:name => '7'}
          end
          raise Sequel::Error::Rollback
        end
        @d << {:name => '5'}
      end
    end

    @d.count.should == 4
    @d.order(:name).map(:name).should == %w{1 2 4 5}
  end

  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}
    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end

  specify "should correctly escape strings" do
    POSTGRES_DB['SELECT ? AS a', "\\dingo"].get(:a) == "\\dingo"
  end

  specify "should properly escape binary data" do
    POSTGRES_DB['SELECT ? AS a', "\1\2\3".to_blob].get(:a) == "\1\2\3"
  end

  specify "should retrieve binary data as Blob object" do
    d = POSTGRES_DB[:test4]
    d << {:name => '123', :value => "\1\2\3".to_blob}
    retrieved_binary_value = d[:name => '123'][:value]
    retrieved_binary_value.should be_a_kind_of(::Sequel::SQL::Blob)
    retrieved_binary_value.should == "\1\2\3"
    retrieved_binary_value = d[:value => "\1\2\3".to_blob][:value]
    retrieved_binary_value.should be_a_kind_of(::Sequel::SQL::Blob)
    retrieved_binary_value.should == "\1\2\3"
  end

  specify "should properly receive binary data" do
    POSTGRES_DB['SELECT ?::bytea AS a', "a"].get(:a) == "a"
  end
end

context "A PostgreSQL dataset with a timestamp field" do
  setup do
    @d = POSTGRES_DB[:test3]
    @d.delete
  end

  specify "should store milliseconds in time fields" do
    t = Time.now
    @d << {:value=>1, :time=>t}
    @d.literal(@d[:value =>'1'][:time]).should == @d.literal(t)
    @d[:value=>'1'][:time].usec.should == t.usec
  end
end

context "A PostgreSQL database" do
  setup do
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
end  

context "A PostgreSQL database" do
  setup do
  end
  
  specify "should support fulltext indexes" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      text :title
      text :body
      full_text_index [:title, :body]
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE public.posts (title text, body text)",
      "CREATE INDEX posts_title_body_index ON public.posts USING gin (to_tsvector('simple', (COALESCE(title, '') || ' ' || COALESCE(body, ''))))"
    ]
  end
  
  specify "should support fulltext indexes with a specific language" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      text :title
      text :body
      full_text_index [:title, :body], :language => 'french'
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE public.posts (title text, body text)",
      "CREATE INDEX posts_title_body_index ON public.posts USING gin (to_tsvector('french', (COALESCE(title, '') || ' ' || COALESCE(body, ''))))"
    ]
  end
  
  specify "should support full_text_search" do
    POSTGRES_DB[:posts].full_text_search(:title, 'ruby').sql.should ==
      "SELECT * FROM posts WHERE (to_tsvector('simple', (COALESCE(title, ''))) @@ to_tsquery('simple', 'ruby'))"
    
    POSTGRES_DB[:posts].full_text_search([:title, :body], ['ruby', 'sequel']).sql.should ==
      "SELECT * FROM posts WHERE (to_tsvector('simple', (COALESCE(title, '') || ' ' || COALESCE(body, ''))) @@ to_tsquery('simple', 'ruby | sequel'))"
      
    POSTGRES_DB[:posts].full_text_search(:title, 'ruby', :language => 'french').sql.should ==
      "SELECT * FROM posts WHERE (to_tsvector('french', (COALESCE(title, ''))) @@ to_tsquery('french', 'ruby'))"
  end

  specify "should support spatial indexes" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      geometry :geom
      spatial_index [:geom]
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE public.posts (geom geometry)",
      "CREATE INDEX posts_geom_index ON public.posts USING gist (geom)"
    ]
  end
  
  specify "should support indexes with index type" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      varchar :title, :size => 5
      index :title, :type => 'hash'
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE public.posts (title varchar(5))",
      "CREATE INDEX posts_title_index ON public.posts USING hash (title)"
    ]
  end
  
  specify "should support unique indexes with index type" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      varchar :title, :size => 5
      index :title, :type => 'hash', :unique => true
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE public.posts (title varchar(5))",
      "CREATE UNIQUE INDEX posts_title_index ON public.posts USING hash (title)"
    ]
  end
  
  specify "should support partial indexes" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      varchar :title, :size => 5
      index :title, :where => {:something => 5}
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE public.posts (title varchar(5))",
      "CREATE INDEX posts_title_index ON public.posts (title) WHERE (something = 5)"
    ]
  end
  
  specify "should support identifiers for table names in indicies" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      varchar :title, :size => 5
      index :title, :where => {:something => 5}
    end
    POSTGRES_DB.create_table_sql_list(Sequel::SQL::Identifier.new(:posts__test), *g.create_info).should == [
      "CREATE TABLE public.posts__test (title varchar(5))",
      "CREATE INDEX posts__test_title_index ON public.posts__test (title) WHERE (something = 5)"
    ]
  end
end

context "Postgres::Dataset#multi_insert_sql / #import" do
  setup do
    @ds = POSTGRES_DB[:test]
  end
  
  specify "should return separate insert statements if server_version < 80200" do
    @ds.meta_def(:server_version){80199}
    
    @ds.multi_insert_sql([:x, :y], [[1, 2], [3, 4]]).should == [
      'INSERT INTO test (x, y) VALUES (1, 2)',
      'INSERT INTO test (x, y) VALUES (3, 4)'
    ]
  end
  
  specify "should a single insert statement if server_version >= 80200" do
    @ds.meta_def(:server_version){80200}
   
    @ds.multi_insert_sql([:x, :y], [[1, 2], [3, 4]]).should == [
      'INSERT INTO test (x, y) VALUES (1, 2), (3, 4)'
    ]

    @ds.meta_def(:server_version){80201}
    
    @ds.multi_insert_sql([:x, :y], [[1, 2], [3, 4]]).should == [
      'INSERT INTO test (x, y) VALUES (1, 2), (3, 4)'
    ]
  end
end

context "Postgres::Dataset#insert" do
  setup do
    @ds = POSTGRES_DB[:test5]
    @ds.delete
  end
  
  specify "should call insert_sql if server_version < 80200" do
    @ds.meta_def(:server_version){80100}
    @ds.should_receive(:execute_insert).once.with('INSERT INTO test5 (value) VALUES (10)', :table=>:test5, :values=>{:value=>10})
    @ds.insert(:value=>10)
  end

  specify "should using call insert_returning_sql if server_version >= 80200" do
    @ds.meta_def(:server_version){80201}
    @ds.should_receive(:single_value).once.with(:sql=>'INSERT INTO test5 (value) VALUES (10) RETURNING xid')
    @ds.insert(:value=>10)
  end

  specify "should have insert_returning_sql use the RETURNING keyword" do
    @ds.insert_returning_sql(:xid, :value=>10).should == "INSERT INTO test5 (value) VALUES (10) RETURNING xid"
    @ds.insert_returning_sql('*'.lit, :value=>10).should == "INSERT INTO test5 (value) VALUES (10) RETURNING *"
  end

  specify "should have insert_select return nil if server_version < 80200" do
    @ds.meta_def(:server_version){80100}
    @ds.insert_select(:value=>10).should == nil
  end

  specify "should have insert_select insert the record and return the inserted record if server_version < 80200" do
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

context "Postgres::Database schema qualified tables" do
  setup do
    POSTGRES_DB << "CREATE SCHEMA schema_test"
    POSTGRES_DB.instance_variable_set(:@primary_keys, {})
    POSTGRES_DB.instance_variable_set(:@primary_key_sequences, {})
  end
  teardown do
    POSTGRES_DB << "DROP SCHEMA schema_test CASCADE"
    POSTGRES_DB.default_schema = :public
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
  
  specify "#tables should include only tables in the public schema if no schema is given" do
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :i}
    POSTGRES_DB.tables.should_not include(:schema_test)
  end
  
  specify "#tables should return tables in the schema provided by the :schema argument" do
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :i}
    POSTGRES_DB.tables(:schema=>:schema_test).should == [:schema_test]
  end
  
  specify "#table_exists? should assume the public schema if no schema is provided" do
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :i}
    POSTGRES_DB.table_exists?(:schema_test).should == false
  end
  
  specify "#table_exists? should see if the table is in a given schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :i}
    POSTGRES_DB.table_exists?(:schema_test__schema_test).should == true
  end
  
  specify "should be able to get primary keys for tables in a given schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){primary_key :i}
    POSTGRES_DB.synchronize{|c| POSTGRES_DB.send(:primary_key_for_table, c, :schema_test__schema_test).should == 'i'}
  end
  
  specify "should be able to get serial sequences for tables in a given schema" do
    POSTGRES_DB.create_table(:schema_test__schema_test){primary_key :i}
    POSTGRES_DB.synchronize{|c| POSTGRES_DB.send(:primary_key_sequence_for_table, c, :schema_test__schema_test).should == '"schema_test"."schema_test_i_seq"'}
  end
  
  specify "should be able to get custom sequences for tables in a given schema" do
    POSTGRES_DB << "CREATE SEQUENCE schema_test.kseq"
    POSTGRES_DB.create_table(:schema_test__schema_test){integer :j; primary_key :k, :type=>:integer, :default=>"nextval('schema_test.kseq'::regclass)".lit}
    POSTGRES_DB.synchronize{|c| POSTGRES_DB.send(:primary_key_sequence_for_table, c, :schema_test__schema_test).should == '"schema_test"."kseq"'}
  end
  
  specify "#default_schema= should change the default schema used from public" do
    POSTGRES_DB.create_table(:schema_test__schema_test){primary_key :i}
    POSTGRES_DB.default_schema = :schema_test
    POSTGRES_DB.table_exists?(:schema_test).should == true
    POSTGRES_DB.tables.should == [:schema_test]
    POSTGRES_DB.synchronize{|c| POSTGRES_DB.send(:primary_key_for_table, c, :schema_test).should == 'i'}
    POSTGRES_DB.synchronize{|c| POSTGRES_DB.send(:primary_key_sequence_for_table, c, :schema_test).should == '"schema_test"."schema_test_i_seq"'}
  end
end

if POSTGRES_DB.server_version >= 80300

  POSTGRES_DB.create_table! :test6 do
    text :title
    text :body
    full_text_index [:title, :body]
  end

  context "PostgreSQL tsearch2" do
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

context "Postgres::Database functions, languages, and triggers" do
  setup do
    @d = POSTGRES_DB
  end
  teardown do
    @d.drop_function('tf', :if_exists=>true, :cascade=>true)
    @d.drop_function('tf', :if_exists=>true, :cascade=>true, :args=>%w'integer integer')
    @d.drop_language(:plpgsql, :if_exists=>true, :cascade=>true)
    @d.drop_trigger(:test5, :identity, :if_exists=>true, :cascade=>true)
  end
  
  specify "#create_function and #drop_function should create and drop functions" do
    proc{@d['SELECT tf()'].all}.should raise_error(Sequel::DatabaseError)
    args = ['tf', 'SELECT 1', {:returns=>:integer}]
    @d.create_function_sql(*args).should =~ /\A\s*CREATE FUNCTION tf\(\)\s+RETURNS integer\s+LANGUAGE SQL\s+AS 'SELECT 1'\s*\z/
    @d.create_function(*args)
    rows = @d['SELECT tf()'].all.should == [{:tf=>1}]
    @d.drop_function_sql('tf').should == 'DROP FUNCTION tf()'
    @d.drop_function('tf')
    proc{@d['SELECT tf()'].all}.should raise_error(Sequel::DatabaseError)
  end
  
  specify "#create_function and #drop_function should support options" do
    args = ['tf', 'SELECT $1 + $2', {:args=>[[:integer, :a], :integer], :replace=>true, :returns=>:integer, :language=>'SQL', :behavior=>:immutable, :strict=>true, :security_definer=>true, :cost=>2, :set=>{:search_path => 'public'}}]
    @d.create_function_sql(*args).should =~ /\A\s*CREATE OR REPLACE FUNCTION tf\(a integer, integer\)\s+RETURNS integer\s+LANGUAGE SQL\s+IMMUTABLE\s+STRICT\s+SECURITY DEFINER\s+COST 2\s+SET search_path = public\s+AS 'SELECT \$1 \+ \$2'\s*\z/
    @d.create_function(*args)
    # Make sure replace works
    @d.create_function(*args)
    rows = @d['SELECT tf(1, 2)'].all.should == [{:tf=>3}]
    args = ['tf', {:if_exists=>true, :cascade=>true, :args=>[[:integer, :a], :integer]}]
    @d.drop_function_sql(*args).should == 'DROP FUNCTION IF EXISTS tf(a integer, integer) CASCADE'
    @d.drop_function(*args)
    # Make sure if exists works
    @d.drop_function(*args)
  end
  
  specify "#create_language and #drop_language should create and drop languages" do
    @d.create_language_sql(:plpgsql).should == 'CREATE LANGUAGE plpgsql'
    @d.create_language(:plpgsql)
    proc{@d.create_language(:plpgsql)}.should raise_error(Sequel::DatabaseError)
    @d.drop_language_sql(:plpgsql).should == 'DROP LANGUAGE plpgsql'
    @d.drop_language(:plpgsql)
    proc{@d.drop_language(:plpgsql)}.should raise_error(Sequel::DatabaseError)
    @d.create_language_sql(:plpgsql, :trusted=>true, :handler=>:a, :validator=>:b).should == 'CREATE TRUSTED LANGUAGE plpgsql HANDLER a VALIDATOR b'
    @d.drop_language_sql(:plpgsql, :if_exists=>true, :cascade=>true).should == 'DROP LANGUAGE IF EXISTS plpgsql CASCADE'
    # Make sure if exists works
    @d.drop_language(:plpgsql, :if_exists=>true, :cascade=>true)
  end
  
  specify "#create_trigger and #drop_trigger should create and drop triggers" do
    @d.create_language(:plpgsql)
    @d.create_function(:tf, 'BEGIN IF NEW.value IS NULL THEN RAISE EXCEPTION \'Blah\'; END IF; RETURN NEW; END;', :language=>:plpgsql, :returns=>:trigger)
    @d.create_trigger_sql(:test, :identity, :tf, :each_row=>true).should == 'CREATE TRIGGER identity BEFORE INSERT OR UPDATE OR DELETE ON public.test FOR EACH ROW EXECUTE PROCEDURE tf()'
    @d.create_trigger(:test, :identity, :tf, :each_row=>true)
    @d[:test].insert(:name=>'a', :value=>1)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>1}]
    proc{@d[:test].filter(:name=>'a').update(:value=>nil)}.should raise_error(Sequel::DatabaseError)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>1}]
    @d[:test].filter(:name=>'a').update(:value=>3)
    @d[:test].filter(:name=>'a').all.should == [{:name=>'a', :value=>3}]
    @d.drop_trigger_sql(:test, :identity).should == 'DROP TRIGGER identity ON public.test'
    @d.drop_trigger(:test, :identity)
    @d.create_trigger_sql(:test, :identity, :tf, :after=>true, :events=>:insert, :args=>[1, 'a']).should == 'CREATE TRIGGER identity AFTER INSERT ON public.test EXECUTE PROCEDURE tf(1, \'a\')'
    @d.drop_trigger_sql(:test, :identity, :if_exists=>true, :cascade=>true).should == 'DROP TRIGGER IF EXISTS identity ON public.test CASCADE'
    # Make sure if exists works
    @d.drop_trigger(:test, :identity, :if_exists=>true, :cascade=>true)
  end
end
