require File.join(File.dirname(__FILE__), '../spec_helper.rb')

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
    @db.schema(:test3, :reload=>true).should == [[:value, {:type=>:integer, :allow_null=>true, :max_chars=>0, :default=>nil, :db_type=>"integer", :numeric_precision=>32}], [:time, {:type=>:datetime, :allow_null=>true, :max_chars=>0, :default=>nil, :db_type=>"timestamp without time zone", :numeric_precision=>0}]]
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

    @d.insert_sql(:value => 333).should == \
      'INSERT INTO "test" ("value") VALUES (333)'

    @d.insert_sql(:x => :y).should == \
      'INSERT INTO "test" ("x") VALUES ("y")'
  end
  
  specify "should quote fields correctly when reversing the order if quoting identifiers" do
    @d.quote_identifiers = true
    @d.reverse_order(:name).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.reverse_order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name"'

    @d.reverse_order(:name, :test.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC, "test"'

    @d.reverse_order(:name.desc, :test).sql.should == \
      'SELECT * FROM "test" ORDER BY "name", "test" DESC'
  end
  
  specify "should support transactions" do
    POSTGRES_DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.should == 1
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

  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}
    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end

  specify "should consider strings containing backslashes to be escaped string literals" do
    @d.literal("\\dingo").should == "'\\\\dingo'"   # literally, E'\\dingo'
    @d.literal("dingo").should == "'dingo'"
  end
end

context "A PostgreSQL dataaset with a timestamp field" do
  setup do
    @d = POSTGRES_DB[:test3]
    @d.delete
  end

  specify "should store milliseconds in time fields" do
    t = Time.now
    @d << {:value=>1, :time=>t}
    @d[:value =>'1'][:time].should == t
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
      "CREATE TABLE posts (title text, body text)",
      "CREATE INDEX posts_title_body_index ON posts USING gin (to_tsvector(title || body))"
    ]
  end
  
  specify "should support fulltext indexes with a specific language" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      text :title
      text :body
      full_text_index [:title, :body], :language => 'french'
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (title text, body text)",
      "CREATE INDEX posts_title_body_index ON posts USING gin (to_tsvector('french', title || body))"
    ]
  end
  
  specify "should support full_text_search" do
    POSTGRES_DB[:posts].full_text_search(:title, 'ruby').sql.should ==
      "SELECT * FROM posts WHERE (to_tsvector(title) @@ to_tsquery('ruby'))"
    
    POSTGRES_DB[:posts].full_text_search([:title, :body], ['ruby', 'sequel']).sql.should ==
      "SELECT * FROM posts WHERE (to_tsvector(title || body) @@ to_tsquery('ruby | sequel'))"
      
    POSTGRES_DB[:posts].full_text_search(:title, 'ruby', :language => 'french').sql.should ==
      "SELECT * FROM posts WHERE (to_tsvector('french', title) @@ to_tsquery('french', 'ruby'))"
  end

  specify "should support spatial indexes" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      geometry :geom
      spatial_index [:geom]
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (geom geometry)",
      "CREATE INDEX posts_geom_index ON posts USING gist (geom)"
    ]
  end
  
  specify "should support indexes with index type" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      varchar :title, :size => 5
      index :title, :type => 'hash'
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (title varchar(5))",
      "CREATE INDEX posts_title_index ON posts USING hash (title)"
    ]
  end
  
  specify "should support unique indexes with index type" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      varchar :title, :size => 5
      index :title, :type => 'hash', :unique => true
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (title varchar(5))",
      "CREATE UNIQUE INDEX posts_title_index ON posts USING hash (title)"
    ]
  end
  
  specify "should support partial indexes" do
    g = Sequel::Schema::Generator.new(POSTGRES_DB) do
      varchar :title, :size => 5
      index :title, :where => {:something => 5}
    end
    POSTGRES_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (title varchar(5))",
      "CREATE INDEX posts_title_index ON posts (title) WHERE (something = 5)"
    ]
  end
end

context "Postgres::Dataset#multi_insert_sql / #import" do
  setup do
    @ds = POSTGRES_DB[:test]
  end
  
  specify "should return separate insert statements if server_version < 80200" do
    @ds.db.meta_def(:server_version) {80199}
    
    @ds.multi_insert_sql([:x, :y], [[1, 2], [3, 4]]).should == [
      'INSERT INTO test (x, y) VALUES (1, 2)',
      'INSERT INTO test (x, y) VALUES (3, 4)'
    ]
  end
  
  specify "should a single insert statement if server_version >= 80200" do
    @ds.db.meta_def(:server_version) {80200}
    
    @ds.multi_insert_sql([:x, :y], [[1, 2], [3, 4]]).should == [
      'INSERT INTO test (x, y) VALUES (1, 2), (3, 4)'
    ]

    @ds.db.meta_def(:server_version) {80201}
    
    @ds.multi_insert_sql([:x, :y], [[1, 2], [3, 4]]).should == [
      'INSERT INTO test (x, y) VALUES (1, 2), (3, 4)'
    ]
  end
end
