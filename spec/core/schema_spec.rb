require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "DB#create_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should accept the table name" do
    @db.create_table(:cats) {}
    @db.sqls.should == ['CREATE TABLE cats ()']
  end

  specify "should accept the table name in multiple formats" do
    @db.create_table(:cats__cats) {}
    @db.create_table("cats__cats1") {}
    @db.create_table(Sequel.identifier(:cats__cats2)) {}
    @db.create_table(Sequel.qualify(:cats3, :cats)) {}
    @db.sqls.should == ['CREATE TABLE cats.cats ()', 'CREATE TABLE cats__cats1 ()', 'CREATE TABLE cats__cats2 ()', 'CREATE TABLE cats3.cats ()']
  end

  specify "should raise an error if the table name argument is not valid" do
    proc{@db.create_table(1) {}}.should raise_error(Sequel::Error)
    proc{@db.create_table(Sequel.as(:cats, :c)) {}}.should raise_error(Sequel::Error)
  end

  specify "should remove cached schema entry" do
    @db.instance_variable_set(:@schemas, {'cats'=>[]})
    @db.create_table(:cats){Integer :a}
    @db.instance_variable_get(:@schemas).should be_empty
  end
  
  specify "should accept multiple columns" do
    @db.create_table(:cats) do
      column :id, :integer
      column :name, :text
    end
    @db.sqls.should == ['CREATE TABLE cats (id integer, name text)']
  end
  
  specify "should accept method calls as data types" do
    @db.create_table(:cats) do
      integer :id
      text :name
    end
    @db.sqls.should == ['CREATE TABLE cats (id integer, name text)']
  end
  
  specify "should transform types given as ruby classes to database-specific types" do
    @db.create_table(:cats) do
      String :a
      Integer :b
      Fixnum :c
      Bignum :d
      Float :e
      BigDecimal :f
      Date :g
      DateTime :h
      Time :i
      Numeric :j
      File :k
      TrueClass :l
      FalseClass :m
      column :n, Fixnum
      primary_key :o, :type=>String
      foreign_key :p, :f, :type=>Date
    end
    @db.sqls.should == ['CREATE TABLE cats (o varchar(255) PRIMARY KEY AUTOINCREMENT, a varchar(255), b integer, c integer, d bigint, e double precision, f numeric, g date, h timestamp, i timestamp, j numeric, k blob, l boolean, m boolean, n integer, p date REFERENCES f)']
  end

  specify "should transform types given as ruby classes to database-specific types" do
    @db.default_string_column_size = 50
    @db.create_table(:cats) do
      String :a
      String :a2, :size=>13
      String :a3, :fixed=>true
      String :a4, :size=>13, :fixed=>true
      String :a5, :text=>true
      varchar :a6
      varchar :a7, :size=>13
    end
    @db.sqls.should == ['CREATE TABLE cats (a varchar(50), a2 varchar(13), a3 char(50), a4 char(13), a5 text, a6 varchar(50), a7 varchar(13))']
  end

  specify "should allow the use of modifiers with ruby class types" do
    @db.create_table(:cats) do
      String :a, :size=>50
      String :b, :text=>true
      String :c, :fixed=>true, :size=>40
      Time :d, :only_time=>true
      BigDecimal :e, :size=>[11,2]
    end
    @db.sqls.should == ['CREATE TABLE cats (a varchar(50), b text, c char(40), d time, e numeric(11, 2))']
  end

  specify "should raise an error if you use a ruby class that isn't handled" do
    proc{@db.create_table(:cats){column :a, Class}}.should raise_error(Sequel::Error)
  end

  specify "should accept primary key definition" do
    @db.create_table(:cats) do
      primary_key :id
    end
    @db.sqls.should == ['CREATE TABLE cats (id integer PRIMARY KEY AUTOINCREMENT)']

    @db.create_table(:cats) do
      primary_key :id, :serial, :auto_increment => false
    end
    @db.sqls.should == ['CREATE TABLE cats (id serial PRIMARY KEY)']

    @db.create_table(:cats) do
      primary_key :id, :type => :serial, :auto_increment => false
    end
    @db.sqls.should == ['CREATE TABLE cats (id serial PRIMARY KEY)']
  end

  specify "should accept and literalize default values" do
    @db.create_table(:cats) do
      integer :id, :default => 123
      text :name, :default => "abc'def"
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer DEFAULT 123, name text DEFAULT 'abc''def')"]
  end
  
  specify "should accept not null definition" do
    @db.create_table(:cats) do
      integer :id
      text :name, :null => false
      text :name2, :allow_null => false
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text NOT NULL, name2 text NOT NULL)"]
  end
  
  specify "should accept null definition" do
    @db.create_table(:cats) do
      integer :id
      text :name, :null => true
      text :name2, :allow_null => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text NULL, name2 text NULL)"]
  end
  
  specify "should accept unique definition" do
    @db.create_table(:cats) do
      integer :id
      text :name, :unique => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text UNIQUE)"]
  end
  
  specify "should handle not deferred unique constraints" do
    @db.create_table(:cats) do
      integer :id
      text :name
      unique :name, :deferrable=>false
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text, UNIQUE (name) NOT DEFERRABLE)"]
  end
  
  specify "should handle deferred unique constraints" do
    @db.create_table(:cats) do
      integer :id
      text :name
      unique :name, :deferrable=>true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text, UNIQUE (name) DEFERRABLE INITIALLY DEFERRED)"]
  end
  
  specify "should handle deferred initially immediate unique constraints" do
    @db.create_table(:cats) do
      integer :id
      text :name
      unique :name, :deferrable=>:immediate
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text, UNIQUE (name) DEFERRABLE INITIALLY IMMEDIATE)"]
  end
  
  specify "should accept unsigned definition" do
    @db.create_table(:cats) do
      integer :value, :unsigned => true
    end
    @db.sqls.should == ["CREATE TABLE cats (value integer UNSIGNED)"]
  end
  
  specify "should accept [SET|ENUM](...) types" do
    @db.create_table(:cats) do
      set :color, :elements => ['black', 'tricolor', 'grey']
    end
    @db.sqls.should == ["CREATE TABLE cats (color set('black', 'tricolor', 'grey'))"]
  end
  
  specify "should accept varchar size" do
    @db.create_table(:cats) do
      varchar :name
    end
    @db.sqls.should == ["CREATE TABLE cats (name varchar(255))"]
    @db.create_table(:cats) do
      varchar :name, :size => 51
    end
    @db.sqls.should == ["CREATE TABLE cats (name varchar(51))"]
  end
  
  specify "should use double precision for double type" do
    @db.create_table(:cats) do
      double :name
    end
    @db.sqls.should == ["CREATE TABLE cats (name double precision)"]
  end

  specify "should accept foreign keys without options" do
    @db.create_table(:cats) do
      foreign_key :project_id
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer)"]
  end

  specify "should accept foreign keys with options" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects)"]
  end

  specify "should accept foreign keys with separate table argument" do
    @db.create_table(:cats) do
      foreign_key :project_id, :projects, :default=>3
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer DEFAULT 3 REFERENCES projects)"]
  end
  
  specify "should raise an error if the table argument to foreign_key isn't a hash, symbol, or nil" do
    proc{@db.create_table(:cats){foreign_key :project_id, Object.new, :default=>3}}.should raise_error(Sequel::Error)
  end
  
  specify "should accept foreign keys with arbitrary keys" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :key => :id
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects(id))"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :key => :zzz
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects(zzz))"]
  end
  
  specify "should accept foreign keys with ON DELETE clause" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :restrict
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE RESTRICT)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :cascade
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE CASCADE)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :no_action
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE NO ACTION)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :set_null
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE SET NULL)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :set_default
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE SET DEFAULT)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => 'NO ACTION FOO'
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE NO ACTION FOO)"]
  end

  specify "should accept foreign keys with ON UPDATE clause" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_update => :restrict
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON UPDATE RESTRICT)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_update => :cascade
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON UPDATE CASCADE)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_update => :no_action
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON UPDATE NO ACTION)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_update => :set_null
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON UPDATE SET NULL)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_update => :set_default
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON UPDATE SET DEFAULT)"]

    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_update => 'SET DEFAULT FOO'
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON UPDATE SET DEFAULT FOO)"]
  end
  
  specify "should accept foreign keys with deferrable option" do
    @db.create_table(:cats) do
      foreign_key :project_id, :projects, :deferrable=>true
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects DEFERRABLE INITIALLY DEFERRED)"]
  end

  specify "should accept collation" do
    @db.create_table(:cats) do
      varchar :name, :collate => :utf8_bin
    end
    @db.sqls.should == ["CREATE TABLE cats (name varchar(255) COLLATE utf8_bin)"]
  end

  specify "should accept inline index definition" do
    @db.create_table(:cats) do
      integer :id, :index => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX cats_id_index ON cats (id)"]
  end
  
  specify "should accept inline index definition with a hash of options" do
    @db.create_table(:cats) do
      integer :id, :index => {:unique=>true}
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE UNIQUE INDEX cats_id_index ON cats (id)"]
  end
  
  specify "should accept inline index definition for foreign keys" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :cascade, :index => true
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE CASCADE)",
      "CREATE INDEX cats_project_id_index ON cats (project_id)"]
  end
  
  specify "should accept inline index definition for foreign keys with a hash of options" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :cascade, :index => {:unique=>true}
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE CASCADE)",
      "CREATE UNIQUE INDEX cats_project_id_index ON cats (project_id)"]
  end
  
  specify "should accept index definitions" do
    @db.create_table(:cats) do
      integer :id
      index :id
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX cats_id_index ON cats (id)"]
  end

  specify "should accept unique index definitions" do
    @db.create_table(:cats) do
      text :name
      unique :name
    end
    @db.sqls.should == ["CREATE TABLE cats (name text, UNIQUE (name))"]
  end

  specify "should not raise on index error for unsupported index definitions if ignore_index_errors is used" do
    proc {
      @db.create_table(:cats, :ignore_index_errors=>true) do
        text :name
        full_text_index :name
      end
    }.should_not raise_error
  end

  specify "should raise on full-text index definitions" do
    proc {
      @db.create_table(:cats) do
        text :name
        full_text_index :name
      end
    }.should raise_error(Sequel::Error)
  end

  specify "should raise on spatial index definitions" do
    proc {
      @db.create_table(:cats) do
        point :geom
        spatial_index :geom
      end
    }.should raise_error(Sequel::Error)
  end

  specify "should raise on partial index definitions" do
    proc {
      @db.create_table(:cats) do
        text :name
        index :name, :where => {:something => true}
      end
    }.should raise_error(Sequel::Error)
  end

  specify "should raise index definitions with type" do
    proc {
      @db.create_table(:cats) do
        text :name
        index :name, :type => :hash
      end
    }.should raise_error(Sequel::Error)
  end

  specify "should ignore errors if the database raises an error on an index creation statement and the :ignore_index_errors option is used" do
    @db.meta_def(:execute_ddl){|*a| raise Sequel::DatabaseError if /blah/.match(a.first); super(*a)}
    lambda{@db.create_table(:cats){Integer :id; index :blah; index :id}}.should raise_error(Sequel::DatabaseError)
    @db.sqls.should == ['CREATE TABLE cats (id integer)']
    lambda{@db.create_table(:cats, :ignore_index_errors=>true){Integer :id; index :blah; index :id}}.should_not raise_error(Sequel::DatabaseError)
    @db.sqls.should == ['CREATE TABLE cats (id integer)', 'CREATE INDEX cats_id_index ON cats (id)']
  end

  specify "should accept multiple index definitions" do
    @db.create_table(:cats) do
      integer :id
      index :id
      index :name
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX cats_id_index ON cats (id)", "CREATE INDEX cats_name_index ON cats (name)"]
  end
  
  specify "should accept functional indexes" do
    @db.create_table(:cats) do
      integer :id
      index Sequel.function(:lower, :name)
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX cats_lower_name__index ON cats (lower(name))"]
  end
  
  specify "should accept indexes with identifiers" do
    @db.create_table(:cats) do
      integer :id
      index Sequel.identifier(:lower__name)
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX cats_lower__name_index ON cats (lower__name)"]
  end
  
  specify "should accept custom index names" do
    @db.create_table(:cats) do
      integer :id
      index :id, :name => 'abc'
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX abc ON cats (id)"]
  end

  specify "should accept unique index definitions" do
    @db.create_table(:cats) do
      integer :id
      index :id, :unique => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE UNIQUE INDEX cats_id_index ON cats (id)"]
  end
  
  specify "should accept composite index definitions" do
    @db.create_table(:cats) do
      integer :id
      index [:id, :name], :unique => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE UNIQUE INDEX cats_id_name_index ON cats (id, name)"]
  end
  
  specify "should accept unnamed constraint definitions with blocks" do
    @db.create_table(:cats) do
      integer :score
      check{(x.sql_number > 0) & (y.sql_number < 1)}
    end
    @db.sqls.should == ["CREATE TABLE cats (score integer, CHECK ((x > 0) AND (y < 1)))"]
  end

  specify "should accept unnamed constraint definitions" do
    @db.create_table(:cats) do
      check 'price < ?', 100
    end
    @db.sqls.should == ["CREATE TABLE cats (CHECK (price < 100))"]
  end

  specify "should accept hash constraints" do
    @db.create_table(:cats) do
      check :price=>100
    end
    @db.sqls.should == ["CREATE TABLE cats (CHECK (price = 100))"]
  end

  specify "should accept named constraint definitions" do
    @db.create_table(:cats) do
      integer :score
      constraint :valid_score, 'score <= 100'
    end
    @db.sqls.should == ["CREATE TABLE cats (score integer, CONSTRAINT valid_score CHECK (score <= 100))"]
  end

  specify "should accept named constraint definitions with block" do
    @db.create_table(:cats) do
      constraint(:blah_blah){(x.sql_number > 0) & (y.sql_number < 1)}
    end
    @db.sqls.should == ["CREATE TABLE cats (CONSTRAINT blah_blah CHECK ((x > 0) AND (y < 1)))"]
  end

  specify "should raise an error if an invalid constraint type is used" do
    proc{@db.create_table(:cats){unique [:a, :b], :type=>:bb}}.should raise_error(Sequel::Error)
  end

  specify "should accept composite primary keys" do
    @db.create_table(:cats) do
      integer :a
      integer :b
      primary_key [:a, :b]
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, PRIMARY KEY (a, b))"]
  end

  specify "should accept named composite primary keys" do
    @db.create_table(:cats) do
      integer :a
      integer :b
      primary_key [:a, :b], :name => :cpk
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, CONSTRAINT cpk PRIMARY KEY (a, b))"]
  end

  specify "should accept composite foreign keys" do
    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, FOREIGN KEY (a, b) REFERENCES abc)"]
  end

  specify "should accept named composite foreign keys" do
    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc, :name => :cfk
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, CONSTRAINT cfk FOREIGN KEY (a, b) REFERENCES abc)"]
  end

  specify "should accept composite foreign keys with arbitrary keys" do
    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc, :key => [:real_a, :real_b]
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, FOREIGN KEY (a, b) REFERENCES abc(real_a, real_b))"]

    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc, :key => [:z, :x]
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, FOREIGN KEY (a, b) REFERENCES abc(z, x))"]
  end

  specify "should accept composite foreign keys with on delete and on update clauses" do
    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc, :on_delete => :cascade
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, FOREIGN KEY (a, b) REFERENCES abc ON DELETE CASCADE)"]

    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc, :on_update => :no_action
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, FOREIGN KEY (a, b) REFERENCES abc ON UPDATE NO ACTION)"]

    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc, :on_delete => :restrict, :on_update => :set_default
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, FOREIGN KEY (a, b) REFERENCES abc ON DELETE RESTRICT ON UPDATE SET DEFAULT)"]

    @db.create_table(:cats) do
      integer :a
      integer :b
      foreign_key [:a, :b], :abc, :key => [:x, :y], :on_delete => :set_null, :on_update => :set_null
    end
    @db.sqls.should == ["CREATE TABLE cats (a integer, b integer, FOREIGN KEY (a, b) REFERENCES abc(x, y) ON DELETE SET NULL ON UPDATE SET NULL)"]
  end

  specify "should accept an :as option to create a table from the results of a dataset" do
    @db.create_table(:cats, :as=>@db[:a])
    @db.sqls.should == ['CREATE TABLE cats AS SELECT * FROM a']
  end

  specify "should accept an :as option to create a table from a SELECT string" do
    @db.create_table(:cats, :as=>'SELECT * FROM a')
    @db.sqls.should == ['CREATE TABLE cats AS SELECT * FROM a']
  end

  specify "should raise an Error if both a block and an :as argument are given" do
    proc{@db.create_table(:cats, :as=>@db[:a]){}}.should raise_error(Sequel::Error)
  end
end

describe "DB#create_table!" do
  before do
    @db = Sequel.mock
  end
  
  specify "should create the table if it does not exist" do
    @db.meta_def(:table_exists?){|a| false}
    @db.create_table!(:cats){|*a|}
    @db.sqls.should == ['CREATE TABLE cats ()']
  end
  
  specify "should drop the table before creating it if it already exists" do
    @db.meta_def(:table_exists?){|a| true}
    @db.create_table!(:cats){|*a|}
    @db.sqls.should == ['DROP TABLE cats', 'CREATE TABLE cats ()']
  end
end

describe "DB#create_table?" do
  before do
    @db = Sequel.mock
  end
  
  specify "should not create the table if the table already exists" do
    @db.meta_def(:table_exists?){|a| true}
    @db.create_table?(:cats){|*a|}
    @db.sqls.should == []
  end
  
  specify "should create the table if the table doesn't already exist" do
    @db.meta_def(:table_exists?){|a| false}
    @db.create_table?(:cats){|*a|}
    @db.sqls.should == ['CREATE TABLE cats ()']
  end
  
  specify "should use IF NOT EXISTS if the database supports that" do
    @db.meta_def(:supports_create_table_if_not_exists?){true}
    @db.create_table?(:cats){|*a|}
    @db.sqls.should == ['CREATE TABLE IF NOT EXISTS cats ()']
  end
end

describe "DB#create_join_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should take a hash with foreign keys and table name values" do
    @db.create_join_table(:cat_id=>:cats, :dog_id=>:dogs)
    @db.sqls.should == ['CREATE TABLE cats_dogs (cat_id integer NOT NULL REFERENCES cats, dog_id integer NOT NULL REFERENCES dogs, PRIMARY KEY (cat_id, dog_id))', 'CREATE INDEX cats_dogs_dog_id_cat_id_index ON cats_dogs (dog_id, cat_id)']
  end
  
  specify "should be able to have values be a hash of options" do
    @db.create_join_table(:cat_id=>{:table=>:cats, :null=>true}, :dog_id=>{:table=>:dogs, :default=>0})
    @db.sqls.should == ['CREATE TABLE cats_dogs (cat_id integer NULL REFERENCES cats, dog_id integer DEFAULT 0 NOT NULL REFERENCES dogs, PRIMARY KEY (cat_id, dog_id))', 'CREATE INDEX cats_dogs_dog_id_cat_id_index ON cats_dogs (dog_id, cat_id)']
  end
  
  specify "should be able to pass a second hash of table options" do
    @db.create_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :temp=>true)
    @db.sqls.should == ['CREATE TEMPORARY TABLE cats_dogs (cat_id integer NOT NULL REFERENCES cats, dog_id integer NOT NULL REFERENCES dogs, PRIMARY KEY (cat_id, dog_id))', 'CREATE INDEX cats_dogs_dog_id_cat_id_index ON cats_dogs (dog_id, cat_id)']
  end
  
  specify "should recognize :name option in table options" do
    @db.create_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :name=>:f)
    @db.sqls.should == ['CREATE TABLE f (cat_id integer NOT NULL REFERENCES cats, dog_id integer NOT NULL REFERENCES dogs, PRIMARY KEY (cat_id, dog_id))', 'CREATE INDEX f_dog_id_cat_id_index ON f (dog_id, cat_id)']
  end
  
  specify "should recognize :index_options option in table options" do
    @db.create_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :index_options=>{:name=>:foo_index})
    @db.sqls.should == ['CREATE TABLE cats_dogs (cat_id integer NOT NULL REFERENCES cats, dog_id integer NOT NULL REFERENCES dogs, PRIMARY KEY (cat_id, dog_id))', 'CREATE INDEX foo_index ON cats_dogs (dog_id, cat_id)']
  end
  
  specify "should recognize :no_index option in table options" do
    @db.create_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :no_index=>true)
    @db.sqls.should == ['CREATE TABLE cats_dogs (cat_id integer NOT NULL REFERENCES cats, dog_id integer NOT NULL REFERENCES dogs, PRIMARY KEY (cat_id, dog_id))']
  end
  
  specify "should recognize :no_primary_key option in table options" do
    @db.create_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :no_primary_key=>true)
    @db.sqls.should == ['CREATE TABLE cats_dogs (cat_id integer NOT NULL REFERENCES cats, dog_id integer NOT NULL REFERENCES dogs)', 'CREATE INDEX cats_dogs_dog_id_cat_id_index ON cats_dogs (dog_id, cat_id)']
  end
  
  specify "should raise an error if the hash doesn't have 2 entries with table names" do
    proc{@db.create_join_table({})}.should raise_error(Sequel::Error)
    proc{@db.create_join_table({:cat_id=>:cats})}.should raise_error(Sequel::Error)
    proc{@db.create_join_table({:cat_id=>:cats, :human_id=>:humans, :dog_id=>:dog})}.should raise_error(Sequel::Error)
    proc{@db.create_join_table({:cat_id=>:cats, :dog_id=>{}})}.should raise_error(Sequel::Error)
  end
end
  
describe "DB#drop_join_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should take a hash with foreign keys and table name values and drop the table" do
    @db.drop_join_table(:cat_id=>:cats, :dog_id=>:dogs)
    @db.sqls.should == ['DROP TABLE cats_dogs']
  end
  
  specify "should be able to have values be a hash of options" do
    @db.drop_join_table(:cat_id=>{:table=>:cats, :null=>true}, :dog_id=>{:table=>:dogs, :default=>0})
    @db.sqls.should == ['DROP TABLE cats_dogs']
  end

  specify "should respect a second hash of table options" do
    @db.drop_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :cascade=>true)
    @db.sqls.should == ['DROP TABLE cats_dogs CASCADE']
  end

  specify "should respect :name option for table name" do
    @db.drop_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :name=>:f)
    @db.sqls.should == ['DROP TABLE f']
  end
  
  specify "should raise an error if the hash doesn't have 2 entries with table names" do
    proc{@db.drop_join_table({})}.should raise_error(Sequel::Error)
    proc{@db.drop_join_table({:cat_id=>:cats})}.should raise_error(Sequel::Error)
    proc{@db.drop_join_table({:cat_id=>:cats, :human_id=>:humans, :dog_id=>:dog})}.should raise_error(Sequel::Error)
    proc{@db.drop_join_table({:cat_id=>:cats, :dog_id=>{}})}.should raise_error(Sequel::Error)
  end
end

describe "DB#drop_table" do
  before do
    @db = Sequel.mock
  end

  specify "should generate a DROP TABLE statement" do
    @db.drop_table :cats
    @db.sqls.should == ['DROP TABLE cats']
  end

  specify "should drop multiple tables at once" do
    @db.drop_table :cats, :dogs
    @db.sqls.should == ['DROP TABLE cats', 'DROP TABLE dogs']
  end

  specify "should take an options hash and support the :cascade option" do
    @db.drop_table :cats, :dogs, :cascade=>true
    @db.sqls.should == ['DROP TABLE cats CASCADE', 'DROP TABLE dogs CASCADE']
  end
end

describe "DB#drop_table?" do
  before do
    @db = Sequel.mock
  end
  
  specify "should drop the table if it exists" do
    @db.meta_def(:table_exists?){|a| true}
    @db.drop_table?(:cats)
    @db.sqls.should == ["DROP TABLE cats"]
  end
  
  specify "should do nothing if the table does not exist" do
    @db.meta_def(:table_exists?){|a| false}
    @db.drop_table?(:cats)
    @db.sqls.should == []
  end
  
  specify "should operate on multiple tables at once" do
    @db.meta_def(:table_exists?){|a| a == :cats}
    @db.drop_table? :cats, :dogs
    @db.sqls.should == ['DROP TABLE cats']
  end

  specify "should take an options hash and support the :cascade option" do
    @db.meta_def(:table_exists?){|a| true}
    @db.drop_table? :cats, :dogs, :cascade=>true
    @db.sqls.should == ['DROP TABLE cats CASCADE', 'DROP TABLE dogs CASCADE']
  end

  specify "should use IF NOT EXISTS if the database supports that" do
    @db.meta_def(:supports_drop_table_if_exists?){true}
    @db.drop_table? :cats, :dogs
    @db.sqls.should == ['DROP TABLE IF EXISTS cats', 'DROP TABLE IF EXISTS dogs']
  end

  specify "should use IF NOT EXISTS with CASCADE if the database supports that" do
    @db.meta_def(:supports_drop_table_if_exists?){true}
    @db.drop_table? :cats, :dogs, :cascade=>true
    @db.sqls.should == ['DROP TABLE IF EXISTS cats CASCADE', 'DROP TABLE IF EXISTS dogs CASCADE']
  end
end

describe "DB#alter_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should allow adding not null constraint via set_column_allow_null with false argument" do
    @db.alter_table(:cats) do
      set_column_allow_null :score, false
    end
    @db.sqls.should == ["ALTER TABLE cats ALTER COLUMN score SET NOT NULL"]
  end
  
  specify "should allow removing not null constraint via set_column_allow_null with true argument" do
    @db.alter_table(:cats) do
      set_column_allow_null :score, true
    end
    @db.sqls.should == ["ALTER TABLE cats ALTER COLUMN score DROP NOT NULL"]
  end

  specify "should allow adding not null constraint via set_column_not_null" do
    @db.alter_table(:cats) do
      set_column_not_null :score
    end
    @db.sqls.should == ["ALTER TABLE cats ALTER COLUMN score SET NOT NULL"]
  end
  
  specify "should allow removing not null constraint via set_column_allow_null without argument" do
    @db.alter_table(:cats) do
      set_column_allow_null :score
    end
    @db.sqls.should == ["ALTER TABLE cats ALTER COLUMN score DROP NOT NULL"]
  end

  specify "should support add_column" do
    @db.alter_table(:cats) do
      add_column :score, :integer
    end
    @db.sqls.should == ["ALTER TABLE cats ADD COLUMN score integer"]
  end

  specify "should support add_constraint" do
    @db.alter_table(:cats) do
      add_constraint :valid_score, 'score <= 100'
    end
    @db.sqls.should == ["ALTER TABLE cats ADD CONSTRAINT valid_score CHECK (score <= 100)"]
  end

  specify "should support add_constraint with block" do
    @db.alter_table(:cats) do
      add_constraint(:blah_blah){(x.sql_number > 0) & (y.sql_number < 1)}
    end
    @db.sqls.should == ["ALTER TABLE cats ADD CONSTRAINT blah_blah CHECK ((x > 0) AND (y < 1))"]
  end

  specify "should support add_unique_constraint" do
    @db.alter_table(:cats) do
      add_unique_constraint [:a, :b]
    end
    @db.sqls.should == ["ALTER TABLE cats ADD UNIQUE (a, b)"]

    @db.alter_table(:cats) do
      add_unique_constraint [:a, :b], :name => :ab_uniq
    end
    @db.sqls.should == ["ALTER TABLE cats ADD CONSTRAINT ab_uniq UNIQUE (a, b)"]
  end

  specify "should support add_foreign_key" do
    @db.alter_table(:cats) do
      add_foreign_key :node_id, :nodes
    end
    @db.sqls.should == ["ALTER TABLE cats ADD COLUMN node_id integer REFERENCES nodes"]
  end

  specify "should support add_foreign_key with composite foreign keys" do
    @db.alter_table(:cats) do
      add_foreign_key [:node_id, :prop_id], :nodes_props
    end
    @db.sqls.should == ["ALTER TABLE cats ADD FOREIGN KEY (node_id, prop_id) REFERENCES nodes_props"]

    @db.alter_table(:cats) do
      add_foreign_key [:node_id, :prop_id], :nodes_props, :name => :cfk
    end
    @db.sqls.should == ["ALTER TABLE cats ADD CONSTRAINT cfk FOREIGN KEY (node_id, prop_id) REFERENCES nodes_props"]

    @db.alter_table(:cats) do
      add_foreign_key [:node_id, :prop_id], :nodes_props, :key => [:nid, :pid]
    end
    @db.sqls.should == ["ALTER TABLE cats ADD FOREIGN KEY (node_id, prop_id) REFERENCES nodes_props(nid, pid)"]

    @db.alter_table(:cats) do
      add_foreign_key [:node_id, :prop_id], :nodes_props, :on_delete => :restrict, :on_update => :cascade
    end
    @db.sqls.should == ["ALTER TABLE cats ADD FOREIGN KEY (node_id, prop_id) REFERENCES nodes_props ON DELETE RESTRICT ON UPDATE CASCADE"]
  end

  specify "should support add_index" do
    @db.alter_table(:cats) do
      add_index :name
    end
    @db.sqls.should == ["CREATE INDEX cats_name_index ON cats (name)"]
  end

  specify "should ignore errors if the database raises an error on an add_index call and the :ignore_errors option is used" do
    @db.meta_def(:execute_ddl){|*a| raise Sequel::DatabaseError}
    lambda{@db.add_index(:cats, :id)}.should raise_error(Sequel::DatabaseError)
    lambda{@db.add_index(:cats, :id, :ignore_errors=>true)}.should_not raise_error(Sequel::DatabaseError)
    @db.sqls.should == []
  end

  specify "should support add_primary_key" do
    @db.alter_table(:cats) do
      add_primary_key :id
    end
    @db.sqls.should == ["ALTER TABLE cats ADD COLUMN id integer PRIMARY KEY AUTOINCREMENT"]
  end

  specify "should support add_primary_key with composite primary keys" do
    @db.alter_table(:cats) do
      add_primary_key [:id, :type]
    end
    @db.sqls.should == ["ALTER TABLE cats ADD PRIMARY KEY (id, type)"]

    @db.alter_table(:cats) do
      add_primary_key [:id, :type], :name => :cpk
    end
    @db.sqls.should == ["ALTER TABLE cats ADD CONSTRAINT cpk PRIMARY KEY (id, type)"]
  end

  specify "should support drop_column" do
    @db.alter_table(:cats) do
      drop_column :score
    end
    @db.sqls.should == ["ALTER TABLE cats DROP COLUMN score"]
  end

  specify "should support drop_column with :cascade=>true option" do
    @db.alter_table(:cats) do
      drop_column :score, :cascade=>true
    end
    @db.sqls.should == ["ALTER TABLE cats DROP COLUMN score CASCADE"]
  end

  specify "should support drop_constraint" do
    @db.alter_table(:cats) do
      drop_constraint :valid_score
    end
    @db.sqls.should == ["ALTER TABLE cats DROP CONSTRAINT valid_score"]
  end

  specify "should support drop_constraint with :cascade=>true option" do
    @db.alter_table(:cats) do
      drop_constraint :valid_score, :cascade=>true
    end
    @db.sqls.should == ["ALTER TABLE cats DROP CONSTRAINT valid_score CASCADE"]
  end

  specify "should support drop_index" do
    @db.alter_table(:cats) do
      drop_index :name
    end
    @db.sqls.should == ["DROP INDEX cats_name_index"]
  end

  specify "should support drop_index with a given name" do
    @db.alter_table(:cats) do
      drop_index :name, :name=>:blah_blah
    end
    @db.sqls.should == ["DROP INDEX blah_blah"]
  end

  specify "should support rename_column" do
    @db.alter_table(:cats) do
      rename_column :name, :old_name
    end
    @db.sqls.should == ["ALTER TABLE cats RENAME COLUMN name TO old_name"]
  end

  specify "should support set_column_default" do
    @db.alter_table(:cats) do
      set_column_default :score, 3
    end
    @db.sqls.should == ["ALTER TABLE cats ALTER COLUMN score SET DEFAULT 3"]
  end

  specify "should support set_column_type" do
    @db.alter_table(:cats) do
      set_column_type :score, :real
    end
    @db.sqls.should == ["ALTER TABLE cats ALTER COLUMN score TYPE real"]
  end

  specify "should support set_column_type with options" do
    @db.alter_table(:cats) do
      set_column_type :score, :integer, :unsigned=>true
      set_column_type :score, :varchar, :size=>30
      set_column_type :score, :enum, :elements=>['a', 'b']
    end
    @db.sqls.should == ["ALTER TABLE cats ALTER COLUMN score TYPE integer UNSIGNED",
      "ALTER TABLE cats ALTER COLUMN score TYPE varchar(30)",
      "ALTER TABLE cats ALTER COLUMN score TYPE enum('a', 'b')"]
  end

  specify "should combine operations into a single query if the database supports it" do
    @db.meta_def(:supports_combining_alter_table_ops?){true}
    @db.alter_table(:cats) do
      add_column :a, Integer
      drop_column :b
      set_column_not_null :c
      rename_column :d, :e
      set_column_default :f, 'g'
      set_column_type :h, Integer
      add_constraint(:i){a > 1}
      drop_constraint :j
    end
    @db.sqls.should == ["ALTER TABLE cats ADD COLUMN a integer, DROP COLUMN b, ALTER COLUMN c SET NOT NULL, RENAME COLUMN d TO e, ALTER COLUMN f SET DEFAULT 'g', ALTER COLUMN h TYPE integer, ADD CONSTRAINT i CHECK (a > 1), DROP CONSTRAINT j"]
  end
  
  specify "should combine operations into consecutive groups of combinable operations if the database supports combining operations" do
    @db.meta_def(:supports_combining_alter_table_ops?){true}
    @db.alter_table(:cats) do
      add_column :a, Integer
      drop_column :b
      set_column_not_null :c
      rename_column :d, :e
      add_index :e
      set_column_default :f, 'g'
      set_column_type :h, Integer
      add_constraint(:i){a > 1}
      drop_constraint :j
    end
    @db.sqls.should == ["ALTER TABLE cats ADD COLUMN a integer, DROP COLUMN b, ALTER COLUMN c SET NOT NULL, RENAME COLUMN d TO e",
      "CREATE INDEX cats_e_index ON cats (e)",
      "ALTER TABLE cats ALTER COLUMN f SET DEFAULT 'g', ALTER COLUMN h TYPE integer, ADD CONSTRAINT i CHECK (a > 1), DROP CONSTRAINT j"]
  end
  
end

describe "Database#create_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.create_table :test do
      primary_key :id, :integer, :null => false
      column :name, :text
      index :name, :unique => true
    end
    @db.sqls.should == ['CREATE TABLE test (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text)',
      'CREATE UNIQUE INDEX test_name_index ON test (name)']
  end
  
  specify "should create a temporary table" do
    @db.create_table :test_tmp, :temp => true do
      primary_key :id, :integer, :null => false
      column :name, :text
      index :name, :unique => true
    end
    
    @db.sqls.should == ['CREATE TEMPORARY TABLE test_tmp (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text)',
      'CREATE UNIQUE INDEX test_tmp_name_index ON test_tmp (name)']
  end

  specify "should not use default schema when creating a temporary table" do
    @db.default_schema = :foo
    @db.create_table :test_tmp, :temp => true do
      column :name, :text
    end
    @db.sqls.should == ['CREATE TEMPORARY TABLE test_tmp (name text)']
  end
end

describe "Database#alter_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.alter_table :xyz do
      add_column :aaa, :text, :null => false, :unique => true
      drop_column :bbb
      rename_column :ccc, :ddd
      set_column_type :eee, :integer
      set_column_default :hhh, 'abcd'
      add_index :fff, :unique => true
      drop_index :ggg
    end
    
    @db.sqls.should == ['ALTER TABLE xyz ADD COLUMN aaa text NOT NULL UNIQUE',
      'ALTER TABLE xyz DROP COLUMN bbb',
      'ALTER TABLE xyz RENAME COLUMN ccc TO ddd',
      'ALTER TABLE xyz ALTER COLUMN eee TYPE integer',
      "ALTER TABLE xyz ALTER COLUMN hhh SET DEFAULT 'abcd'",
      'CREATE UNIQUE INDEX xyz_fff_index ON xyz (fff)',
      'DROP INDEX xyz_ggg_index']
  end
end

describe "Database#add_column" do
  specify "should construct proper SQL" do
    db = Sequel.mock
    db.add_column :test, :name, :text, :unique => true
    db.sqls.should == ['ALTER TABLE test ADD COLUMN name text UNIQUE']
  end
end

describe "Database#drop_column" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.drop_column :test, :name
    @db.sqls.should == ['ALTER TABLE test DROP COLUMN name']
  end
  
  specify "should use CASCADE for :cascade=>true option" do
    @db.drop_column :test, :name, :cascade=>true
    @db.sqls.should == ['ALTER TABLE test DROP COLUMN name CASCADE']
  end
end

describe "Database#rename_column" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.rename_column :test, :abc, :def
    @db.sqls.should == ['ALTER TABLE test RENAME COLUMN abc TO def']
  end
end

describe "Database#set_column_type" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.set_column_type :test, :name, :integer
    @db.sqls.should == ['ALTER TABLE test ALTER COLUMN name TYPE integer']
  end
end

describe "Database#set_column_default" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.set_column_default :test, :name, 'zyx'
    @db.sqls.should == ["ALTER TABLE test ALTER COLUMN name SET DEFAULT 'zyx'"]
  end
end

describe "Database#add_index" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.add_index :test, :name, :unique => true
    @db.sqls.should == ['CREATE UNIQUE INDEX test_name_index ON test (name)']
  end
  
  specify "should accept multiple columns" do
    @db.add_index :test, [:one, :two]
    @db.sqls.should == ['CREATE INDEX test_one_two_index ON test (one, two)']
  end
end

describe "Database#drop_index" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.drop_index :test, :name
    @db.sqls.should == ['DROP INDEX test_name_index']
  end
  
end

describe "Database#drop_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.drop_table :test
    @db.sqls.should == ['DROP TABLE test']
  end
  
  specify "should accept multiple table names" do
    @db.drop_table :a, :bb, :ccc
    @db.sqls.should == ['DROP TABLE a', 'DROP TABLE bb', 'DROP TABLE ccc']
  end
end

describe "Database#rename_table" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.rename_table :abc, :xyz
    @db.sqls.should == ['ALTER TABLE abc RENAME TO xyz']
  end
end

describe "Database#create_view" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL with raw SQL" do
    @db.create_view :test, "SELECT * FROM xyz"
    @db.sqls.should == ['CREATE VIEW test AS SELECT * FROM xyz']
    @db.create_view Sequel.identifier(:test), "SELECT * FROM xyz"
    @db.sqls.should == ['CREATE VIEW test AS SELECT * FROM xyz']
  end
  
  specify "should construct proper SQL with dataset" do
    @db.create_view :test, @db[:items].select(:a, :b).order(:c)
    @db.sqls.should == ['CREATE VIEW test AS SELECT a, b FROM items ORDER BY c']
    @db.create_or_replace_view :sch__test, "SELECT * FROM xyz"
    @db.sqls.should == ['DROP VIEW sch.test', 'CREATE VIEW sch.test AS SELECT * FROM xyz']
  end

  specify "should construct proper SQL with dataset" do
    @db.create_or_replace_view :test, @db[:items].select(:a, :b).order(:c)
    @db.sqls.should == ['DROP VIEW test', 'CREATE VIEW test AS SELECT a, b FROM items ORDER BY c']
    @db.create_or_replace_view Sequel.identifier(:test), @db[:items].select(:a, :b).order(:c)
    @db.sqls.should == ['DROP VIEW test', 'CREATE VIEW test AS SELECT a, b FROM items ORDER BY c']
  end

  specify "should use CREATE OR REPLACE VIEW if such syntax is supported" do
    def @db.supports_create_or_replace_view?() true end
    @db.create_or_replace_view :test, @db[:items]
    @db.sqls.should == ['CREATE OR REPLACE VIEW test AS SELECT * FROM items']
  end
end

describe "Database#drop_view" do
  before do
    @db = Sequel.mock
  end
  
  specify "should construct proper SQL" do
    @db.drop_view :test
    @db.drop_view Sequel.identifier(:test)
    @db.drop_view :sch__test
    @db.drop_view Sequel.qualify(:sch, :test)
    @db.sqls.should == ['DROP VIEW test', 'DROP VIEW test', 'DROP VIEW sch.test', 'DROP VIEW sch.test']
  end

  specify "should drop multiple views at once" do
    @db.drop_view :cats, :dogs
    @db.sqls.should == ['DROP VIEW cats', 'DROP VIEW dogs']
  end

  specify "should take an options hash and support the :cascade option" do
    @db.drop_view :cats, :dogs, :cascade=>true
    @db.sqls.should == ['DROP VIEW cats CASCADE', 'DROP VIEW dogs CASCADE']
  end
end

describe "Database#alter_table_sql" do
  specify "should raise error for an invalid op" do
    proc {Sequel.mock.send(:alter_table_sql, :mau, :op => :blah)}.should raise_error(Sequel::Error)
  end
end

describe "Schema Parser" do
  before do
    @sqls = []
    @db = Sequel::Database.new
  end

  specify "should raise an error if there are no columns" do
    @db.meta_def(:schema_parse_table) do |t, opts|
      []
    end
    proc{@db.schema(:x)}.should raise_error(Sequel::Error)
  end

  specify "should cache data by default" do
    @db.meta_def(:schema_parse_table) do |t, opts|
      [[:a, {}]]
    end
    @db.schema(:x).should equal(@db.schema(:x))
  end

  specify "should not cache data if :reload=>true is given" do
    @db.meta_def(:schema_parse_table) do |t, opts|
      [[:a, {}]]
    end
    @db.schema(:x).should_not equal(@db.schema(:x, :reload=>true))
  end

  specify "should not cache schema metadata if cache_schema is false" do
    @db.cache_schema = false
    @db.meta_def(:schema_parse_table) do |t, opts|
      [[:a, {}]]
    end
    @db.schema(:x).should_not equal(@db.schema(:x))
  end

  specify "should provide options if given a table name" do
    c = nil
    @db.meta_def(:schema_parse_table) do |t, opts|
      c = [t, opts]
      [[:a, {:db_type=>t.to_s}]]
    end
    @db.schema(:x)
    c.should == ["x", {}]
    @db.schema(:s__x)
    c.should == ["x", {:schema=>"s"}]
    ds = @db[:s__y]
    @db.schema(ds)
    c.should == ["y", {:schema=>"s", :dataset=>ds}]
  end

  specify "should parse the schema correctly for a single table" do
    sqls = @sqls
    proc{@db.schema(:x)}.should raise_error(Sequel::Error)
    @db.meta_def(:schema_parse_table) do |t, opts|
      sqls << t
      [[:a, {:db_type=>t.to_s}]]
    end
    @db.schema(:x).should == [[:a, {:db_type=>"x", :ruby_default=>nil}]]
    @sqls.should == ['x']
    @db.schema(:x).should == [[:a, {:db_type=>"x", :ruby_default=>nil}]]
    @sqls.should == ['x']
    @db.schema(:x, :reload=>true).should == [[:a, {:db_type=>"x", :ruby_default=>nil}]]
    @sqls.should == ['x', 'x']
  end

  specify "should convert various types of table name arguments" do
    @db.meta_def(:schema_parse_table) do |t, opts|
      [[t, opts]]
    end
    s1 = @db.schema(:x)
    s1.should == [['x', {:ruby_default=>nil}]]
    @db.schema(:x).object_id.should == s1.object_id
    @db.schema(Sequel.identifier(:x)).object_id.should == s1.object_id

    s2 = @db.schema(:x__y)
    s2.should == [['y', {:schema=>'x', :ruby_default=>nil}]]
    @db.schema(:x__y).object_id.should == s2.object_id
    @db.schema(Sequel.qualify(:x, :y)).object_id.should == s2.object_id

    s2 = @db.schema(Sequel.qualify(:v, :x__y))
    s2.should == [['y', {:schema=>'x', :ruby_default=>nil, :information_schema_schema=>Sequel.identifier('v')}]]
    @db.schema(Sequel.qualify(:v, :x__y)).object_id.should == s2.object_id
    @db.schema(Sequel.qualify(:v__x, :y)).object_id.should == s2.object_id

    s2 = @db.schema(Sequel.qualify(:u__v, :x__y))
    s2.should == [['y', {:schema=>'x', :ruby_default=>nil, :information_schema_schema=>Sequel.qualify('u', 'v')}]]
    @db.schema(Sequel.qualify(:u__v, :x__y)).object_id.should == s2.object_id
    @db.schema(Sequel.qualify(Sequel.qualify(:u, :v), Sequel.qualify(:x, :y))).object_id.should == s2.object_id
  end

  specify "should correctly parse all supported data types" do
    sm = Module.new do
      def schema_parse_table(t, opts)
        [[:x, {:type=>schema_column_type(t.to_s)}]]
      end
    end
    @db.extend(sm)
    @db.schema(:tinyint).first.last[:type].should == :integer
    @db.schema(:int).first.last[:type].should == :integer
    @db.schema(:integer).first.last[:type].should == :integer
    @db.schema(:bigint).first.last[:type].should == :integer
    @db.schema(:smallint).first.last[:type].should == :integer
    @db.schema(:character).first.last[:type].should == :string
    @db.schema(:"character varying").first.last[:type].should == :string
    @db.schema(:varchar).first.last[:type].should == :string
    @db.schema(:"varchar(255)").first.last[:type].should == :string
    @db.schema(:text).first.last[:type].should == :string
    @db.schema(:date).first.last[:type].should == :date
    @db.schema(:datetime).first.last[:type].should == :datetime
    @db.schema(:timestamp).first.last[:type].should == :datetime
    @db.schema(:"timestamp with time zone").first.last[:type].should == :datetime
    @db.schema(:"timestamp without time zone").first.last[:type].should == :datetime
    @db.schema(:time).first.last[:type].should == :time
    @db.schema(:"time with time zone").first.last[:type].should == :time
    @db.schema(:"time without time zone").first.last[:type].should == :time
    @db.schema(:boolean).first.last[:type].should == :boolean
    @db.schema(:real).first.last[:type].should == :float
    @db.schema(:float).first.last[:type].should == :float
    @db.schema(:double).first.last[:type].should == :float
    @db.schema(:"double(1,2)").first.last[:type].should == :float
    @db.schema(:"double precision").first.last[:type].should == :float
    @db.schema(:number).first.last[:type].should == :decimal
    @db.schema(:numeric).first.last[:type].should == :decimal
    @db.schema(:decimal).first.last[:type].should == :decimal
    @db.schema(:"number(10,0)").first.last[:type].should == :integer
    @db.schema(:"numeric(10, 10)").first.last[:type].should == :decimal
    @db.schema(:"decimal(10,1)").first.last[:type].should == :decimal
    @db.schema(:bytea).first.last[:type].should == :blob
    @db.schema(:blob).first.last[:type].should == :blob
    @db.schema(:image).first.last[:type].should == :blob
    @db.schema(:nchar).first.last[:type].should == :string
    @db.schema(:nvarchar).first.last[:type].should == :string
    @db.schema(:ntext).first.last[:type].should == :string
    @db.schema(:smalldatetime).first.last[:type].should == :datetime
    @db.schema(:binary).first.last[:type].should == :blob
    @db.schema(:varbinary).first.last[:type].should == :blob
    @db.schema(:enum).first.last[:type].should == :enum

    @db = Sequel.mock(:host=>'postgres')
    @db.extend(sm)
    @db.schema(:interval).first.last[:type].should == :interval

    @db = Sequel.mock(:host=>'mysql')
    @db.extend(sm)
    @db.schema(:set).first.last[:type].should == :set
  end
end
