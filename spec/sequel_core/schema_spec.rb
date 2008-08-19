require File.join(File.dirname(__FILE__), 'spec_helper')

context "DB#create_table" do
  setup do
    @db = SchemaDummyDatabase.new
  end
  
  specify "should accept the table name" do
    @db.create_table(:cats) {}
    @db.sqls.should == ['CREATE TABLE cats ()']
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

  specify "should accept primary key definition" do
    @db.create_table(:cats) do
      primary_key :id
    end
    @db.sqls.should == ['CREATE TABLE cats (id integer PRIMARY KEY AUTOINCREMENT)']

    @db.sqls.clear
    @db.create_table(:cats) do
      primary_key :id, :serial, :auto_increment => false
    end
    @db.sqls.should == ['CREATE TABLE cats (id serial PRIMARY KEY)']

    @db.sqls.clear
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
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text NOT NULL)"]
  end
  
  specify "should accept null definition" do
    @db.create_table(:cats) do
      integer :id
      text :name, :null => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text NULL)"]
  end
  
  specify "should accept unique definition" do
    @db.create_table(:cats) do
      integer :id
      text :name, :unique => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text UNIQUE)"]
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
    @db.sqls.clear
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

    @db.sqls.clear
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

    @db.sqls.clear
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :cascade
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE CASCADE)"]

    @db.sqls.clear
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :no_action
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE NO ACTION)"]
    @db.sqls.clear

    @db.sqls.clear
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :set_null
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE SET NULL)"]
    @db.sqls.clear

    @db.sqls.clear
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :set_default
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE SET DEFAULT)"]
    @db.sqls.clear
  end
  
  specify "should accept inline index definition" do
    @db.create_table(:cats) do
      integer :id, :index => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX cats_id_index ON cats (id)"]
  end
  
  specify "should accept inline index definition for foreign keys" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects, :on_delete => :cascade, :index => true
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects ON DELETE CASCADE)",
      "CREATE INDEX cats_project_id_index ON cats (project_id)"]
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
    @db.sqls.should == ["CREATE TABLE cats (name text)", "CREATE UNIQUE INDEX cats_name_index ON cats (name)"]
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

  specify "should accept multiple index definitions" do
    @db.create_table(:cats) do
      integer :id
      index :id
      index :name
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer)", "CREATE INDEX cats_id_index ON cats (id)", "CREATE INDEX cats_name_index ON cats (name)"]
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
      check {(:x > 0) & (:y < 1)}
    end
    @db.sqls.should == ["CREATE TABLE cats (score integer, CHECK ((x > 0) AND (y < 1)))"]
  end

  specify "should accept unnamed constraint definitions" do
    @db.create_table(:cats) do
      check 'price < ?', 100
    end
    @db.sqls.should == ["CREATE TABLE cats (CHECK (price < 100))"]
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
      constraint(:blah_blah) {(:x > 0) & (:y < 1)}
    end
    @db.sqls.should == ["CREATE TABLE cats (CONSTRAINT blah_blah CHECK ((x > 0) AND (y < 1)))"]
  end
end

context "DB#create_table!" do
  setup do
    @db = SchemaDummyDatabase.new
  end
  
  specify "should drop the table and then create it" do
    @db.create_table!(:cats) {}
    @db.sqls.should == ['DROP TABLE cats', 'CREATE TABLE cats ()']
  end
end

context "DB#drop_table" do
  setup do
    @db = SchemaDummyDatabase.new
  end

  specify "should generate a DROP TABLE statement" do
    @db.drop_table :cats
    @db.sqls.should == ['DROP TABLE cats']
  end
end

context "DB#alter_table" do
  setup do
    @db = SchemaDummyDatabase.new
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
      add_constraint(:blah_blah) {(:x > 0) & (:y < 1)}
    end
    @db.sqls.should == ["ALTER TABLE cats ADD CONSTRAINT blah_blah CHECK ((x > 0) AND (y < 1))"]
  end

  specify "should support add_foreign_key" do
    @db.alter_table(:cats) do
      add_foreign_key :node_id, :nodes
    end
    @db.sqls.should == ["ALTER TABLE cats ADD COLUMN node_id integer REFERENCES nodes"]
  end

  specify "should support add_index" do
    @db.alter_table(:cats) do
      add_index :name
    end
    @db.sqls.should == ["CREATE INDEX cats_name_index ON cats (name)"]
  end

  specify "should support add_primary_key" do
    @db.alter_table(:cats) do
      add_primary_key :id
    end
    @db.sqls.should == ["ALTER TABLE cats ADD COLUMN id integer PRIMARY KEY AUTOINCREMENT"]
  end

  specify "should support drop_column" do
    @db.alter_table(:cats) do
      drop_column :score
    end
    @db.sqls.should == ["ALTER TABLE cats DROP COLUMN score"]
  end

  specify "should support drop_constraint" do
    @db.alter_table(:cats) do
      drop_constraint :valid_score
    end
    @db.sqls.should == ["ALTER TABLE cats DROP CONSTRAINT valid_score"]
  end

  specify "should support drop_index" do
    @db.alter_table(:cats) do
      drop_index :name
    end
    @db.sqls.should == ["DROP INDEX cats_name_index"]
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
end

context "Schema Parser" do
  setup do
    sqls = @sqls = []
    @db = Sequel::Database.new
    @db.meta_def(:dataset) do
      ds = super()
      ds.instance_variable_set(:@sqls, sqls)
      def ds.fetch_rows(sql)
        @sqls << sql
        table = /'(.*)'/.match(sql)[1]
        h = {:column=>"a", :db_type=>table, :max_chars=>nil, :numeric_precision=>nil, :default=>'', :allow_null=>'YES'}
        (h[:column] = h[:table_name] = :x) if sql =~ /BASE TABLE/
        yield h
      end
      ds
    end
  end
  after do
    Sequel.convert_tinyint_to_bool = true
  end

  specify "should parse the schema correctly for a single table" do
    @db.schema(:x).should == [[:a, {:type=>nil, :db_type=>"x", :max_chars=>nil, :numeric_precision=>nil, :default=>nil, :allow_null=>true}]]
    @sqls.should == ["SELECT column_name AS column, data_type AS db_type, character_maximum_length AS max_chars, numeric_precision, column_default AS default, is_nullable AS allow_null FROM information_schema.tables AS t INNER JOIN information_schema.columns AS c USING (table_catalog, table_schema, table_name) WHERE (c.table_name = 'x')"]
    @db.schema(:x).should == [[:a, {:type=>nil, :db_type=>"x", :max_chars=>nil, :numeric_precision=>nil, :default=>nil, :allow_null=>true}]]
    @sqls.length.should == 1
    @db.schema(:x, :reload=>true).should == [[:a, {:type=>nil, :db_type=>"x", :max_chars=>nil, :numeric_precision=>nil, :default=>nil, :allow_null=>true}]]
    @sqls.length.should == 2
  end

  specify "should parse the schema correctly for all tables" do
    @db.schema.should == {:x=>[[:x, {:type=>nil, :db_type=>"BASE TABLE", :max_chars=>nil, :numeric_precision=>nil, :default=>nil, :allow_null=>true}]]}
    @sqls.should == ["SELECT column_name AS column, data_type AS db_type, character_maximum_length AS max_chars, numeric_precision, column_default AS default, is_nullable AS allow_null, c.table_name FROM information_schema.tables AS t INNER JOIN information_schema.columns AS c USING (table_catalog, table_schema, table_name) WHERE (t.table_type = 'BASE TABLE')"]
    @db.schema.should == {:x=>[[:x, {:type=>nil, :db_type=>"BASE TABLE", :max_chars=>nil, :numeric_precision=>nil, :default=>nil, :allow_null=>true}]]}
    @sqls.length.should == 1
    @db.schema(nil, :reload=>true).should == {:x=>[[:x, {:type=>nil, :db_type=>"BASE TABLE", :max_chars=>nil, :numeric_precision=>nil, :default=>nil, :allow_null=>true}]]}
    @sqls.length.should == 2
  end

  specify "should correctly parse all supported data types" do
    @db.schema(:tinyint).first.last[:type].should == :boolean
    Sequel.convert_tinyint_to_bool = false
    @db.schema(:tinyint, :reload=>true).first.last[:type].should == :integer
    @db.schema(:int).first.last[:type].should == :integer
    @db.schema(:integer).first.last[:type].should == :integer
    @db.schema(:bigint).first.last[:type].should == :integer
    @db.schema(:smallint).first.last[:type].should == :integer
    @db.schema(:character).first.last[:type].should == :string
    @db.schema(:"character varying").first.last[:type].should == :string
    @db.schema(:varchar).first.last[:type].should == :string
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
    @db.schema(:"double precision").first.last[:type].should == :float
    @db.schema(:numeric).first.last[:type].should == :decimal
    @db.schema(:decimal).first.last[:type].should == :decimal
    @db.schema(:money).first.last[:type].should == :decimal
    @db.schema(:bytea).first.last[:type].should == :blob
  end
end
