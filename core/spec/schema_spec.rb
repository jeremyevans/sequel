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
  
  specify "should accept unique definition" do
    @db.create_table(:cats) do
      integer :id
      text :name, :unique => true
    end
    @db.sqls.should == ["CREATE TABLE cats (id integer, name text UNIQUE)"]
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

  specify "should accept foreign keys" do
    @db.create_table(:cats) do
      foreign_key :project_id, :table => :projects
    end
    @db.sqls.should == ["CREATE TABLE cats (project_id integer REFERENCES projects)"]
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
