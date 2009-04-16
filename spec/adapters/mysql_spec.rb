require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(MYSQL_USER)
  MYSQL_USER = 'root'
end
unless defined?(MYSQL_DB)
  MYSQL_URL = (ENV['SEQUEL_MY_SPEC_DB']||"mysql://#{MYSQL_USER}@localhost/sandbox") unless defined? MYSQL_URL
  MYSQL_DB = Sequel.connect(MYSQL_URL)
end
unless defined?(MYSQL_SOCKET_FILE)
  MYSQL_SOCKET_FILE = '/tmp/mysql.sock'
end

MYSQL_URI = URI.parse(MYSQL_DB.uri)

MYSQL_DB.create_table! :items do
  text :name
  integer :value, :index => true
end
MYSQL_DB.create_table! :test2 do
  text :name
  integer :value
end
MYSQL_DB.create_table! :booltest do
  tinyint :value
end
def MYSQL_DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  MYSQL_DB.sqls << msg
end
MYSQL_DB.logger = logger

if MYSQL_DB.class.adapter_scheme == :do
  SQL_BEGIN = 'Transaction.begin'
  SQL_ROLLBACK = 'Transaction.rollback'
  SQL_COMMIT = 'Transaction.commit'
else
  SQL_BEGIN = 'BEGIN'
  SQL_ROLLBACK = 'ROLLBACK'
  SQL_COMMIT = 'COMMIT'
end

context "MySQL", '#create_table' do
  before do
    @db = MYSQL_DB
    MYSQL_DB.sqls.clear
  end
  after do
    drop_table(:dolls) rescue nil
    drop_table(:tmp_dolls) rescue nil
  end
  
  specify "should allow to specify options for MySQL" do
    @db.create_table!(:dolls, :engine => 'MyISAM', :charset => 'latin2'){text :name}
    @db.sqls.last.should == "CREATE TABLE dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"
  end
  
  specify "should create a temporary table" do
    @db.create_table!(:tmp_dolls, :temp => true, :engine => 'MyISAM', :charset => 'latin2'){text :name}
    @db.sqls.last.should == "CREATE TEMPORARY TABLE tmp_dolls (name text) ENGINE=MyISAM DEFAULT CHARSET=latin2"
  end
end

context "A MySQL database" do
  before do
    @db = MYSQL_DB
  end
  after do
    Sequel.convert_tinyint_to_bool = true
  end

  specify "should provide the server version" do
    @db.server_version.should >= 40000
  end

  specify "should correctly parse the schema" do
    @db.schema(:booltest, :reload=>true).should == [[:value, {:type=>:boolean, :allow_null=>true, :primary_key=>false, :default=>nil, :db_type=>"tinyint(4)"}]]
    
    Sequel.convert_tinyint_to_bool = false
    @db.schema(:booltest, :reload=>true).should == [[:value, {:type=>:integer, :allow_null=>true, :primary_key=>false, :default=>nil, :db_type=>"tinyint(4)"}]]
  end
end

context "A MySQL dataset" do
  before do
    @d = MYSQL_DB[:items]
    @d.delete # remove all records
    MYSQL_DB.sqls.clear
  end
  
  specify "should quote columns and tables using back-ticks if quoting identifiers" do
    @d.quote_identifiers = true
    @d.select(:name).sql.should == \
      'SELECT `name` FROM `items`'
      
    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM `items`'

    @d.select(:max.sql_function(:value)).sql.should == \
      'SELECT max(`value`) FROM `items`'
      
    @d.select(:NOW.sql_function).sql.should == \
    'SELECT NOW() FROM `items`'

    @d.select(:max.sql_function(:items__value)).sql.should == \
      'SELECT max(`items`.`value`) FROM `items`'

    @d.order(:name.desc).sql.should == \
      'SELECT * FROM `items` ORDER BY `name` DESC'

    @d.select('items.name AS item_name'.lit).sql.should == \
      'SELECT items.name AS item_name FROM `items`'
      
    @d.select('`name`'.lit).sql.should == \
      'SELECT `name` FROM `items`'

    @d.select('max(items.`name`) AS `max_name`'.lit).sql.should == \
      'SELECT max(items.`name`) AS `max_name` FROM `items`'
      
    @d.select(:test.sql_function(:abc, 'hello')).sql.should == \
      "SELECT test(`abc`, 'hello') FROM `items`"

    @d.select(:test.sql_function(:abc__def, 'hello')).sql.should == \
      "SELECT test(`abc`.`def`, 'hello') FROM `items`"

    @d.select(:test.sql_function(:abc__def, 'hello').as(:x2)).sql.should == \
      "SELECT test(`abc`.`def`, 'hello') AS `x2` FROM `items`"

    @d.insert_sql(:value => 333).should == \
      'INSERT INTO `items` (`value`) VALUES (333)'

    @d.insert_sql(:x => :y).should == \
      'INSERT INTO `items` (`x`) VALUES (`y`)'
  end
  
  specify "should quote fields correctly when reversing the order" do
    @d.quote_identifiers = true
    @d.reverse_order(:name).sql.should == \
      'SELECT * FROM `items` ORDER BY `name` DESC'

    @d.reverse_order(:name.desc).sql.should == \
      'SELECT * FROM `items` ORDER BY `name` ASC'

    @d.reverse_order(:name, :test.desc).sql.should == \
      'SELECT * FROM `items` ORDER BY `name` DESC, `test` ASC'

    @d.reverse_order(:name.desc, :test).sql.should == \
      'SELECT * FROM `items` ORDER BY `name` ASC, `test` DESC'
  end
  
  specify "should support ORDER clause in UPDATE statements" do
    @d.order(:name).update_sql(:value => 1).should == \
      'UPDATE items SET value = 1 ORDER BY name'
  end
  
  specify "should support LIMIT clause in UPDATE statements" do
    @d.limit(10).update_sql(:value => 1).should == \
      'UPDATE items SET value = 1 LIMIT 10'
  end
  
  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}
    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end
  
  specify "should correctly literalize strings with comment backslashes in them" do
    @d.delete
    proc {@d << {:name => ':\\'}}.should_not raise_error
    
    @d.first[:name].should == ':\\'
  end
end

context "MySQL datasets" do
  before do
    @d = MYSQL_DB[:orders]
  end
  after do
    Sequel.convert_tinyint_to_bool = true
  end
  
  specify "should correctly quote column references" do
    @d.quote_identifiers = true
    market = 'ICE'
    ack_stamp = Time.now - 15 * 60 # 15 minutes ago
    @d.select(:market, :minute.sql_function(:from_unixtime.sql_function(:ack)).as(:minute)).
      where{|o|(:ack.sql_number > ack_stamp) & {:market => market}}.
      group_by(:minute.sql_function(:from_unixtime.sql_function(:ack))).sql.should == \
      "SELECT `market`, minute(from_unixtime(`ack`)) AS `minute` FROM `orders` WHERE ((`ack` > #{@d.literal(ack_stamp)}) AND (`market` = 'ICE')) GROUP BY minute(from_unixtime(`ack`))"
  end

  specify "should accept and return tinyints as bools or integers when configured to do so" do
    MYSQL_DB[:booltest].delete
    MYSQL_DB[:booltest] << {:value=>true}
    MYSQL_DB[:booltest].all.should == [{:value=>true}]
    MYSQL_DB[:booltest].delete
    MYSQL_DB[:booltest] << {:value=>false}
    MYSQL_DB[:booltest].all.should == [{:value=>false}]
    
    Sequel.convert_tinyint_to_bool = false
    MYSQL_DB[:booltest].delete
    MYSQL_DB[:booltest] << {:value=>true}
    MYSQL_DB[:booltest].all.should == [{:value=>1}]
    MYSQL_DB[:booltest].delete
    MYSQL_DB[:booltest] << {:value=>false}
    MYSQL_DB[:booltest].all.should == [{:value=>0}]
    
    MYSQL_DB[:booltest].delete
    MYSQL_DB[:booltest] << {:value=>1}
    MYSQL_DB[:booltest].all.should == [{:value=>1}]
    MYSQL_DB[:booltest].delete
    MYSQL_DB[:booltest] << {:value=>0}
    MYSQL_DB[:booltest].all.should == [{:value=>0}]
  end
end

context "MySQL join expressions" do
  before do
    @ds = MYSQL_DB[:nodes]
    @ds.db.meta_def(:server_version) {50014}
  end

  specify "should raise error for :full_outer join requests." do
    lambda{@ds.join_table(:full_outer, :nodes)}.should raise_error(Sequel::Error)
  end
  specify "should support natural left joins" do
    @ds.join_table(:natural_left, :nodes).sql.should == \
      'SELECT * FROM nodes NATURAL LEFT JOIN nodes'
  end
  specify "should support natural right joins" do
    @ds.join_table(:natural_right, :nodes).sql.should == \
      'SELECT * FROM nodes NATURAL RIGHT JOIN nodes'
  end
  specify "should support natural left outer joins" do
    @ds.join_table(:natural_left_outer, :nodes).sql.should == \
      'SELECT * FROM nodes NATURAL LEFT OUTER JOIN nodes'
  end
  specify "should support natural right outer joins" do
    @ds.join_table(:natural_right_outer, :nodes).sql.should == \
      'SELECT * FROM nodes NATURAL RIGHT OUTER JOIN nodes'
  end
  specify "should support natural inner joins" do
    @ds.join_table(:natural_inner, :nodes).sql.should == \
      'SELECT * FROM nodes NATURAL LEFT JOIN nodes'
  end
  specify "should support cross joins" do
    @ds.join_table(:cross, :nodes).sql.should == \
      'SELECT * FROM nodes CROSS JOIN nodes'
  end
  specify "should support cross joins as inner joins if conditions are used" do
    @ds.join_table(:cross, :nodes, :id=>:id).sql.should == \
      'SELECT * FROM nodes INNER JOIN nodes ON (nodes.id = nodes.id)'
  end
  specify "should support straight joins (force left table to be read before right)" do
    @ds.join_table(:straight, :nodes).sql.should == \
      'SELECT * FROM nodes STRAIGHT_JOIN nodes'
  end
  specify "should support natural joins on multiple tables." do
    @ds.join_table(:natural_left_outer, [:nodes, :branches]).sql.should == \
      'SELECT * FROM nodes NATURAL LEFT OUTER JOIN (nodes, branches)'
  end
  specify "should support straight joins on multiple tables." do
    @ds.join_table(:straight, [:nodes,:branches]).sql.should == \
      'SELECT * FROM nodes STRAIGHT_JOIN (nodes, branches)'
  end
end

context "Joined MySQL dataset" do
  before do
    @ds = MYSQL_DB[:nodes]
  end
  
  specify "should quote fields correctly" do
    @ds.quote_identifiers = true
    @ds.join(:attributes, :node_id => :id).sql.should == \
      "SELECT * FROM `nodes` INNER JOIN `attributes` ON (`attributes`.`node_id` = `nodes`.`id`)"
  end
  
  specify "should allow a having clause on ungrouped datasets" do
    proc {@ds.having('blah')}.should_not raise_error

    @ds.having('blah').sql.should == \
      "SELECT * FROM nodes HAVING (blah)"
  end
  
  specify "should put a having clause before an order by clause" do
    @ds.order(:aaa).having(:bbb => :ccc).sql.should == \
      "SELECT * FROM nodes HAVING (bbb = ccc) ORDER BY aaa"
  end
end

context "A MySQL database" do
  before do
    @db = MYSQL_DB
  end

  specify "should support add_column operations" do
    @db.add_column :test2, :xyz, :text
    
    @db[:test2].columns.should == [:name, :value, :xyz]
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => '000'}
    @db[:test2].first[:xyz].should == '000'
  end
  
  specify "should support drop_column operations" do
    @db[:test2].columns.should == [:name, :value, :xyz]
    @db.drop_column :test2, :xyz
    
    @db[:test2].columns.should == [:name, :value]
  end
  
  specify "should support rename_column operations" do
    @db[:test2].delete
    @db.add_column :test2, :xyz, :text
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 'qqqq'}

    @db[:test2].columns.should == [:name, :value, :xyz]
    @db.rename_column :test2, :xyz, :zyx, :type => :text
    @db[:test2].columns.should == [:name, :value, :zyx]
    @db[:test2].first[:zyx].should == 'qqqq'
  end
  
  specify "should support rename_column operations with types like varchar(255)" do
    @db[:test2].delete
    @db.add_column :test2, :tre, :text
    @db[:test2] << {:name => 'mmm', :value => 111, :tre => 'qqqq'}

    @db[:test2].columns.should == [:name, :value, :zyx, :tre]
    @db.rename_column :test2, :tre, :ert, :type => :varchar, :size=>255
    @db[:test2].columns.should == [:name, :value, :zyx, :ert]
    @db[:test2].first[:ert].should == 'qqqq'
  end
  
  specify "should support set_column_type operations" do
    @db.add_column :test2, :xyz, :float
    @db[:test2].delete
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 56.78}
    @db.set_column_type :test2, :xyz, :integer
    
    @db[:test2].first[:xyz].should == 57
  end
  
  specify "should support add_index" do
    @db.add_index :test2, :value
  end
  
  specify "should support drop_index" do
    @db.drop_index :test2, :value
  end
  
  specify "should support add_foreign_key" do
    @db.alter_table :test2 do
      add_foreign_key :value2, :test2, :key=>:value
    end
    @db[:test2].columns.should == [:name, :value, :zyx, :ert, :xyz, :value2]
  end
end  

context "A MySQL database", "with table options" do
  before(:all) do
    @options = {}
    @options[:engine] = 'MyISAM'
    @options[:charset] = 'latin2'
    @options[:collate] = 'swedish'
    
    Sequel::MySQL.default_engine = 'InnoDB'
    Sequel::MySQL.default_charset = 'utf8'
    Sequel::MySQL.default_collate = 'utf8'    
    
    @db = MYSQL_DB
    @g = Sequel::Schema::Generator.new(@db) do
      integer :size
      text :name
    end
  end
  
  after(:all) do
    Sequel::MySQL.default_engine = nil
    Sequel::MySQL.default_charset = nil
    Sequel::MySQL.default_collate = nil
  end
  
  specify "should allow to pass custom options (engine, charset, collate) for table creation" do
    statements = @db.send(:create_table_sql_list, :items, *(@g.create_info << @options))
    statements.should == [
      "CREATE TABLE items (size integer, name text) ENGINE=MyISAM DEFAULT CHARSET=latin2 DEFAULT COLLATE=swedish"
    ]
  end
  
  specify "should use default options if specified (engine, charset, collate) for table creation" do
    statements = @db.send(:create_table_sql_list, :items, *(@g.create_info))
    statements.should == [
      "CREATE TABLE items (size integer, name text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8"
    ]
  end
  
  specify "should not use default if option has a nil value" do
    statements = @db.send(:create_table_sql_list, :items, *(@g.create_info << {:engine=>nil, :charset=>nil, :collate=>nil}))
    statements.should == [
      "CREATE TABLE items (size integer, name text)"
    ]
  end
end

context "A MySQL database" do
  before do
    @db = MYSQL_DB
  end
  
  specify "should support defaults for boolean columns" do
    g = Sequel::Schema::Generator.new(@db) do
      boolean :active1, :default => true
      boolean :active2, :default => false
    end
    statements = @db.send(:create_table_sql_list, :items, *g.create_info)
    statements.should == [
      "CREATE TABLE items (active1 boolean DEFAULT 1, active2 boolean DEFAULT 0)"
    ]
  end
  
  specify "should correctly format CREATE TABLE statements with foreign keys" do
    g = Sequel::Schema::Generator.new(@db) do
      foreign_key :p_id, :table => :users, :key => :id, 
        :null => false, :on_delete => :cascade
    end
    @db.send(:create_table_sql_list, :items, *g.create_info).should == [
      "CREATE TABLE items (p_id integer NOT NULL, FOREIGN KEY (p_id) REFERENCES users(id) ON DELETE CASCADE)"
    ]
  end
  
specify "should correctly format ALTER TABLE statements with foreign keys" do
  g = Sequel::Schema::AlterTableGenerator.new(@db) do
    add_foreign_key :p_id, :users, :key => :id, :null => false, :on_delete => :cascade
  end
  @db.send(:alter_table_sql_list, :items, g.operations).should == [[
    "ALTER TABLE items ADD COLUMN p_id integer NOT NULL",
    "ALTER TABLE items ADD FOREIGN KEY (p_id) REFERENCES users(id) ON DELETE CASCADE"
  ]]
end
  
  specify "should accept repeated raw sql statements using Database#<<" do
    @db << 'DELETE FROM items'
    @db[:items].count.should == 0
    
    @db << "INSERT INTO items (name, value) VALUES ('tutu', 1234)"
    @db[:items].first.should == {:name => 'tutu', :value => 1234}
    
    @db << 'DELETE FROM items'
    @db[:items].first.should == nil
  end
  
  specify "should handle multiple select statements at once" do
    @db[:items].delete
    @db[:items].insert(:name => 'tutu', :value => 1234)
    @db["SELECT * FROM items; SELECT * FROM items"].all.should == \
      [{:name => 'tutu', :value => 1234}, {:name => 'tutu', :value => 1234}]
  end
end  

# Socket tests should only be run if the MySQL server is on localhost
if %w'localhost 127.0.0.1 ::1'.include?(MYSQL_URI.host) and MYSQL_DB.class.adapter_scheme == :mysql
  context "A MySQL database" do
    specify "should accept a socket option" do
      db = Sequel.mysql(MYSQL_DB.opts[:database], :host => 'localhost', :user => MYSQL_DB.opts[:user], :password => MYSQL_DB.opts[:password], :socket => MYSQL_SOCKET_FILE)
      proc {db.test_connection}.should_not raise_error
    end
    
    specify "should accept a socket option without host option" do
      db = Sequel.mysql(MYSQL_DB.opts[:database], :user => MYSQL_DB.opts[:user], :password => MYSQL_DB.opts[:password], :socket => MYSQL_SOCKET_FILE)
      proc {db.test_connection}.should_not raise_error
    end
    
    specify "should fail to connect with invalid socket" do
      db = Sequel.mysql(MYSQL_DB.opts[:database], :user => MYSQL_DB.opts[:user], :password => MYSQL_DB.opts[:password], :socket =>'blah')
      proc {db.test_connection}.should raise_error
    end
  end
end

context "A grouped MySQL dataset" do
  before do
    MYSQL_DB[:test2].delete
    MYSQL_DB[:test2] << {:name => '11', :value => 10}
    MYSQL_DB[:test2] << {:name => '11', :value => 20}
    MYSQL_DB[:test2] << {:name => '11', :value => 30}
    MYSQL_DB[:test2] << {:name => '12', :value => 10}
    MYSQL_DB[:test2] << {:name => '12', :value => 20}
    MYSQL_DB[:test2] << {:name => '13', :value => 10}
  end
  
  specify "should return the correct count for raw sql query" do
    ds = MYSQL_DB["select name FROM test2 WHERE name = '11' GROUP BY name"]
    ds.count.should == 1
  end
  
  specify "should return the correct count for a normal dataset" do
    ds = MYSQL_DB[:test2].select(:name).where(:name => '11').group(:name)
    ds.count.should == 1
  end
end

context "A MySQL database" do
  specify "should support fulltext indexes" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      text :title
      text :body
      full_text_index [:title, :body]
    end
    MYSQL_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [
      "CREATE TABLE posts (title text, body text)",
      "CREATE FULLTEXT INDEX posts_title_body_index ON posts (title, body)"
    ]
  end
  
  specify "should support full_text_search" do
    MYSQL_DB[:posts].full_text_search(:title, 'ruby').sql.should ==
      "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('ruby'))"
    
    MYSQL_DB[:posts].full_text_search([:title, :body], ['ruby', 'sequel']).sql.should ==
      "SELECT * FROM posts WHERE (MATCH (title, body) AGAINST ('ruby', 'sequel'))"
      
    MYSQL_DB[:posts].full_text_search(:title, '+ruby -rails', :boolean => true).sql.should ==
      "SELECT * FROM posts WHERE (MATCH (title) AGAINST ('+ruby -rails' IN BOOLEAN MODE))"
  end

  specify "should support spatial indexes" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      point :geom
      spatial_index [:geom]
    end
    MYSQL_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [
      "CREATE TABLE posts (geom point)",
      "CREATE SPATIAL INDEX posts_geom_index ON posts (geom)"
    ]
  end

  specify "should support indexes with index type" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      text :title
      index :title, :type => :hash
    end
    MYSQL_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [
      "CREATE TABLE posts (title text)",
      "CREATE INDEX posts_title_index ON posts (title) USING hash"
    ]
  end

  specify "should support unique indexes with index type" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      text :title
      index :title, :type => :hash, :unique => true
    end
    MYSQL_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [
      "CREATE TABLE posts (title text)",
      "CREATE UNIQUE INDEX posts_title_index ON posts (title) USING hash"
    ]
  end
end

context "MySQL::Dataset#insert" do
  before do
    @d = MYSQL_DB[:items]
    @d.delete
    MYSQL_DB.sqls.clear
  end

  specify "should insert record with default values when no arguments given" do
    @d.insert
    
    MYSQL_DB.sqls.should == [
      "INSERT INTO items () VALUES ()"
    ]
    
    @d.all.should == [
      {:name => nil, :value => nil}
    ]
  end

  specify "should insert record with default values when empty hash given" do
    @d.insert({})
    
    MYSQL_DB.sqls.should == [
      "INSERT INTO items () VALUES ()"
    ]
    
    @d.all.should == [
      {:name => nil, :value => nil}
    ]
  end

  specify "should insert record with default values when empty array given" do
    @d.insert []
    
    MYSQL_DB.sqls.should == [
      "INSERT INTO items () VALUES ()"
    ]
    
    @d.all.should == [
      {:name => nil, :value => nil}
    ]
  end
end

context "MySQL::Dataset#multi_insert" do
  before do
    @d = MYSQL_DB[:items]
    @d.delete
    MYSQL_DB.sqls.clear
  end
  
  specify "should insert multiple records in a single statement" do
    @d.multi_insert([{:name => 'abc'}, {:name => 'def'}])
    
    MYSQL_DB.sqls.should == [
      SQL_BEGIN,
      "INSERT INTO items (name) VALUES ('abc'), ('def')",
      SQL_COMMIT
    ]

    @d.all.should == [
      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
    ]
  end

  specify "should split the list of records into batches if :commit_every option is given" do
    @d.multi_insert([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
      :commit_every => 2)

    MYSQL_DB.sqls.should == [
      SQL_BEGIN,
      "INSERT INTO items (value) VALUES (1), (2)",
      SQL_COMMIT,
      SQL_BEGIN,
      "INSERT INTO items (value) VALUES (3), (4)",
      SQL_COMMIT
    ]
    
    @d.all.should == [
      {:name => nil, :value => 1}, 
      {:name => nil, :value => 2},
      {:name => nil, :value => 3}, 
      {:name => nil, :value => 4}
    ]
  end

  specify "should split the list of records into batches if :slice option is given" do
    @d.multi_insert([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
      :slice => 2)

    MYSQL_DB.sqls.should == [
      SQL_BEGIN,
      "INSERT INTO items (value) VALUES (1), (2)",
      SQL_COMMIT,
      SQL_BEGIN,
      "INSERT INTO items (value) VALUES (3), (4)",
      SQL_COMMIT
    ]
    
    @d.all.should == [
      {:name => nil, :value => 1}, 
      {:name => nil, :value => 2},
      {:name => nil, :value => 3}, 
      {:name => nil, :value => 4}
    ]
  end
  
  specify "should support inserting using columns and values arrays" do
    @d.import([:name, :value], [['abc', 1], ['def', 2]])

    MYSQL_DB.sqls.should == [
      SQL_BEGIN,
      "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2)",
      SQL_COMMIT
    ]
    
    @d.all.should == [
      {:name => 'abc', :value => 1}, 
      {:name => 'def', :value => 2}
    ]
  end
end

context "MySQL::Dataset#insert_ignore" do
  before do
    @d = MYSQL_DB[:items]
    @d.delete
    MYSQL_DB.sqls.clear
  end
  
  specify "should add the IGNORE keyword when inserting" do
    @d.insert_ignore.multi_insert([{:name => 'abc'}, {:name => 'def'}])
    
    MYSQL_DB.sqls.should == [
      SQL_BEGIN,
      "INSERT IGNORE INTO items (name) VALUES ('abc'), ('def')",
      SQL_COMMIT
    ]

    @d.all.should == [
      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
    ]
  end
end

context "MySQL::Dataset#on_duplicate_key_update" do
  before do
    @d = MYSQL_DB[:items]
    @d.delete
    MYSQL_DB.sqls.clear
  end
  
  specify "should add the ON DUPLICATE KEY UPDATE and ALL columns when no args given" do
    @d.on_duplicate_key_update.import([:name,:value], 
      [['abc', 1], ['def',2]]
    )
    
    MYSQL_DB.sqls.should == [
      SQL_BEGIN,
      "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE name=VALUES(name), value=VALUES(value)",
      SQL_COMMIT
    ]

    @d.all.should == [
      {:name => 'abc', :value => 1}, {:name => 'def', :value => 2}
    ]
  end
  specify "should add the ON DUPLICATE KEY UPDATE and columns specified when args are given" do
    @d.on_duplicate_key_update(:value).import([:name,:value], 
      [['abc', 1], ['def',2]]
    )
    
    MYSQL_DB.sqls.should == [
      SQL_BEGIN,
      "INSERT INTO items (name, value) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE value=VALUES(value)",
      SQL_COMMIT
    ]

    @d.all.should == [
      {:name => 'abc', :value => 1}, {:name => 'def', :value => 2}
    ]
  end
  
end

context "MySQL::Dataset#replace" do
  before do
    MYSQL_DB.drop_table(:items) if MYSQL_DB.table_exists?(:items)
    MYSQL_DB.create_table :items do
      integer :id, :unique => true
      integer :value, :index => true
    end
    @d = MYSQL_DB[:items]
    MYSQL_DB.sqls.clear
  end
  
  specify "should create a record if the condition is not met" do
    @d.replace(:id => 111, :value => 333)
    @d.all.should == [{:id => 111, :value => 333}]
  end
  
  specify "should update a record if the condition is met" do
    @d << {:id => 111}
    @d.all.should == [{:id => 111, :value => nil}]
    @d.replace(:id => 111, :value => 333)
    @d.all.should == [{:id => 111, :value => 333}]
  end
end

context "MySQL::Dataset#complex_expression_sql" do
  before do
    @d = MYSQL_DB.dataset
  end

  specify "should handle pattern matches correctly" do
    @d.literal(:x.like('a')).should == "(x LIKE BINARY 'a')"
    @d.literal(~:x.like('a')).should == "(x NOT LIKE BINARY 'a')"
    @d.literal(:x.ilike('a')).should == "(x LIKE 'a')"
    @d.literal(~:x.ilike('a')).should == "(x NOT LIKE 'a')"
    @d.literal(:x.like(/a/)).should == "(x REGEXP BINARY 'a')"
    @d.literal(~:x.like(/a/)).should == "(x NOT REGEXP BINARY 'a')"
    @d.literal(:x.like(/a/i)).should == "(x REGEXP 'a')"
    @d.literal(~:x.like(/a/i)).should == "(x NOT REGEXP 'a')"
  end

  specify "should handle string concatenation with CONCAT if more than one record" do
    @d.literal([:x, :y].sql_string_join).should == "CONCAT(x, y)"
    @d.literal([:x, :y].sql_string_join(' ')).should == "CONCAT(x, ' ', y)"
    @d.literal([:x.sql_function(:y), 1, 'z'.lit].sql_string_join(:y.sql_subscript(1))).should == "CONCAT(x(y), y[1], '1', y[1], z)"
  end

  specify "should handle string concatenation as simple string if just one record" do
    @d.literal([:x].sql_string_join).should == "x"
    @d.literal([:x].sql_string_join(' ')).should == "x"
  end
end

unless MYSQL_DB.class.adapter_scheme == :do
  context "MySQL Stored Procedures" do
    after do
      MYSQL_DB.execute('DROP PROCEDURE test_sproc')
    end
    
    specify "should be callable on the database object" do
      MYSQL_DB.execute('CREATE PROCEDURE test_sproc() BEGIN DELETE FROM items; END')
      MYSQL_DB[:items].delete
      MYSQL_DB[:items].insert(:value=>1)
      MYSQL_DB[:items].count.should == 1
      MYSQL_DB.call_sproc(:test_sproc)
      MYSQL_DB[:items].count.should == 0
    end
    
    specify "should be callable on the dataset object" do
      MYSQL_DB.execute('CREATE PROCEDURE test_sproc(a INTEGER) BEGIN SELECT *, a AS b FROM items; END')
      MYSQL_DB[:items].delete
      @d = MYSQL_DB[:items]
      @d.call_sproc(:select, :test_sproc, 3).should == []
      @d.insert(:value=>1)
      @d.call_sproc(:select, :test_sproc, 4).should == [{:id=>nil, :value=>1, :b=>4}]
      @d.row_proc = proc{|r| r.keys.each{|k| r[k] *= 2 if r[k].is_a?(Integer)}; r}
      @d.call_sproc(:select, :test_sproc, 3).should == [{:id=>nil, :value=>2, :b=>6}]
    end
    
    specify "should be callable on the dataset object with multiple arguments" do
      MYSQL_DB.execute('CREATE PROCEDURE test_sproc(a INTEGER, c INTEGER) BEGIN SELECT *, a AS b, c AS d FROM items; END')
      MYSQL_DB[:items].delete
      @d = MYSQL_DB[:items]
      @d.call_sproc(:select, :test_sproc, 3, 4).should == []
      @d.insert(:value=>1)
      @d.call_sproc(:select, :test_sproc, 4, 5).should == [{:id=>nil, :value=>1, :b=>4, :d=>5}]
      @d.row_proc = proc{|r| r.keys.each{|k| r[k] *= 2 if r[k].is_a?(Integer)}; r}
      @d.call_sproc(:select, :test_sproc, 3, 4).should == [{:id=>nil, :value=>2, :b=>6, :d => 8}]
    end
  end
end

if MYSQL_DB.class.adapter_scheme == :mysql
  context "MySQL bad date/time conversions" do
    after do
      Sequel::MySQL.convert_invalid_date_time = false
    end
  
    specify "should raise an exception when a bad date/time is used and convert_invalid_date_time is false" do
      Sequel::MySQL.convert_invalid_date_time = false
      proc{MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value}.should raise_error(Sequel::InvalidValue)
      proc{MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value}.should raise_error(Sequel::InvalidValue)
      proc{MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value}.should raise_error(Sequel::InvalidValue)
    end
  
    specify "should not use a nil value bad date/time is used and convert_invalid_date_time is nil or :nil" do
      Sequel::MySQL.convert_invalid_date_time = nil
      MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == nil
      MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == nil
      MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value.should == nil
      Sequel::MySQL.convert_invalid_date_time = :nil
      MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == nil
      MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == nil
      MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value.should == nil
    end
  
    specify "should not use a nil value bad date/time is used and convert_invalid_date_time is :string" do
      Sequel::MySQL.convert_invalid_date_time = :string
      MYSQL_DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == '0000-00-00'
      MYSQL_DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == '0000-00-00 00:00:00'
      MYSQL_DB["SELECT CAST('25:00:00' AS time)"].single_value.should == '25:00:00'
    end
  end
end
