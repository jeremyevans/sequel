require File.join(File.dirname(__FILE__), '../../lib/sequel_core')
require File.join(File.dirname(__FILE__), '../spec_helper.rb')
require 'logger'

unless defined?(MYSQL_DB)
  MYSQL_URL = 'mysql://root@localhost/sandbox' unless defined? MYSQL_URL
  MYSQL_DB = Sequel(MYSQL_URL)
end
unless defined?(MYSQL_SOCKET_FILE)
  MYSQL_SOCKET_FILE = '/tmp/mysql.sock'
end

MYSQL_URI = URI.parse(MYSQL_DB.uri)
MYSQL_DB_NAME = MYSQL_URI.path =~ /\/(.*)/ && $1

MYSQL_DB.drop_table(:items) if MYSQL_DB.table_exists?(:items)
MYSQL_DB.drop_table(:test2) if MYSQL_DB.table_exists?(:test2)
MYSQL_DB.create_table :items do
  text :name
  integer :value, :index => true
end
MYSQL_DB.create_table :test2 do
  text :name
  integer :value
end

context "A MySQL database" do
  setup do
    @db = MYSQL_DB
  end
  
  specify "should provide disconnect functionality" do
    @db.tables
    @db.pool.size.should == 1
    @db.disconnect
    @db.pool.size.should == 0
  end

  specify "should provide the server version" do
    @db.server_version.should >= 40000
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
end

context "A MySQL dataset" do
  setup do
    @d = MYSQL_DB[:items]
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
  
  specify "should quote columns using back-ticks" do
    @d.select(:name).sql.should == \
      'SELECT `name` FROM items'
      
    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM items'

    @d.select(:value.MAX).sql.should == \
      'SELECT max(`value`) FROM items'
      
    @d.select(:NOW[]).sql.should == \
    'SELECT NOW() FROM items'

    @d.select(:items__value.MAX).sql.should == \
      'SELECT max(items.`value`) FROM items'

    @d.order(:name.DESC).sql.should == \
      'SELECT * FROM items ORDER BY `name` DESC'

    @d.select('items.name AS item_name'.lit).sql.should == \
      'SELECT items.name AS item_name FROM items'
      
    @d.select('`name`'.lit).sql.should == \
      'SELECT `name` FROM items'

    @d.select('max(items.`name`) AS `max_name`'.lit).sql.should == \
      'SELECT max(items.`name`) AS `max_name` FROM items'
      
    @d.select(:test[:abc, 'hello']).sql.should == \
      "SELECT test(`abc`, 'hello') FROM items"

    @d.select(:test[:abc__def, 'hello']).sql.should == \
      "SELECT test(abc.`def`, 'hello') FROM items"

    @d.select(:test[:abc__def, 'hello'].as(:x2)).sql.should == \
      "SELECT test(abc.`def`, 'hello') AS `x2` FROM items"

    @d.insert_sql(:value => 333).should == \
      'INSERT INTO items (`value`) VALUES (333)'

    @d.insert_sql(:x => :y).should == \
      'INSERT INTO items (`x`) VALUES (`y`)'
  end
  
  specify "should quote fields correctly when reversing the order" do
    @d.reverse_order(:name).sql.should == \
      'SELECT * FROM items ORDER BY `name` DESC'

    @d.reverse_order(:name.DESC).sql.should == \
      'SELECT * FROM items ORDER BY `name`'

    @d.reverse_order(:name, :test.DESC).sql.should == \
      'SELECT * FROM items ORDER BY `name` DESC, `test`'

    @d.reverse_order(:name.DESC, :test).sql.should == \
      'SELECT * FROM items ORDER BY `name`, `test` DESC'
  end
  
  specify "should support ORDER clause in UPDATE statements" do
    @d.order(:name).update_sql(:value => 1).should == \
      'UPDATE items SET `value` = 1 ORDER BY `name`'
  end
  
  specify "should support LIMIT clause in UPDATE statements" do
    @d.limit(10).update_sql(:value => 1).should == \
      'UPDATE items SET `value` = 1 LIMIT 10'
  end
  
  specify "should support transactions" do
    MYSQL_DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.should == 1
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
  setup do
    @d = MYSQL_DB[:orders]
  end
  
  specify "should correctly quote column references" do
    market = 'ICE'
    ack_stamp = Time.now - 15 * 60 # 15 minutes ago
    @d.query do
      select :market, :minute[:from_unixtime[:ack]].as(:minute)
      where do
        :ack > ack_stamp
        :market == market
      end
      group_by :minute[:from_unixtime[:ack]]
    end.sql.should == \
      "SELECT `market`, minute(from_unixtime(`ack`)) AS `minute` FROM orders WHERE ((`ack` > #{@d.literal(ack_stamp)}) AND (`market` = 'ICE')) GROUP BY minute(from_unixtime(`ack`))"
  end
end

# # Commented out because it was causing subsequent examples to fail for some reason
# context "Simple stored procedure test" do
#   setup do
#     # Create a simple stored procedure but drop it first if there
#     MYSQL_DB.execute("DROP PROCEDURE IF EXISTS sp_get_server_id;")
#     MYSQL_DB.execute("CREATE PROCEDURE sp_get_server_id() SQL SECURITY DEFINER SELECT @@SERVER_ID as server_id;")
#   end
# 
#   specify "should return the server-id via a stored procedure call" do
#     @server_id = MYSQL_DB["SELECT @@SERVER_ID as server_id;"].first[:server_id] # grab the server_id via a simple query
#     @server_id_by_sp = MYSQL_DB["CALL sp_get_server_id();"].first[:server_id]
#     @server_id_by_sp.should == @server_id  # compare it to output from stored procedure
#   end
# end
# 
context "MySQL join expressions" do
  setup do
    @ds = MYSQL_DB[:nodes]
    @ds.db.meta_def(:server_version) {50014}
  end

  specify "should raise error for :full_outer join requests." do
    lambda{@ds.join_expr(:full_outer, :nodes)}.should raise_error(Sequel::Error::InvalidJoinType)
  end
  specify "should support natural left joins" do
    @ds.join_expr(:natural_left, :nodes).should == \
      'NATURAL LEFT JOIN nodes'
  end
  specify "should support natural right joins" do
    @ds.join_expr(:natural_right, :nodes).should == \
      'NATURAL RIGHT JOIN nodes'
  end
  specify "should support natural left outer joins" do
    @ds.join_expr(:natural_left_outer, :nodes).should == \
      'NATURAL LEFT OUTER JOIN nodes'
  end
  specify "should support natural right outer joins" do
    @ds.join_expr(:natural_right_outer, :nodes).should == \
      'NATURAL RIGHT OUTER JOIN nodes'
  end
  specify "should support natural inner joins" do
    @ds.join_expr(:natural_inner, :nodes).should == \
      'NATURAL LEFT JOIN nodes'
  end
  specify "should support cross joins (equivalent to inner join in MySQL, not in std SQL)" do
    @ds.join_expr(:cross, :nodes).should == \
      'INNER JOIN nodes'
  end
  specify "should support straight joins (force left table to be read before right)" do
    @ds.join_expr(:straight, :nodes).should == \
      'STRAIGHT_JOIN nodes'
  end
  specify "should support natural joins on multiple tables." do
    @ds.join_expr(:natural_left_outer, [:nodes, :branches]).should == \
      'NATURAL LEFT OUTER JOIN ( `nodes`, `branches` )'
  end
  specify "should support straight joins on multiple tables." do
    @ds.join_expr(:straight, [:nodes,:branches]).should == \
      'STRAIGHT_JOIN ( `nodes`, `branches` )'
  end
end

context "Joined MySQL dataset" do
  setup do
    @ds = MYSQL_DB[:nodes].join(:attributes, :node_id => :id)
    @ds2 = MYSQL_DB[:nodes]
  end
  
  specify "should quote fields correctly" do
    @ds.sql.should == \
      "SELECT * FROM nodes INNER JOIN attributes ON (attributes.`node_id` = nodes.`id`)"
  end
  
  specify "should allow a having clause on ungrouped datasets" do
    proc {@ds2.having('blah')}.should_not raise_error

    @ds2.having('blah').sql.should == \
      "SELECT * FROM nodes HAVING blah"
  end
  
  specify "should put a having clause before an order by clause" do
    @ds2.order(:aaa).having(:bbb => :ccc).sql.should == \
      "SELECT * FROM nodes HAVING (`bbb` = `ccc`) ORDER BY `aaa`"
  end
end

context "A MySQL database" do
  setup do
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
    @db.rename_column :test2, :tre, :ert, :type => :varchar[255]
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
end  

context "A MySQL database" do
  setup do
    @db = MYSQL_DB
  end
  
  specify "should support defaults for boolean columns" do
    g = Sequel::Schema::Generator.new(@db) do
      boolean :active1, :default => true
      boolean :active2, :default => false
    end
    statements = @db.create_table_sql_list(:items, *g.create_info)
    statements.should == [
      "CREATE TABLE items (`active1` boolean DEFAULT 1, `active2` boolean DEFAULT 0)"
    ]
  end
  
  specify "should correctly format CREATE TABLE statements with foreign keys" do
    g = Sequel::Schema::Generator.new(@db) do
      foreign_key :p_id, :table => :users, :key => :id, 
        :null => false, :on_delete => :cascade
    end
    @db.create_table_sql_list(:items, *g.create_info).should == [
      "CREATE TABLE items (`p_id` integer NOT NULL, FOREIGN KEY (`p_id`) REFERENCES users(`id`) ON DELETE CASCADE)"
    ]
  end
  
  specify "should accept repeated raw sql statements using Database#<<" do
    @db << 'DELETE FROM items'
    @db[:items].count.should == 0
    
    @db << "INSERT INTO items (name, value) VALUES ('tutu', 1234)"
    @db[:items].first.should == {:name => 'tutu', :value => 1234}
    
    @db << 'DELETE FROM items'
    @db[:items].first.should == nil
  end
end  

context "A MySQL database" do
  specify "should accept a socket option" do
    db = Sequel.mysql(MYSQL_DB_NAME, :host => 'localhost', :user => 'root', :socket => MYSQL_SOCKET_FILE)
    proc {db.test_connection}.should_not raise_error
  end
  
  specify "should accept a socket option without host option" do
    db = Sequel.mysql(MYSQL_DB_NAME, :user => 'root', :socket => MYSQL_SOCKET_FILE)
    proc {db.test_connection}.should_not raise_error
  end
  
  specify "should fail to connect with invalid socket" do
    db = Sequel.mysql(MYSQL_DB_NAME, :host => 'localhost', :user => 'root', :socket => 'blah')
    proc {db.test_connection}.should raise_error
  end
end

context "A grouped MySQL dataset" do
  setup do
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
  setup do
  end
  
  specify "should support fulltext indexes" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      text :title
      text :body
      full_text_index [:title, :body]
    end
    MYSQL_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (`title` text, `body` text)",
      "CREATE FULLTEXT INDEX posts_title_body_index ON posts (`title`, `body`)"
    ]
  end
  
  specify "should support full_text_search" do
    MYSQL_DB[:posts].full_text_search(:title, 'ruby').sql.should ==
      "SELECT * FROM posts WHERE (MATCH (`title`) AGAINST ('ruby'))"
    
    MYSQL_DB[:posts].full_text_search([:title, :body], ['ruby', 'sequel']).sql.should ==
      "SELECT * FROM posts WHERE (MATCH (`title`, `body`) AGAINST ('ruby', 'sequel'))"
      
    MYSQL_DB[:posts].full_text_search(:title, '+ruby -rails', :boolean => true).sql.should ==
      "SELECT * FROM posts WHERE (MATCH (`title`) AGAINST ('+ruby -rails' IN BOOLEAN MODE))"
  end

  specify "should support spatial indexes" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      point :geom
      spatial_index [:geom]
    end
    MYSQL_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (`geom` point)",
      "CREATE SPATIAL INDEX posts_geom_index ON posts (`geom`)"
    ]
  end

  specify "should support indexes with index type" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      text :title
      index :title, :type => :hash
    end
    MYSQL_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (`title` text)",
      "CREATE INDEX posts_title_index ON posts (`title`) USING hash"
    ]
  end

  specify "should support unique indexes with index type" do
    g = Sequel::Schema::Generator.new(MYSQL_DB) do
      text :title
      index :title, :type => :hash, :unique => true
    end
    MYSQL_DB.create_table_sql_list(:posts, *g.create_info).should == [
      "CREATE TABLE posts (`title` text)",
      "CREATE UNIQUE INDEX posts_title_index ON posts (`title`) USING hash"
    ]
  end
end

class Sequel::MySQL::Database
  alias_method :orig_execute, :execute
  attr_accessor :sqls
  def execute(sql, &block)
    @sqls ||= []; @sqls << sql
    orig_execute(sql, &block)
  end

  def transaction
    @pool.hold do |conn|
      @transactions ||= []
      if @transactions.include? Thread.current
        return yield(conn)
      end
      @sqls ||= []; @sqls << SQL_BEGIN
      conn.query(SQL_BEGIN)
      begin
        @transactions << Thread.current
        result = yield(conn)
        @sqls ||= []; @sqls << SQL_COMMIT
        conn.query(SQL_COMMIT)
        result
      rescue => e
        @sqls ||= []; @sqls << SQL_ROLLBACK
        conn.query(SQL_ROLLBACK)
        raise e unless Sequel::Error::Rollback === e
      ensure
        @transactions.delete(Thread.current)
      end
    end
  end
end

context "MySQL::Dataset#insert" do
  setup do
    @d = MYSQL_DB[:items]
    @d.delete # remove all records
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
    @d.insert {}
    
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
  setup do
    @d = MYSQL_DB[:items]
    @d.delete # remove all records
    MYSQL_DB.sqls.clear
  end
  
  specify "should insert multiple records in a single statement" do
    @d.multi_insert([{:name => 'abc'}, {:name => 'def'}])
    
    MYSQL_DB.sqls.should == [
      'BEGIN',
      "INSERT INTO items (`name`) VALUES ('abc'), ('def')",
      'COMMIT'
    ]

    @d.all.should == [
      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
    ]
  end

  specify "should split the list of records into batches if :commit_every option is given" do
    @d.multi_insert([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
      :commit_every => 2)

    MYSQL_DB.sqls.should == [
      'BEGIN',
      "INSERT INTO items (`value`) VALUES (1), (2)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (`value`) VALUES (3), (4)",
      'COMMIT'
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
      'BEGIN',
      "INSERT INTO items (`value`) VALUES (1), (2)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (`value`) VALUES (3), (4)",
      'COMMIT'
    ]
    
    @d.all.should == [
      {:name => nil, :value => 1}, 
      {:name => nil, :value => 2},
      {:name => nil, :value => 3}, 
      {:name => nil, :value => 4}
    ]
  end
  
  specify "should support inserting using columns and values arrays" do
    @d.multi_insert([:name, :value], [['abc', 1], ['def', 2]])

    MYSQL_DB.sqls.should == [
      'BEGIN',
      "INSERT INTO items (`name`, `value`) VALUES ('abc', 1), ('def', 2)",
      'COMMIT'
    ]
    
    @d.all.should == [
      {:name => 'abc', :value => 1}, 
      {:name => 'def', :value => 2}
    ]
  end
end

context "MySQL::Dataset#replace" do
  setup do
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
