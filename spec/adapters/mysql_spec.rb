SEQUEL_ADAPTER_TEST = :mysql

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

unless defined?(MYSQL_SOCKET_FILE)
  MYSQL_SOCKET_FILE = '/tmp/mysql.sock'
end
MYSQL_URI = URI.parse(DB.uri)

def DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  DB.sqls << msg
end
DB.loggers = [logger]
DB.drop_table?(:items, :dolls, :booltest)

SQL_BEGIN = 'BEGIN'
SQL_ROLLBACK = 'ROLLBACK'
SQL_COMMIT = 'COMMIT'

describe "MySQL", '#create_table' do
  before do
    @db = DB
    DB.sqls.clear
  end
  after do
    @db.drop_table?(:dolls)
  end

  specify "should allow to specify options for MySQL" do
    @db.create_table(:dolls, :engine => 'MyISAM', :charset => 'latin2'){text :name}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `dolls` (`name` text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]
    end
  end

  specify "should create a temporary table" do
    @db.create_table(:tmp_dolls, :temp => true, :engine => 'MyISAM', :charset => 'latin2'){text :name}
    check_sqls do
      @db.sqls.should == ["CREATE TEMPORARY TABLE `tmp_dolls` (`name` text) ENGINE=MyISAM DEFAULT CHARSET=latin2"]
    end
  end

  specify "should not use a default for a String :text=>true type" do
    @db.create_table(:dolls){String :name, :text=>true, :default=>'blah'}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `dolls` (`name` text)"]
    end
  end

  specify "should not use a default for a File type" do
    @db.create_table(:dolls){File :name, :default=>'blah'}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `dolls` (`name` blob)"]
    end
  end

  specify "should respect the size option for File type" do
    @db.create_table(:dolls) do
      File :n1
      File :n2, :size=>:tiny
      File :n3, :size=>:medium
      File :n4, :size=>:long
      File :n5, :size=>255
    end
    @db.schema(:dolls).map{|k, v| v[:db_type]}.should == %w"blob tinyblob mediumblob longblob blob"
  end

  specify "should include an :auto_increment schema attribute if auto incrementing" do
    @db.create_table(:dolls) do
      Integer :n2
      String :n3
      Integer :n4, :auto_increment=>true, :unique=>true
    end
    @db.schema(:dolls).map{|k, v| v[:auto_increment]}.should == [nil, nil, true]
  end

  specify "should support collate with various other column options" do
    @db.create_table!(:dolls){ String :name, :size=>128, :collate=>:utf8_bin, :default=>'foo', :null=>false, :unique=>true}
    @db[:dolls].insert
    @db[:dolls].select_map(:name).should == ["foo"]
  end

  specify "should be able to parse the default value for set and enum types" do
    @db.create_table!(:dolls){column :t, "set('a', 'b', 'c', 'd')", :default=>'a,b'}
    @db.schema(:dolls).first.last[:ruby_default].should == 'a,b'
    @db.create_table!(:dolls){column :t, "enum('a', 'b', 'c', 'd')", :default=>'b'}
    @db.schema(:dolls).first.last[:ruby_default].should == 'b'
  end
end

if [:mysql, :mysql2].include?(DB.adapter_scheme)
  describe "Sequel::MySQL::Database#convert_tinyint_to_bool" do
    before do
      @db = DB
      @db.create_table(:booltest){column :b, 'tinyint(1)'; column :i, 'tinyint(4)'}
      @ds = @db[:booltest]
    end
    after do
      @db.convert_tinyint_to_bool = true
      @db.drop_table?(:booltest)
    end

    specify "should consider tinyint(1) datatypes as boolean if set, but not larger tinyints" do
      @db.schema(:booltest, :reload=>true).should == [[:b, {:type=>:boolean, :allow_null=>true, :primary_key=>false, :default=>nil, :ruby_default=>nil, :db_type=>"tinyint(1)"}, ], [:i, {:type=>:integer, :allow_null=>true, :primary_key=>false, :default=>nil, :ruby_default=>nil, :db_type=>"tinyint(4)"}, ]]
      @db.convert_tinyint_to_bool = false
      @db.schema(:booltest, :reload=>true).should == [[:b, {:type=>:integer, :allow_null=>true, :primary_key=>false, :default=>nil, :ruby_default=>nil, :db_type=>"tinyint(1)"}, ], [:i, {:type=>:integer, :allow_null=>true, :primary_key=>false, :default=>nil, :ruby_default=>nil, :db_type=>"tinyint(4)"}, ]]
    end

    specify "should return tinyint(1)s as bools and tinyint(4)s as integers when set" do
      @db.convert_tinyint_to_bool = true
      @ds.delete
      @ds << {:b=>true, :i=>10}
      @ds.all.should == [{:b=>true, :i=>10}]
      @ds.delete
      @ds << {:b=>false, :i=>0}
      @ds.all.should == [{:b=>false, :i=>0}]
      @ds.delete
      @ds << {:b=>true, :i=>1}
      @ds.all.should == [{:b=>true, :i=>1}]
    end

    specify "should return all tinyints as integers when unset" do
      @db.convert_tinyint_to_bool = false
      @ds.delete
      @ds << {:b=>true, :i=>10}
      @ds.all.should == [{:b=>1, :i=>10}]
      @ds.delete
      @ds << {:b=>false, :i=>0}
      @ds.all.should == [{:b=>0, :i=>0}]

      @ds.delete
      @ds << {:b=>1, :i=>10}
      @ds.all.should == [{:b=>1, :i=>10}]
      @ds.delete
      @ds << {:b=>0, :i=>0}
      @ds.all.should == [{:b=>0, :i=>0}]
    end

    specify "should allow disabling the conversion on a per-dataset basis" do
      @db.convert_tinyint_to_bool = true
      ds = @ds.clone
      def ds.cast_tinyint_integer?(f) true end #mysql
      def ds.convert_tinyint_to_bool?() false end #mysql2
      ds.delete
      ds << {:b=>true, :i=>10}
      ds.all.should == [{:b=>1, :i=>10}]
      @ds.all.should == [{:b=>true, :i=>10}]
    end
  end
end

describe "A MySQL dataset" do
  before do
    DB.create_table(:items){String :name; Integer :value}
    @d = DB[:items]
    DB.sqls.clear
  end
  after do
    DB.drop_table?(:items)
  end

  specify "should quote columns and tables using back-ticks if quoting identifiers" do
    @d.quote_identifiers = true
    @d.select(:name).sql.should == 'SELECT `name` FROM `items`'
    @d.select(Sequel.lit('COUNT(*)')).sql.should == 'SELECT COUNT(*) FROM `items`'
    @d.select(Sequel.function(:max, :value)).sql.should == 'SELECT max(`value`) FROM `items`'
    @d.select(Sequel.function(:NOW)).sql.should == 'SELECT NOW() FROM `items`'
    @d.select(Sequel.function(:max, :items__value)).sql.should == 'SELECT max(`items`.`value`) FROM `items`'
    @d.order(Sequel.expr(:name).desc).sql.should == 'SELECT * FROM `items` ORDER BY `name` DESC'
    @d.select(Sequel.lit('items.name AS item_name')).sql.should == 'SELECT items.name AS item_name FROM `items`'
    @d.select(Sequel.lit('`name`')).sql.should == 'SELECT `name` FROM `items`'
    @d.select(Sequel.lit('max(items.`name`) AS `max_name`')).sql.should == 'SELECT max(items.`name`) AS `max_name` FROM `items`'
    @d.select(Sequel.function(:test, :abc, 'hello')).sql.should == "SELECT test(`abc`, 'hello') FROM `items`"
    @d.select(Sequel.function(:test, :abc__def, 'hello')).sql.should == "SELECT test(`abc`.`def`, 'hello') FROM `items`"
    @d.select(Sequel.function(:test, :abc__def, 'hello').as(:x2)).sql.should == "SELECT test(`abc`.`def`, 'hello') AS `x2` FROM `items`"
    @d.insert_sql(:value => 333).should == 'INSERT INTO `items` (`value`) VALUES (333)'
    @d.insert_sql(:x => :y).should == 'INSERT INTO `items` (`x`) VALUES (`y`)'
  end

  specify "should quote fields correctly when reversing the order" do
    @d.quote_identifiers = true
    @d.reverse_order(:name).sql.should == 'SELECT * FROM `items` ORDER BY `name` DESC'
    @d.reverse_order(Sequel.desc(:name)).sql.should == 'SELECT * FROM `items` ORDER BY `name` ASC'
    @d.reverse_order(:name, Sequel.desc(:test)).sql.should == 'SELECT * FROM `items` ORDER BY `name` DESC, `test` ASC'
    @d.reverse_order(Sequel.desc(:name), :test).sql.should == 'SELECT * FROM `items` ORDER BY `name` ASC, `test` DESC'
  end

  specify "should support ORDER clause in UPDATE statements" do
    @d.order(:name).update_sql(:value => 1).should == 'UPDATE `items` SET `value` = 1 ORDER BY `name`'
  end

  specify "should support LIMIT clause in UPDATE statements" do
    @d.limit(10).update_sql(:value => 1).should == 'UPDATE `items` SET `value` = 1 LIMIT 10'
  end

  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}
    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end

  specify "should have explain output" do
    @d.explain.should be_a_kind_of(String)
    @d.explain(:extended=>true).should be_a_kind_of(String)
    @d.explain.should_not == @d.explain(:extended=>true)
  end

  specify "should correctly literalize strings with comment backslashes in them" do
    @d.delete
    proc {@d << {:name => ':\\'}}.should_not raise_error

    @d.first[:name].should == ':\\'
  end

  specify "should handle prepared statements with on_duplicate_key_update" do
    @d.db.add_index :items, :value, :unique=>true
    ds = @d.on_duplicate_key_update
    ps = ds.prepare(:insert, :insert_user_id_feature_name, :value => :$v, :name => :$n)
    ps.call(:v => 1, :n => 'a')
    ds.all.should == [{:value=>1, :name=>'a'}]
    ps.call(:v => 1, :n => 'b')
    ds.all.should == [{:value=>1, :name=>'b'}]
  end
end

describe "MySQL datasets" do
  before do
    @d = DB[:orders]
  end

  specify "should correctly quote column references" do
    @d.quote_identifiers = true
    market = 'ICE'
    ack_stamp = Time.now - 15 * 60 # 15 minutes ago
    @d.select(:market, Sequel.function(:minute, Sequel.function(:from_unixtime, :ack)).as(:minute)).
      where{(ack > ack_stamp) & {:market => market}}.
      group_by(Sequel.function(:minute, Sequel.function(:from_unixtime, :ack))).sql.should == \
      "SELECT `market`, minute(from_unixtime(`ack`)) AS `minute` FROM `orders` WHERE ((`ack` > #{@d.literal(ack_stamp)}) AND (`market` = 'ICE')) GROUP BY minute(from_unixtime(`ack`))"
  end
end

describe "Dataset#distinct" do
  before do
    @db = DB
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
    # MySQL doesn't respect orders when using the nonstandard GROUP BY
    [[20], [30]].should include(@ds.order(:b, :a).distinct(:b).map(:a))
  end
end

describe "MySQL join expressions" do
  before do
    @ds = DB[:nodes]
  end

  specify "should raise error for :full_outer join requests." do
    lambda{@ds.join_table(:full_outer, :nodes)}.should raise_error(Sequel::Error)
  end
  specify "should support natural left joins" do
    @ds.join_table(:natural_left, :nodes).sql.should == \
      'SELECT * FROM `nodes` NATURAL LEFT JOIN `nodes`'
  end
  specify "should support natural right joins" do
    @ds.join_table(:natural_right, :nodes).sql.should == \
      'SELECT * FROM `nodes` NATURAL RIGHT JOIN `nodes`'
  end
  specify "should support natural left outer joins" do
    @ds.join_table(:natural_left_outer, :nodes).sql.should == \
      'SELECT * FROM `nodes` NATURAL LEFT OUTER JOIN `nodes`'
  end
  specify "should support natural right outer joins" do
    @ds.join_table(:natural_right_outer, :nodes).sql.should == \
      'SELECT * FROM `nodes` NATURAL RIGHT OUTER JOIN `nodes`'
  end
  specify "should support natural inner joins" do
    @ds.join_table(:natural_inner, :nodes).sql.should == \
      'SELECT * FROM `nodes` NATURAL LEFT JOIN `nodes`'
  end
  specify "should support cross joins" do
    @ds.join_table(:cross, :nodes).sql.should == \
      'SELECT * FROM `nodes` CROSS JOIN `nodes`'
  end
  specify "should support cross joins as inner joins if conditions are used" do
    @ds.join_table(:cross, :nodes, :id=>:id).sql.should == \
      'SELECT * FROM `nodes` INNER JOIN `nodes` ON (`nodes`.`id` = `nodes`.`id`)'
  end
  specify "should support straight joins (force left table to be read before right)" do
    @ds.join_table(:straight, :nodes).sql.should == \
      'SELECT * FROM `nodes` STRAIGHT_JOIN `nodes`'
  end
  specify "should support natural joins on multiple tables." do
    @ds.join_table(:natural_left_outer, [:nodes, :branches]).sql.should == \
      'SELECT * FROM `nodes` NATURAL LEFT OUTER JOIN (`nodes`, `branches`)'
  end
  specify "should support straight joins on multiple tables." do
    @ds.join_table(:straight, [:nodes,:branches]).sql.should == \
      'SELECT * FROM `nodes` STRAIGHT_JOIN (`nodes`, `branches`)'
  end
end

describe "Joined MySQL dataset" do
  before do
    @ds = DB[:nodes]
  end

  specify "should quote fields correctly" do
    @ds.quote_identifiers = true
    @ds.join(:attributes, :node_id => :id).sql.should == \
      "SELECT * FROM `nodes` INNER JOIN `attributes` ON (`attributes`.`node_id` = `nodes`.`id`)"
  end

  specify "should allow a having clause on ungrouped datasets" do
    proc {@ds.having('blah')}.should_not raise_error

    @ds.having('blah').sql.should == \
      "SELECT * FROM `nodes` HAVING (blah)"
  end

  specify "should put a having clause before an order by clause" do
    @ds.order(:aaa).having(:bbb => :ccc).sql.should == \
      "SELECT * FROM `nodes` HAVING (`bbb` = `ccc`) ORDER BY `aaa`"
  end
end

describe "A MySQL database" do
  after do
    DB.drop_table?(:test_innodb)
  end

  specify "should handle the creation and dropping of an InnoDB table with foreign keys" do
    proc{DB.create_table!(:test_innodb, :engine=>:InnoDB){primary_key :id; foreign_key :fk, :test_innodb, :key=>:id}}.should_not raise_error
  end
end

describe "A MySQL database" do
  before(:all) do
    @db = DB
    @db.create_table! :test2 do
      text :name
      integer :value
    end
  end
  after(:all) do
    @db.drop_table?(:test2)
  end

  specify "should provide the server version" do
    @db.server_version.should >= 40000
  end

  specify "should cache the server version" do
    # warm cache:
    @db.server_version
    @db.sqls.clear
    3.times{@db.server_version}
    @db.sqls.should be_empty
  end

  specify "should support for_share" do
    @db.transaction{@db[:test2].for_share.all.should == []}
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
      add_index :value, :unique=>true
      add_foreign_key :value2, :test2, :key=>:value
    end
    @db[:test2].columns.should == [:name, :value, :zyx, :ert, :xyz, :value2]
  end
end  

describe "A MySQL database with table options" do
  before do
    @options = {:engine=>'MyISAM', :charset=>'latin1', :collate => 'latin1_swedish_ci'}

    Sequel::MySQL.default_engine = 'InnoDB'
    Sequel::MySQL.default_charset = 'utf8'
    Sequel::MySQL.default_collate = 'utf8_general_ci'

    @db = DB
    @db.drop_table?(:items)

    DB.sqls.clear
  end
  after do
    @db.drop_table?(:items)

    Sequel::MySQL.default_engine = nil
    Sequel::MySQL.default_charset = nil
    Sequel::MySQL.default_collate = nil
  end

  specify "should allow to pass custom options (engine, charset, collate) for table creation" do
    @db.create_table(:items, @options){Integer :size; text :name}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`size` integer, `name` text) ENGINE=MyISAM DEFAULT CHARSET=latin1 DEFAULT COLLATE=latin1_swedish_ci"]
    end
  end

  specify "should use default options if specified (engine, charset, collate) for table creation" do
    @db.create_table(:items){Integer :size; text :name}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`size` integer, `name` text) ENGINE=InnoDB DEFAULT CHARSET=utf8 DEFAULT COLLATE=utf8_general_ci"]
    end
  end

  specify "should not use default if option has a nil value" do
    @db.create_table(:items, :engine=>nil, :charset=>nil, :collate=>nil){Integer :size; text :name}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`size` integer, `name` text)"]
    end
  end
end

describe "A MySQL database" do
  before do
    @db = DB
    @db.drop_table?(:items)
    DB.sqls.clear
  end
  after do
    @db.drop_table?(:items, :users)
  end

  specify "should support defaults for boolean columns" do
    @db.create_table(:items){TrueClass :active1, :default=>true; FalseClass :active2, :default => false}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`active1` tinyint(1) DEFAULT 1, `active2` tinyint(1) DEFAULT 0)"]
    end
  end

  specify "should correctly format CREATE TABLE statements with foreign keys" do
    @db.create_table(:items){primary_key :id; foreign_key :p_id, :items, :key => :id, :null => false, :on_delete => :cascade}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer PRIMARY KEY AUTO_INCREMENT, `p_id` integer NOT NULL, UNIQUE (`id`), FOREIGN KEY (`p_id`) REFERENCES `items`(`id`) ON DELETE CASCADE)"]
    end
  end

  specify "should correctly format CREATE TABLE statements with foreign keys, when :key != the default (:id)" do
    @db.create_table(:items){primary_key :id; Integer :other_than_id; foreign_key :p_id, :items, :key => :other_than_id, :null => false, :on_delete => :cascade}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer PRIMARY KEY AUTO_INCREMENT, `other_than_id` integer, `p_id` integer NOT NULL, UNIQUE (`other_than_id`), FOREIGN KEY (`p_id`) REFERENCES `items`(`other_than_id`) ON DELETE CASCADE)"]
    end
  end

  specify "should correctly format ALTER TABLE statements with foreign keys" do
    @db.create_table(:items){Integer :id}
    @db.create_table(:users){primary_key :id}
    @db.alter_table(:items){add_foreign_key :p_id, :users, :key => :id, :null => false, :on_delete => :cascade}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer)",
        "CREATE TABLE `users` (`id` integer PRIMARY KEY AUTO_INCREMENT)",
        "ALTER TABLE `items` ADD COLUMN `p_id` integer NOT NULL, ADD FOREIGN KEY (`p_id`) REFERENCES `users`(`id`) ON DELETE CASCADE"]
    end
  end

  specify "should correctly format ALTER TABLE statements with named foreign keys" do
    @db.create_table(:items){Integer :id}
    @db.create_table(:users){primary_key :id}
    @db.alter_table(:items){add_foreign_key :p_id, :users, :key => :id, :null => false, :on_delete => :cascade, :foreign_key_constraint_name => :pk_items__users }
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer)",
        "CREATE TABLE `users` (`id` integer PRIMARY KEY AUTO_INCREMENT)",
        "ALTER TABLE `items` ADD COLUMN `p_id` integer NOT NULL, ADD CONSTRAINT `pk_items__users` FOREIGN KEY (`p_id`) REFERENCES `users`(`id`) ON DELETE CASCADE"]
    end
  end

  specify "should have rename_column support keep existing options" do
    @db.create_table(:items){String :id, :null=>false, :default=>'blah'}
    @db.alter_table(:items){rename_column :id, :nid}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` varchar(255) NOT NULL DEFAULT 'blah')", "DESCRIBE `items`", "ALTER TABLE `items` CHANGE COLUMN `id` `nid` varchar(255) NOT NULL DEFAULT 'blah'"]
    end
    @db[:items].insert
    @db[:items].all.should == [{:nid=>'blah'}]
    proc{@db[:items].insert(:nid=>nil)}.should raise_error(Sequel::DatabaseError)
  end

  specify "should have set_column_type support keep existing options" do
    @db.create_table(:items){Integer :id, :null=>false, :default=>5}
    @db.alter_table(:items){set_column_type :id, Bignum}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer NOT NULL DEFAULT 5)", "DESCRIBE `items`", "ALTER TABLE `items` CHANGE COLUMN `id` `id` bigint NOT NULL DEFAULT 5"]
    end
    @db[:items].insert
    @db[:items].all.should == [{:id=>5}]
    proc{@db[:items].insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)
    @db[:items].delete
    @db[:items].insert(2**40)
    @db[:items].all.should == [{:id=>2**40}]
  end

  specify "should have set_column_type pass through options" do
    @db.create_table(:items){integer :id; enum :list, :elements=>%w[one]}
    @db.alter_table(:items){set_column_type :id, :int, :unsigned=>true, :size=>8; set_column_type :list, :enum, :elements=>%w[two]}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer, `list` enum('one'))", "DESCRIBE `items`", "ALTER TABLE `items` CHANGE COLUMN `id` `id` int(8) UNSIGNED NULL, CHANGE COLUMN `list` `list` enum('two') NULL"]
    end
  end

  specify "should have set_column_default support keep existing options" do
    @db.create_table(:items){Integer :id, :null=>false, :default=>5}
    @db.alter_table(:items){set_column_default :id, 6}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer NOT NULL DEFAULT 5)", "DESCRIBE `items`", "ALTER TABLE `items` CHANGE COLUMN `id` `id` int(11) NOT NULL DEFAULT 6"]
    end
    @db[:items].insert
    @db[:items].all.should == [{:id=>6}]
    proc{@db[:items].insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)
  end

  specify "should have set_column_allow_null support keep existing options" do
    @db.create_table(:items){Integer :id, :null=>false, :default=>5}
    @db.alter_table(:items){set_column_allow_null :id, true}
    check_sqls do
      @db.sqls.should == ["CREATE TABLE `items` (`id` integer NOT NULL DEFAULT 5)", "DESCRIBE `items`", "ALTER TABLE `items` CHANGE COLUMN `id` `id` int(11) NULL DEFAULT 5"]
    end
    @db[:items].insert
    @db[:items].all.should == [{:id=>5}]
    proc{@db[:items].insert(:id=>nil)}.should_not
  end

  specify "should accept repeated raw sql statements using Database#<<" do
    @db.create_table(:items){String :name; Integer :value}
    @db << 'DELETE FROM items'
    @db[:items].count.should == 0

    @db << "INSERT INTO items (name, value) VALUES ('tutu', 1234)"
    @db[:items].first.should == {:name => 'tutu', :value => 1234}

    @db << 'DELETE FROM items'
    @db[:items].first.should == nil
  end
end  

# Socket tests should only be run if the MySQL server is on localhost
if %w'localhost 127.0.0.1 ::1'.include?(MYSQL_URI.host) and DB.adapter_scheme == :mysql
  describe "A MySQL database" do
    specify "should accept a socket option" do
      db = Sequel.mysql(DB.opts[:database], :host => 'localhost', :user => DB.opts[:user], :password => DB.opts[:password], :socket => MYSQL_SOCKET_FILE)
      proc {db.test_connection}.should_not raise_error
    end

    specify "should accept a socket option without host option" do
      db = Sequel.mysql(DB.opts[:database], :user => DB.opts[:user], :password => DB.opts[:password], :socket => MYSQL_SOCKET_FILE)
      proc {db.test_connection}.should_not raise_error
    end

    specify "should fail to connect with invalid socket" do
      db = Sequel.mysql(DB.opts[:database], :user => DB.opts[:user], :password => DB.opts[:password], :socket =>'blah')
      proc {db.test_connection}.should raise_error
    end
  end
end

describe "A MySQL database" do
  specify "should accept a read_timeout option when connecting" do
    db = Sequel.connect(DB.opts.merge(:read_timeout=>22342))
    proc {db.test_connection}.should_not raise_error
  end

  specify "should accept a connect_timeout option when connecting" do
    db = Sequel.connect(DB.opts.merge(:connect_timeout=>22342))
    proc {db.test_connection}.should_not raise_error
  end
end

describe "MySQL foreign key support" do
  after do
    DB.drop_table?(:testfk, :testpk)
  end

  specify "should create table without :key" do
    DB.create_table!(:testpk){primary_key :id}
    DB.create_table!(:testfk){foreign_key :fk, :testpk}
  end

  specify "should create table with composite keys without :key" do
    DB.create_table!(:testpk){Integer :id; Integer :id2; primary_key([:id, :id2])}
    DB.create_table!(:testfk){Integer :fk; Integer :fk2; foreign_key([:fk, :fk2], :testpk)}
  end

  specify "should create table with self referential without :key" do
    DB.create_table!(:testfk){primary_key :id; foreign_key :fk, :testfk}
  end

  specify "should create table with self referential with composite keys without :key" do
    DB.create_table!(:testfk){Integer :id; Integer :id2; Integer :fk; Integer :fk2; primary_key([:id, :id2]); foreign_key([:fk, :fk2], :testfk)}
  end

  specify "should alter table without :key" do
    DB.create_table!(:testpk){primary_key :id}
    DB.create_table!(:testfk){Integer :id}
    DB.alter_table(:testfk){add_foreign_key :fk, :testpk}
  end

  specify "should alter table with composite keys without :key" do
    DB.create_table!(:testpk){Integer :id; Integer :id2; primary_key([:id, :id2])}
    DB.create_table!(:testfk){Integer :fk; Integer :fk2}
    DB.alter_table(:testfk){add_foreign_key([:fk, :fk2], :testpk)}
  end

  specify "should alter table with self referential without :key" do
    DB.create_table!(:testfk){primary_key :id}
    DB.alter_table(:testfk){add_foreign_key :fk, :testfk}
  end

  specify "should alter table with self referential with composite keys without :key" do
    DB.create_table!(:testfk){Integer :id; Integer :id2; Integer :fk; Integer :fk2; primary_key([:id, :id2])}
    DB.alter_table(:testfk){add_foreign_key [:fk, :fk2], :testfk}
  end
end

describe "A grouped MySQL dataset" do
  before do
    DB.create_table! :test2 do
      text :name
      integer :value
    end
    DB[:test2] << {:name => '11', :value => 10}
    DB[:test2] << {:name => '11', :value => 20}
    DB[:test2] << {:name => '11', :value => 30}
    DB[:test2] << {:name => '12', :value => 10}
    DB[:test2] << {:name => '12', :value => 20}
    DB[:test2] << {:name => '13', :value => 10}
  end
  after do
    DB.drop_table?(:test2)
  end

  specify "should return the correct count for raw sql query" do
    ds = DB["select name FROM test2 WHERE name = '11' GROUP BY name"]
    ds.count.should == 1
  end

  specify "should return the correct count for a normal dataset" do
    ds = DB[:test2].select(:name).where(:name => '11').group(:name)
    ds.count.should == 1
  end
end

describe "A MySQL database" do
  before do
    @db = DB
    @db.drop_table?(:posts)
    @db.sqls.clear
  end
  after do
    @db.drop_table?(:posts)
  end

  specify "should support fulltext indexes and full_text_search" do
    @db.create_table(:posts, :engine=>:MyISAM){text :title; text :body; full_text_index :title; full_text_index [:title, :body]}
    check_sqls do
      @db.sqls.should == [
        "CREATE TABLE `posts` (`title` text, `body` text) ENGINE=MyISAM",
        "CREATE FULLTEXT INDEX `posts_title_index` ON `posts` (`title`)",
        "CREATE FULLTEXT INDEX `posts_title_body_index` ON `posts` (`title`, `body`)"
      ]
    end

    @db[:posts].insert(:title=>'ruby rails', :body=>'y')
    @db[:posts].insert(:title=>'sequel', :body=>'ruby')
    @db[:posts].insert(:title=>'ruby scooby', :body=>'x')
    @db.sqls.clear

    @db[:posts].full_text_search(:title, 'rails').all.should == [{:title=>'ruby rails', :body=>'y'}]
    @db[:posts].full_text_search([:title, :body], ['sequel', 'ruby']).all.should == [{:title=>'sequel', :body=>'ruby'}]
    @db[:posts].full_text_search(:title, '+ruby -rails', :boolean => true).all.should == [{:title=>'ruby scooby', :body=>'x'}]
    check_sqls do
      @db.sqls.should == [
        "SELECT * FROM `posts` WHERE (MATCH (`title`) AGAINST ('rails'))",
        "SELECT * FROM `posts` WHERE (MATCH (`title`, `body`) AGAINST ('sequel ruby'))",
        "SELECT * FROM `posts` WHERE (MATCH (`title`) AGAINST ('+ruby -rails' IN BOOLEAN MODE))"]
    end

    @db[:posts].full_text_search(:title, :$n).call(:select, :n=>'rails').should == [{:title=>'ruby rails', :body=>'y'}]
    @db[:posts].full_text_search(:title, :$n).prepare(:select, :fts_select).call(:n=>'rails').should == [{:title=>'ruby rails', :body=>'y'}]
  end

  specify "should support spatial indexes" do
    @db.create_table(:posts, :engine=>:MyISAM){point :geom, :null=>false; spatial_index [:geom]}
    check_sqls do
      @db.sqls.should == [
        "CREATE TABLE `posts` (`geom` point NOT NULL) ENGINE=MyISAM",
        "CREATE SPATIAL INDEX `posts_geom_index` ON `posts` (`geom`)"
      ]
    end
  end

  specify "should support indexes with index type" do
    @db.create_table(:posts){Integer :id; index :id, :type => :btree}
    check_sqls do
      @db.sqls.should == [
        "CREATE TABLE `posts` (`id` integer)",
        "CREATE INDEX `posts_id_index` USING btree ON `posts` (`id`)"
      ]
    end
  end

  specify "should support unique indexes with index type" do
    @db.create_table(:posts){Integer :id; index :id, :type => :btree, :unique => true}
    check_sqls do
      @db.sqls.should == [
        "CREATE TABLE `posts` (`id` integer)",
        "CREATE UNIQUE INDEX `posts_id_index` USING btree ON `posts` (`id`)"
      ]
    end
  end

  specify "should not dump partial indexes" do
    @db.create_table(:posts){text :id}
    @db << "CREATE INDEX posts_id_index ON posts (id(10))"
    @db.indexes(:posts).should == {}
  end

  specify "should dump partial indexes if :partial option is set to true" do
    @db.create_table(:posts){text :id}
    @db << "CREATE INDEX posts_id_index ON posts (id(10))"
    @db.indexes(:posts, :partial => true).should == {:posts_id_index => {:columns => [:id], :unique => false}}
  end
end

describe "MySQL::Dataset#insert and related methods" do
  before do
    DB.create_table(:items){String :name; Integer :value}
    @d = DB[:items]
    DB.sqls.clear
  end
  after do
    DB.drop_table?(:items)
  end

  specify "#insert should insert record with default values when no arguments given" do
    @d.insert
    check_sqls do
      DB.sqls.should == ["INSERT INTO `items` () VALUES ()"]
    end
    @d.all.should == [{:name => nil, :value => nil}]
  end

  specify "#insert  should insert record with default values when empty hash given" do
    @d.insert({})
    check_sqls do
      DB.sqls.should == ["INSERT INTO `items` () VALUES ()"]
    end
    @d.all.should == [{:name => nil, :value => nil}]
  end

  specify "#insert should insert record with default values when empty array given" do
    @d.insert []
    check_sqls do
      DB.sqls.should == ["INSERT INTO `items` () VALUES ()"]
    end
    @d.all.should == [{:name => nil, :value => nil}]
  end

  specify "#on_duplicate_key_update should work with regular inserts" do
    DB.add_index :items, :name, :unique=>true
    DB.sqls.clear
    @d.insert(:name => 'abc', :value => 1)
    @d.on_duplicate_key_update(:name, :value => 6).insert(:name => 'abc', :value => 1)
    @d.on_duplicate_key_update(:name, :value => 6).insert(:name => 'def', :value => 2)

    check_sqls do
      DB.sqls.length.should == 3
      DB.sqls[0].should =~ /\AINSERT INTO `items` \(`(name|value)`, `(name|value)`\) VALUES \(('abc'|1), (1|'abc')\)\z/
      DB.sqls[1].should =~ /\AINSERT INTO `items` \(`(name|value)`, `(name|value)`\) VALUES \(('abc'|1), (1|'abc')\) ON DUPLICATE KEY UPDATE `name`=VALUES\(`name`\), `value`=6\z/
      DB.sqls[2].should =~ /\AINSERT INTO `items` \(`(name|value)`, `(name|value)`\) VALUES \(('def'|2), (2|'def')\) ON DUPLICATE KEY UPDATE `name`=VALUES\(`name`\), `value`=6\z/
    end

    @d.all.should == [{:name => 'abc', :value => 6}, {:name => 'def', :value => 2}]
  end

  specify "#multi_replace should insert multiple records in a single statement" do
    @d.multi_replace([{:name => 'abc'}, {:name => 'def'}])

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "REPLACE INTO `items` (`name`) VALUES ('abc'), ('def')",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
    ]
  end

  specify "#multi_replace should split the list of records into batches if :commit_every option is given" do
    @d.multi_replace([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
      :commit_every => 2)

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "REPLACE INTO `items` (`value`) VALUES (1), (2)",
        SQL_COMMIT,
        SQL_BEGIN,
        "REPLACE INTO `items` (`value`) VALUES (3), (4)",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => nil, :value => 1}, 
      {:name => nil, :value => 2},
      {:name => nil, :value => 3}, 
      {:name => nil, :value => 4}
    ]
  end

  specify "#multi_replace should split the list of records into batches if :slice option is given" do
    @d.multi_replace([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
      :slice => 2)

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "REPLACE INTO `items` (`value`) VALUES (1), (2)",
        SQL_COMMIT,
        SQL_BEGIN,
        "REPLACE INTO `items` (`value`) VALUES (3), (4)",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => nil, :value => 1}, 
      {:name => nil, :value => 2},
      {:name => nil, :value => 3}, 
      {:name => nil, :value => 4}
    ]
  end

  specify "#multi_insert should insert multiple records in a single statement" do
    @d.multi_insert([{:name => 'abc'}, {:name => 'def'}])

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "INSERT INTO `items` (`name`) VALUES ('abc'), ('def')",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
    ]
  end

  specify "#multi_insert should split the list of records into batches if :commit_every option is given" do
    @d.multi_insert([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
      :commit_every => 2)

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "INSERT INTO `items` (`value`) VALUES (1), (2)",
        SQL_COMMIT,
        SQL_BEGIN,
        "INSERT INTO `items` (`value`) VALUES (3), (4)",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => nil, :value => 1}, 
      {:name => nil, :value => 2},
      {:name => nil, :value => 3}, 
      {:name => nil, :value => 4}
    ]
  end

  specify "#multi_insert should split the list of records into batches if :slice option is given" do
    @d.multi_insert([{:value => 1}, {:value => 2}, {:value => 3}, {:value => 4}],
      :slice => 2)

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "INSERT INTO `items` (`value`) VALUES (1), (2)",
        SQL_COMMIT,
        SQL_BEGIN,
        "INSERT INTO `items` (`value`) VALUES (3), (4)",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => nil, :value => 1}, 
      {:name => nil, :value => 2},
      {:name => nil, :value => 3}, 
      {:name => nil, :value => 4}
    ]
  end

  specify "#import should support inserting using columns and values arrays" do
    @d.import([:name, :value], [['abc', 1], ['def', 2]])

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "INSERT INTO `items` (`name`, `value`) VALUES ('abc', 1), ('def', 2)",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => 'abc', :value => 1}, 
      {:name => 'def', :value => 2}
    ]
  end

  specify "#insert_ignore should add the IGNORE keyword when inserting" do
    @d.insert_ignore.multi_insert([{:name => 'abc'}, {:name => 'def'}])

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "INSERT IGNORE INTO `items` (`name`) VALUES ('abc'), ('def')",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => 'abc', :value => nil}, {:name => 'def', :value => nil}
    ]
  end

  specify "#insert_ignore should add the IGNORE keyword for single inserts" do
    @d.insert_ignore.insert(:name => 'ghi')
    check_sqls do
      DB.sqls.should == ["INSERT IGNORE INTO `items` (`name`) VALUES ('ghi')"]
    end
    @d.all.should == [{:name => 'ghi', :value => nil}]
  end

  specify "#on_duplicate_key_update should add the ON DUPLICATE KEY UPDATE and ALL columns when no args given" do
    @d.on_duplicate_key_update.import([:name,:value], [['abc', 1], ['def',2]])

    check_sqls do
      DB.sqls.should == [
        "SELECT * FROM `items` LIMIT 1",
        SQL_BEGIN,
        "INSERT INTO `items` (`name`, `value`) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `value`=VALUES(`value`)",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => 'abc', :value => 1}, {:name => 'def', :value => 2}
    ]
  end

  specify "#on_duplicate_key_update should add the ON DUPLICATE KEY UPDATE and columns specified when args are given" do
    @d.on_duplicate_key_update(:value).import([:name,:value], 
      [['abc', 1], ['def',2]]
    )

    check_sqls do
      DB.sqls.should == [
        SQL_BEGIN,
        "INSERT INTO `items` (`name`, `value`) VALUES ('abc', 1), ('def', 2) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)",
        SQL_COMMIT
      ]
    end

    @d.all.should == [
      {:name => 'abc', :value => 1}, {:name => 'def', :value => 2}
    ]
  end

end

describe "MySQL::Dataset#update and related methods" do
  before do
    DB.create_table(:items){String :name; Integer :value; index :name, :unique=>true}
    @d = DB[:items]
  end
  after do
    DB.drop_table?(:items)
  end

  specify "#update_ignore should not raise error where normal update would fail" do
    @d.insert(:name => 'cow', :value => 0)
    @d.insert(:name => 'cat', :value => 1)
    proc{@d.where(:value => 1).update(:name => 'cow')}.should raise_error(Sequel::DatabaseError)
    DB.sqls.clear
    @d.update_ignore.where(:value => 1).update(:name => 'cow')
    check_sqls do
      DB.sqls.should == ["UPDATE IGNORE `items` SET `name` = 'cow' WHERE (`value` = 1)"]
    end
    @d.order(:name).all.should == [{:name => 'cat', :value => 1}, {:name => 'cow', :value => 0}]
  end
end

describe "MySQL::Dataset#replace" do
  before do
    DB.create_table(:items){Integer :id, :unique=>true; Integer :value}
    @d = DB[:items]
    DB.sqls.clear
  end
  after do
    DB.drop_table?(:items)
  end

  specify "should use default values if they exist" do
    DB.alter_table(:items){set_column_default :id, 1; set_column_default :value, 2}
    @d.replace
    @d.all.should == [{:id=>1, :value=>2}]
    @d.replace([])
    @d.all.should == [{:id=>1, :value=>2}]
    @d.replace({})
    @d.all.should == [{:id=>1, :value=>2}]
  end
end

describe "MySQL::Dataset#complex_expression_sql" do
  before do
    @d = DB.dataset
  end

  specify "should handle string concatenation with CONCAT if more than one record" do
    @d.literal(Sequel.join([:x, :y])).should == "CONCAT(`x`, `y`)"
    @d.literal(Sequel.join([:x, :y], ' ')).should == "CONCAT(`x`, ' ', `y`)"
    @d.literal(Sequel.join([Sequel.function(:x, :y), 1, Sequel.lit('z')], Sequel.subscript(:y, 1))).should == "CONCAT(x(`y`), `y`[1], '1', `y`[1], z)"
  end

  specify "should handle string concatenation as simple string if just one record" do
    @d.literal(Sequel.join([:x])).should == "`x`"
    @d.literal(Sequel.join([:x], ' ')).should == "`x`"
  end
end

describe "MySQL::Dataset#calc_found_rows" do
  before do
    DB.create_table!(:items){Integer :a}
  end
  after do
    DB.drop_table?(:items)
  end

  specify "should add the SQL_CALC_FOUND_ROWS keyword when selecting" do
    DB[:items].select(:a).calc_found_rows.limit(1).sql.should == \
      'SELECT SQL_CALC_FOUND_ROWS `a` FROM `items` LIMIT 1'
  end

  specify "should count matching rows disregarding LIMIT clause" do
    DB[:items].multi_insert([{:a => 1}, {:a => 1}, {:a => 2}])
    DB.sqls.clear

    DB.synchronize do
      DB[:items].calc_found_rows.filter(:a => 1).limit(1).all.should == [{:a => 1}]
      DB.dataset.select(Sequel.function(:FOUND_ROWS).as(:rows)).all.should == [{:rows => 2 }]
    end

    check_sqls do
      DB.sqls.should == [
        'SELECT SQL_CALC_FOUND_ROWS * FROM `items` WHERE (`a` = 1) LIMIT 1',
        'SELECT FOUND_ROWS() AS `rows`',
      ]
    end
  end
end

if DB.adapter_scheme == :mysql or DB.adapter_scheme == :jdbc or DB.adapter_scheme == :mysql2
  describe "MySQL Stored Procedures" do
    before do
      DB.create_table(:items){Integer :id; Integer :value}
      @d = DB[:items]
      DB.sqls.clear
    end
    after do
      DB.drop_table?(:items)
      DB.execute('DROP PROCEDURE test_sproc')
    end

    specify "should be callable on the database object" do
      DB.execute_ddl('CREATE PROCEDURE test_sproc() BEGIN DELETE FROM items; END')
      DB[:items].delete
      DB[:items].insert(:value=>1)
      DB[:items].count.should == 1
      DB.call_sproc(:test_sproc)
      DB[:items].count.should == 0
    end

    # Mysql2 doesn't support stored procedures that return result sets, probably because
    # CLIENT_MULTI_RESULTS is not set.
    unless DB.adapter_scheme == :mysql2
      specify "should be callable on the dataset object" do
        DB.execute_ddl('CREATE PROCEDURE test_sproc(a INTEGER) BEGIN SELECT *, a AS b FROM items; END')
        DB[:items].delete
        @d = DB[:items]
        @d.call_sproc(:select, :test_sproc, 3).should == []
        @d.insert(:value=>1)
        @d.call_sproc(:select, :test_sproc, 4).should == [{:id=>nil, :value=>1, :b=>4}]
        @d.row_proc = proc{|r| r.keys.each{|k| r[k] *= 2 if r[k].is_a?(Integer)}; r}
        @d.call_sproc(:select, :test_sproc, 3).should == [{:id=>nil, :value=>2, :b=>6}]
      end

      specify "should be callable on the dataset object with multiple arguments" do
        DB.execute_ddl('CREATE PROCEDURE test_sproc(a INTEGER, c INTEGER) BEGIN SELECT *, a AS b, c AS d FROM items; END')
        DB[:items].delete
        @d = DB[:items]
        @d.call_sproc(:select, :test_sproc, 3, 4).should == []
        @d.insert(:value=>1)
        @d.call_sproc(:select, :test_sproc, 4, 5).should == [{:id=>nil, :value=>1, :b=>4, :d=>5}]
        @d.row_proc = proc{|r| r.keys.each{|k| r[k] *= 2 if r[k].is_a?(Integer)}; r}
        @d.call_sproc(:select, :test_sproc, 3, 4).should == [{:id=>nil, :value=>2, :b=>6, :d => 8}]
      end
    end

    specify "should deal with nil values" do
      DB.execute_ddl('CREATE PROCEDURE test_sproc(i INTEGER, v INTEGER) BEGIN INSERT INTO items VALUES (i, v); END')
      DB[:items].delete
      DB.call_sproc(:test_sproc, :args=>[1, nil])
      DB[:items].all.should == [{:id=>1, :value=>nil}]
    end
  end
end

if DB.adapter_scheme == :mysql
  describe "MySQL bad date/time conversions" do
    after do
      DB.convert_invalid_date_time = false
    end

    specify "should raise an exception when a bad date/time is used and convert_invalid_date_time is false" do
      DB.convert_invalid_date_time = false
      proc{DB["SELECT CAST('0000-00-00' AS date)"].single_value}.should raise_error(Sequel::InvalidValue)
      proc{DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value}.should raise_error(Sequel::InvalidValue)
      proc{DB["SELECT CAST('25:00:00' AS time)"].single_value}.should raise_error(Sequel::InvalidValue)
    end

    specify "should not use a nil value bad date/time is used and convert_invalid_date_time is nil or :nil" do
      DB.convert_invalid_date_time = nil
      DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == nil
      DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == nil
      DB["SELECT CAST('25:00:00' AS time)"].single_value.should == nil
      DB.convert_invalid_date_time = :nil
      DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == nil
      DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == nil
      DB["SELECT CAST('25:00:00' AS time)"].single_value.should == nil
    end

    specify "should not use a nil value bad date/time is used and convert_invalid_date_time is :string" do
      DB.convert_invalid_date_time = :string
      DB["SELECT CAST('0000-00-00' AS date)"].single_value.should == '0000-00-00'
      DB["SELECT CAST('0000-00-00 00:00:00' AS datetime)"].single_value.should == '0000-00-00 00:00:00'
      DB["SELECT CAST('25:00:00' AS time)"].single_value.should == '25:00:00'
    end
  end

  describe "MySQL multiple result sets" do
    before do
      DB.create_table!(:a){Integer :a}
      DB.create_table!(:b){Integer :b}
      @ds = DB['SELECT * FROM a; SELECT * FROM b']
      DB[:a].insert(10)
      DB[:a].insert(15)
      DB[:b].insert(20)
      DB[:b].insert(25)
    end
    after do
      DB.drop_table?(:a, :b)
    end

    specify "should combine all results by default" do
      @ds.all.should == [{:a=>10}, {:a=>15}, {:b=>20}, {:b=>25}]
    end

    specify "should work with Database#run" do
      proc{DB.run('SELECT * FROM a; SELECT * FROM b')}.should_not raise_error
      proc{DB.run('SELECT * FROM a; SELECT * FROM b')}.should_not raise_error
    end

    specify "should work with Database#run and other statements" do
      proc{DB.run('UPDATE a SET a = 1; SELECT * FROM a; DELETE FROM b')}.should_not raise_error
      DB[:a].select_order_map(:a).should == [1, 1]
      DB[:b].all.should == []
    end

    specify "should split results returned into arrays if split_multiple_result_sets is used" do
      @ds.split_multiple_result_sets.all.should == [[{:a=>10}, {:a=>15}], [{:b=>20}, {:b=>25}]]
    end

    specify "should have regular row_procs work when splitting multiple result sets" do
      @ds.row_proc = proc{|x| x[x.keys.first] *= 2; x}
      @ds.split_multiple_result_sets.all.should == [[{:a=>20}, {:a=>30}], [{:b=>40}, {:b=>50}]]
    end

    specify "should use the columns from the first result set when splitting result sets" do
      @ds.split_multiple_result_sets.columns.should == [:a]
    end

    specify "should not allow graphing a dataset that splits multiple statements" do
      proc{@ds.split_multiple_result_sets.graph(:b, :b=>:a)}.should raise_error(Sequel::Error)
    end

    specify "should not allow splitting a graphed dataset" do
      proc{DB[:a].graph(:b, :b=>:a).split_multiple_result_sets}.should raise_error(Sequel::Error)
    end
  end
end

if DB.adapter_scheme == :mysql2
  describe "Mysql2 streaming" do
    before(:all) do
      DB.create_table!(:a){Integer :a}
      DB.transaction do
        1000.times do |i|
          DB[:a].insert(i)
        end
      end
      @ds = DB[:a].stream.order(:a)
    end
    after(:all) do
      DB.drop_table?(:a)
    end

    specify "should correctly stream results" do
      @ds.map(:a).should == (0...1000).to_a
    end

    specify "should correctly handle early returning when streaming results" do
      3.times{@ds.each{|r| break r[:a]}.should == 0}
    end
  end
end
