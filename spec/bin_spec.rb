require 'rubygems'
require 'rbconfig'
require 'yaml'

RUBY = File.join(RbConfig::CONFIG['bindir'], RbConfig::CONFIG['RUBY_INSTALL_NAME'])
OUTPUT = "spec/bin-sequel-spec-output-#{$$}.log"
TMP_FILE = "spec/bin-sequel-tmp-#{$$}.rb"
BIN_SPEC_DB = "spec/bin-sequel-spec-db-#{$$}.sqlite3"
BIN_SPEC_DB2 = "spec/bin-sequel-spec-db2-#{$$}.sqlite3"

if defined?(RUBY_ENGINE) && RUBY_ENGINE == 'jruby'
  CONN_PREFIX = 'jdbc:sqlite:'
  CONN_HASH = {:adapter=>'jdbc', :uri=>"#{CONN_PREFIX}#{BIN_SPEC_DB}"}
else
  CONN_PREFIX = 'sqlite://'
  CONN_HASH = {:adapter=>'sqlite', :database=>BIN_SPEC_DB}
end

unless Object.const_defined?('Sequel') && Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel'
end

DB = Sequel.connect("#{CONN_PREFIX}#{BIN_SPEC_DB}")
DB2 = Sequel.connect("#{CONN_PREFIX}#{BIN_SPEC_DB2}")
File.delete(BIN_SPEC_DB) if File.file?(BIN_SPEC_DB)
File.delete(BIN_SPEC_DB2) if File.file?(BIN_SPEC_DB2)

describe "bin/sequel" do
  def bin(opts={})
    cmd = "#{opts[:pre]}\"#{RUBY}\" -I lib bin/sequel #{opts[:args]} #{"#{CONN_PREFIX}#{BIN_SPEC_DB}" unless opts[:no_conn]} #{opts[:post]}> #{OUTPUT}#{" 2>&1" if opts[:stderr]}"
    system(cmd)
    File.read(OUTPUT)
  end

  after do
    DB.disconnect
    DB2.disconnect
    File.delete(BIN_SPEC_DB) if File.file?(BIN_SPEC_DB)
    File.delete(BIN_SPEC_DB2) if File.file?(BIN_SPEC_DB2)
    File.delete(TMP_FILE) if File.file?(TMP_FILE)
  end
  after(:all) do
    File.delete(OUTPUT) if File.file?(OUTPUT)
  end
  
  it "-h should print the help" do
    help = bin(:args=>"-h", :no_conn=>true)
    help.should =~ /\ASequel: The Database Toolkit for Ruby/
    help.should =~ /^Usage: sequel /
  end

  it "-c should run code" do
    bin(:args=>'-c "print DB.tables.inspect"').should == '[]'
    DB.create_table(:a){Integer :a}
    bin(:args=>'-c "print DB.tables.inspect"').should == '[:a]'
  end

  it "-C should copy databases" do
    DB.create_table(:a) do
      primary_key :a
      String :name
    end
    DB.create_table(:b) do
      foreign_key :a, :a
      index :a
    end
    DB[:a].insert(1, 'foo')
    DB[:b].insert(1)
    bin(:args=>'-C', :post=>"#{CONN_PREFIX}#{BIN_SPEC_DB2}").should =~ Regexp.new(<<END)
Databases connections successful
Migrations dumped successfully
Tables created
Begin copying data
Begin copying records for table: a
Finished copying 1 records for table: a
Begin copying records for table: b
Finished copying 1 records for table: b
Finished copying data
Begin creating indexes
Finished creating indexes
Begin adding foreign key constraints
Finished adding foreign key constraints
Database copy finished in \\d\\.\\d+ seconds
END
    DB2.tables.sort_by{|t| t.to_s}.should == [:a, :b]
    DB[:a].all.should == [{:a=>1, :name=>'foo'}]
    DB[:b].all.should == [{:a=>1}]
    DB2.schema(:a).should == [[:a, {:allow_null=>false, :default=>nil, :primary_key=>true, :db_type=>"integer", :type=>:integer, :ruby_default=>nil}], [:name, {:allow_null=>true, :default=>nil, :primary_key=>false, :db_type=>"varchar(255)", :type=>:string, :ruby_default=>nil}]]
    DB2.schema(:b).should == [[:a, {:allow_null=>true, :default=>nil, :primary_key=>false, :db_type=>"integer", :type=>:integer, :ruby_default=>nil}]]
    DB2.indexes(:a).should == {}
    DB2.indexes(:b).should == {:b_a_index=>{:unique=>false, :columns=>[:a]}}
    DB2.foreign_key_list(:a).should == []
    DB2.foreign_key_list(:b).should == [{:columns=>[:a], :table=>:a, :key=>nil, :on_update=>:no_action, :on_delete=>:no_action}]
  end

  it "-d and -D should dump generic and specific migrations" do
    DB.create_table(:a) do
      primary_key :a
      String :name
    end
    DB.create_table(:b) do
      foreign_key :a, :a
      index :a
    end
    bin(:args=>'-d').should == <<END
Sequel.migration do
  change do
    create_table(:a) do
      primary_key :a
      String :name, :size=>255
    end
    
    create_table(:b, :ignore_index_errors=>true) do
      foreign_key :a, :a
      
      index [:a]
    end
  end
end
END
    bin(:args=>'-D').should == <<END
Sequel.migration do
  change do
    create_table(:a) do
      primary_key :a
      column :name, "varchar(255)"
    end
    
    create_table(:b) do
      foreign_key :a, :a
      
      index [:a]
    end
  end
end
END
  end

  it "-E should echo SQL statements to stdout" do
    bin(:args=>'-E -c DB.tables').should =~ %r{I, \[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+ #\d+\]  INFO -- : \(\d\.\d+s\) PRAGMA foreign_keys = 1\nI, \[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+ #\d+\]  INFO -- : \(\d\.\d+s\) PRAGMA case_sensitive_like = 1\nI, \[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+ #\d+\]  INFO -- : \(\d\.\d+s\) SELECT \* FROM `sqlite_master` WHERE \(type = 'table' AND NOT name = 'sqlite_sequence'\)\n}
  end

  it "-I should include directory in load path" do
    bin(:args=>'-Ifoo -c "p 1 if $:.include?(\'foo\')"').should == "1\n"
  end

  it "-l should log SQL statements to file" do
    bin(:args=>"-l #{TMP_FILE} -c DB.tables").should == ''
    File.read(TMP_FILE).should =~ %r{I, \[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+ #\d+\]  INFO -- : \(\d\.\d+s\) PRAGMA foreign_keys = 1\nI, \[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+ #\d+\]  INFO -- : \(\d\.\d+s\) PRAGMA case_sensitive_like = 1\nI, \[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+ #\d+\]  INFO -- : \(\d\.\d+s\) SELECT \* FROM `sqlite_master` WHERE \(type = 'table' AND NOT name = 'sqlite_sequence'\)\n}
  end

  it "-L should load all *.rb files in given directory" do
    bin(:args=>'-L ./lib/sequel/connection_pool -c "p [Sequel::SingleConnectionPool, Sequel::ThreadedConnectionPool, Sequel::ShardedSingleConnectionPool, Sequel::ShardedThreadedConnectionPool].length"').should == "4\n"
  end

  it "-m should migrate database up" do
    bin(:args=>"-m spec/files/integer_migrations").should == ''
    DB.tables.sort_by{|t| t.to_s}.should == [:schema_info, :sm1111, :sm2222, :sm3333]
  end

  it "-M should specify version to migrate to" do
    bin(:args=>"-m spec/files/integer_migrations -M 2").should == ''
    DB.tables.sort_by{|t| t.to_s}.should == [:schema_info, :sm1111, :sm2222]
  end

  it "-N should not test for a valid connection" do
    bin(:no_conn=>true, :args=>"-c '' -N #{CONN_PREFIX}spec/nonexistent/foo").should == ''
    bin(:no_conn=>true, :args=>"-c '' #{CONN_PREFIX}spec/nonexistent/foo", :stderr=>true).should =~ /\AError: Sequel::DatabaseConnectionError: /
  end

  it "-r should require a given library" do
    bin(:args=>'-rsequel/extensions/sql_expr -c "print DB.literal(1.sql_expr)"').should == "1"
  end

  it "-S should dump the schema cache" do
    bin(:args=>"-S #{TMP_FILE}").should == ''
    Marshal.load(File.read(TMP_FILE)).should == {}
    DB.create_table(:a){Integer :a}
    bin(:args=>"-S #{TMP_FILE}").should == ''
    Marshal.load(File.read(TMP_FILE)).should == {"`a`"=>[[:a, {:type=>:integer, :db_type=>"integer", :ruby_default=>nil, :allow_null=>true, :default=>nil, :primary_key=>false}]]}
  end

  it "-t should output full backtraces on error" do
    bin(:args=>'-c "lambda{lambda{lambda{raise \'foo\'}.call}.call}.call"', :stderr=>true).count("\n").should < 3
    bin(:args=>'-t -c "lambda{lambda{lambda{raise \'foo\'}.call}.call}.call"', :stderr=>true).count("\n").should > 3
  end

  it "-v should output the Sequel version" do
    bin(:args=>"-v", :no_conn=>true).should == "sequel #{Sequel.version}\n"
  end

  it "should error if using -M without -m" do
    bin(:args=>'-M 2', :stderr=>true).should == "Error: Must specify -m if using -M\n"
  end

  it "should error if using mutually exclusive options together" do
    bin(:args=>'-c foo -d', :stderr=>true).should == "Error: Cannot specify -c and -d together\n"
    bin(:args=>'-D -d', :stderr=>true).should == "Error: Cannot specify -D and -d together\n"
    bin(:args=>'-m foo -d', :stderr=>true).should == "Error: Cannot specify -m and -d together\n"
    bin(:args=>'-S foo -d', :stderr=>true).should == "Error: Cannot specify -S and -d together\n"
  end

  it "should use a mock database if no database is given" do
    bin(:args=>'-c "print DB.adapter_scheme"', :no_conn=>true).should == "mock"
  end

  it "should work with a yaml config file" do
    File.open(TMP_FILE, 'wb'){|f| f.write(YAML.dump(CONN_HASH))}
    bin(:args=>"-c \"print DB.tables.inspect\" #{TMP_FILE}", :no_conn=>true).should == "[]"
    DB.create_table(:a){Integer :a}
    bin(:args=>"-c \"print DB.tables.inspect\" #{TMP_FILE}", :no_conn=>true).should == "[:a]"
  end

  it "should work with a yaml config file with string keys" do
    h = {}
    CONN_HASH.each{|k,v| h[k.to_s] = v}
    File.open(TMP_FILE, 'wb'){|f| f.write(YAML.dump(h))}
    DB.create_table(:a){Integer :a}
    bin(:args=>"-c \"print DB.tables.inspect\" #{TMP_FILE}", :no_conn=>true).should == "[:a]"
  end

  it "should work with a yaml config file with environments" do
    File.open(TMP_FILE, 'wb'){|f| f.write(YAML.dump(:development=>CONN_HASH))}
    bin(:args=>"-c \"print DB.tables.inspect\" #{TMP_FILE}", :no_conn=>true).should == "[]"
    DB.create_table(:a){Integer :a}
    bin(:args=>"-c \"print DB.tables.inspect\" #{TMP_FILE}", :no_conn=>true).should == "[:a]"
  end

  it "-e should set environment for yaml config file" do
    File.open(TMP_FILE, 'wb'){|f| f.write(YAML.dump(:foo=>CONN_HASH))}
    bin(:args=>"-c \"print DB.tables.inspect\" -e foo #{TMP_FILE}", :no_conn=>true).should == "[]"
    DB.create_table(:a){Integer :a}
    bin(:args=>"-c \"print DB.tables.inspect\" -e foo #{TMP_FILE}", :no_conn=>true).should == "[:a]"
    File.open(TMP_FILE, 'wb'){|f| f.write(YAML.dump('foo'=>CONN_HASH))}
    bin(:args=>"-c \"print DB.tables.inspect\" -e foo #{TMP_FILE}", :no_conn=>true).should == "[:a]"
  end

  it "should run code in given filenames" do
    File.open(TMP_FILE, 'wb'){|f| f.write('print DB.tables.inspect')}
    bin(:post=>TMP_FILE).should == '[]'
    DB.create_table(:a){Integer :a}
    bin(:post=>TMP_FILE).should == '[:a]'
  end

  it "should run code provided on stdin" do
    bin(:pre=>'echo print DB.tables.inspect | ').should == '[]'
    DB.create_table(:a){Integer :a}
    bin(:pre=>'echo print DB.tables.inspect | ').should == '[:a]'
  end
end
