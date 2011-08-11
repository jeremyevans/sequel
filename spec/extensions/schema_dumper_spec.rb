require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Sequel::Schema::Generator dump methods" do
  before do
    @d = Sequel::Database.new
    @g = Sequel::Schema::Generator
  end

  it "should allow the same table information to be converted to a string for evaling inside of another instance with the same result" do
    g = @g.new(@d) do
      Integer :a
      varchar :b
      column :dt, DateTime
      column :vc, :varchar
      primary_key :c
      foreign_key :d, :a
      foreign_key :e
      foreign_key [:d, :e], :name=>:cfk
      constraint :blah, "a=1"
      check :a=>1
      unique [:e]
      index :a
      index [:c, :e]
      index [:b, :c], :type=>:hash
      index [:d], :unique=>true
      spatial_index :a
      full_text_index [:b, :c]
    end
    g2 = @g.new(@d) do
      instance_eval(g.dump_columns, __FILE__, __LINE__)
      instance_eval(g.dump_constraints, __FILE__, __LINE__)
      instance_eval(g.dump_indexes, __FILE__, __LINE__)
    end
    g.columns.should == g2.columns
    g.constraints.should == g2.constraints
    g.indexes.should == g2.indexes
  end

  it "should allow dumping indexes as separate add_index and drop_index methods" do
    g = @g.new(@d) do
      index :a
      index [:c, :e], :name=>:blah
      index [:b, :c], :unique=>true
    end

    t = <<END_CODE
add_index :t, [:a]
add_index :t, [:c, :e], :name=>:blah
add_index :t, [:b, :c], :unique=>true
END_CODE
    g.dump_indexes(:add_index=>:t).should == t.strip

    t = <<END_CODE
drop_index :t, [:a]
drop_index :t, [:c, :e], :name=>:blah
drop_index :t, [:b, :c], :unique=>true
END_CODE
    g.dump_indexes(:drop_index=>:t).should == t.strip
  end

  it "should raise an error if you try to dump a Generator that uses a constraint with a proc" do
    proc{@g.new(@d){check{a>1}}.dump_constraints}.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Database dump methods" do
  before do
    @d = Sequel::Database.new
    @d.meta_def(:tables){|o| [:t1, :t2]}
    @d.meta_def(:schema) do |t, *o|
      case t
      when :t1, 't__t1', :t__t1.identifier
        [[:c1, {:db_type=>'integer', :primary_key=>true, :allow_null=>false}],
         [:c2, {:db_type=>'varchar(20)', :allow_null=>true}]]
      when :t2
        [[:c1, {:db_type=>'integer', :primary_key=>true, :allow_null=>false}],
         [:c2, {:db_type=>'numeric', :primary_key=>true, :allow_null=>false}]]
      when :t3
        [[:c1, {:db_type=>'date', :default=>"'now()'", :allow_null=>true}],
         [:c2, {:db_type=>'datetime', :allow_null=>false}]]
      when :t5
        [[:c1, {:db_type=>'blahblah', :allow_null=>true}]]
      when :t6
        [[:c1, {:db_type=>'bigint', :primary_key=>true, :allow_null=>true}]]
      when :t7
        [[:c1, {:db_type=>'somedbspecifictype', :primary_key=>true, :allow_null=>false}]]
      end
    end
  end

  it "should support dumping table schemas as create_table method calls" do
    @d.dump_table_schema(:t1).should == "create_table(:t1) do\n  primary_key :c1\n  String :c2, :size=>20\nend"
  end

  it "should support dumping table schemas when given a string" do
    @d.dump_table_schema('t__t1').should == "create_table(\"t__t1\") do\n  primary_key :c1\n  String :c2, :size=>20\nend"
  end

  it "should support dumping table schemas when given an identifier" do
    @d.dump_table_schema(:t__t1.identifier).should == "create_table(\"t__t1\") do\n  primary_key :c1\n  String :c2, :size=>20\nend"
  end

  it "should dump non-Integer primary key columns with explicit :type" do
    @d.dump_table_schema(:t6).should == "create_table(:t6) do\n  primary_key :c1, :type=>Bignum\nend"
  end

  it "should dump primary key columns with explicit :type equal to the database type when :same_db option is passed" do
    @d.dump_table_schema(:t7, :same_db => true).should == "create_table(:t7) do\n  primary_key :c1, :type=>\"somedbspecifictype\"\nend"
  end

  it "should use a composite primary_key calls if there is a composite primary key" do
    @d.dump_table_schema(:t2).should == "create_table(:t2) do\n  Integer :c1, :null=>false\n  BigDecimal :c2, :null=>false\n  \n  primary_key [:c1, :c2]\nend"
  end

  it "should include index information if available" do
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_table_schema(:t1).should == "create_table(:t1, :ignore_index_errors=>true) do\n  primary_key :c1\n  String :c2, :size=>20\n  \n  index [:c1], :name=>:i1\n  index [:c2, :c1], :unique=>true\nend"
  end

  it "should support dumping the whole database as a migration" do
    @d.dump_schema_migration.should == <<-END_MIG
Sequel.migration do
  up do
    create_table(:t1) do
      primary_key :c1
      String :c2, :size=>20
    end
    
    create_table(:t2) do
      Integer :c1, :null=>false
      BigDecimal :c2, :null=>false
      
      primary_key [:c1, :c2]
    end
  end
  
  down do
    drop_table(:t1, :t2)
  end
end
END_MIG
  end

  it "should sort table names when dumping a migration" do
    @d.meta_def(:tables){|o| [:t2, :t1]}
    @d.dump_schema_migration.should == <<-END_MIG
Sequel.migration do
  up do
    create_table(:t1) do
      primary_key :c1
      String :c2, :size=>20
    end
    
    create_table(:t2) do
      Integer :c1, :null=>false
      BigDecimal :c2, :null=>false
      
      primary_key [:c1, :c2]
    end
  end
  
  down do
    drop_table(:t1, :t2)
  end
end
END_MIG
  end

  it "should honor the :same_db option to not convert types" do
    @d.dump_table_schema(:t1, :same_db=>true).should == "create_table(:t1) do\n  primary_key :c1\n  column :c2, \"varchar(20)\"\nend"
    @d.dump_schema_migration(:same_db=>true).should == <<-END_MIG
Sequel.migration do
  up do
    create_table(:t1) do
      primary_key :c1
      column :c2, "varchar(20)"
    end
    
    create_table(:t2) do
      column :c1, "integer", :null=>false
      column :c2, "numeric", :null=>false
      
      primary_key [:c1, :c2]
    end
  end
  
  down do
    drop_table(:t1, :t2)
  end
end
END_MIG
  end

  it "should honor the :indexes => false option to not include indexes" do
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_table_schema(:t1, :indexes=>false).should == "create_table(:t1) do\n  primary_key :c1\n  String :c2, :size=>20\nend"
    @d.dump_schema_migration(:indexes=>false).should == <<-END_MIG
Sequel.migration do
  up do
    create_table(:t1) do
      primary_key :c1
      String :c2, :size=>20
    end
    
    create_table(:t2) do
      Integer :c1, :null=>false
      BigDecimal :c2, :null=>false
      
      primary_key [:c1, :c2]
    end
  end
  
  down do
    drop_table(:t1, :t2)
  end
end
END_MIG
  end

  it "should support dumping just indexes as a migration" do
    @d.meta_def(:tables){|o| [:t1]}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_indexes_migration.should == <<-END_MIG
Sequel.migration do
  up do
    add_index :t1, [:c1], :ignore_errors=>true, :name=>:i1
    add_index :t1, [:c2, :c1], :ignore_errors=>true, :unique=>true
  end
  
  down do
    drop_index :t1, [:c1], :ignore_errors=>true, :name=>:i1
    drop_index :t1, [:c2, :c1], :ignore_errors=>true, :unique=>true
  end
end
END_MIG
  end

  it "should handle not null values and defaults" do
    @d.dump_table_schema(:t3).should == "create_table(:t3) do\n  Date :c1\n  DateTime :c2, :null=>false\nend"
  end
  
  it "should handle converting common defaults" do
    @d.meta_def(:schema) do |t, *os|
      s = [[:c1, {:db_type=>'boolean', :default=>"false", :type=>:boolean, :allow_null=>true}],
       [:c2, {:db_type=>'varchar', :default=>"'blah'", :type=>:string, :allow_null=>true}],
       [:c3, {:db_type=>'integer', :default=>"-1", :type=>:integer, :allow_null=>true}],
       [:c4, {:db_type=>'float', :default=>"1.0", :type=>:float, :allow_null=>true}],
       [:c5, {:db_type=>'decimal', :default=>"100.50", :type=>:decimal, :allow_null=>true}],
       [:c6, {:db_type=>'blob', :default=>"'blah'", :type=>:blob, :allow_null=>true}],
       [:c7, {:db_type=>'date', :default=>"'2008-10-29'", :type=>:date, :allow_null=>true}],
       [:c8, {:db_type=>'datetime', :default=>"'2008-10-29 10:20:30'", :type=>:datetime, :allow_null=>true}],
       [:c9, {:db_type=>'time', :default=>"'10:20:30'", :type=>:time, :allow_null=>true}],
       [:c10, {:db_type=>'interval', :default=>"'6 weeks'", :type=>:interval, :allow_null=>true}]]
      s.each{|_, c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      s
    end
    @d.dump_table_schema(:t4).gsub(/[+-]\d\d:\d\d"\)/, '")').should == "create_table(:t4) do\n  TrueClass :c1, :default=>false\n  String :c2, :default=>\"blah\"\n  Integer :c3, :default=>-1\n  Float :c4, :default=>1.0\n  BigDecimal :c5, :default=>BigDecimal.new(\"0.1005E3\")\n  File :c6, :default=>Sequel::SQL::Blob.new(\"blah\")\n  Date :c7, :default=>Date.parse(\"2008-10-29\")\n  DateTime :c8, :default=>DateTime.parse(\"2008-10-29T10:20:30\")\n  Time :c9, :default=>Sequel::SQLTime.parse(\"10:20:30\"), :only_time=>true\n  String :c10\nend"
    @d.dump_table_schema(:t4, :same_db=>true).gsub(/[+-]\d\d:\d\d"\)/, '")').should == "create_table(:t4) do\n  column :c1, \"boolean\", :default=>false\n  column :c2, \"varchar\", :default=>\"blah\"\n  column :c3, \"integer\", :default=>-1\n  column :c4, \"float\", :default=>1.0\n  column :c5, \"decimal\", :default=>BigDecimal.new(\"0.1005E3\")\n  column :c6, \"blob\", :default=>Sequel::SQL::Blob.new(\"blah\")\n  column :c7, \"date\", :default=>Date.parse(\"2008-10-29\")\n  column :c8, \"datetime\", :default=>DateTime.parse(\"2008-10-29T10:20:30\")\n  column :c9, \"time\", :default=>Sequel::SQLTime.parse(\"10:20:30\")\n  column :c10, \"interval\", :default=>\"'6 weeks'\".lit\nend"
  end
  
  it "should not use a '...'.lit as a fallback if using MySQL with the :same_db option" do
    @d.meta_def(:database_type){:mysql}
    @d.meta_def(:schema) do |t, *os|
      s = [[:c10, {:db_type=>'interval', :default=>"'6 weeks'", :type=>:interval, :allow_null=>true}]]
      s.each{|_, c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      s
    end
    @d.dump_table_schema(:t5, :same_db=>true).should == "create_table(:t5) do\n  column :c10, \"interval\"\nend"
  end

  it "should convert unknown database types to strings" do
    @d.dump_table_schema(:t5).should == "create_table(:t5) do\n  String :c1\nend"
  end

  it "should convert many database types to ruby types" do
    types = %w"mediumint smallint int integer mediumint(6) smallint(7) int(8) integer(9)
      tinyint tinyint(2) bigint bigint(20) real float double boolean tinytext mediumtext
      longtext text clob date datetime timestamp time char character
      varchar varchar(255) varchar(30) bpchar string money
      decimal decimal(10,2) numeric numeric(15,3) number bytea tinyblob mediumblob longblob
      blob varbinary varbinary(10) binary binary(20) year" +
      ["double precision", "timestamp with time zone", "timestamp without time zone",
       "time with time zone", "time without time zone", "character varying(20)"] +
      %w"nvarchar ntext smalldatetime smallmoney binary varbinary nchar" +
      ["timestamp(6) without time zone", "timestamp(6) with time zone", "int(12) unsigned", 'bigint unsigned']
    @d.meta_def(:schema) do |t, *o|
      i = 0
      types.map{|x| [:"c#{i+=1}", {:db_type=>x, :allow_null=>true}]}
    end
    table = <<END_MIG
create_table(:x) do
  Integer :c1
  Integer :c2
  Integer :c3
  Integer :c4
  Integer :c5
  Integer :c6
  Integer :c7
  Integer :c8
  Integer :c9
  Integer :c10
  Bignum :c11
  Bignum :c12
  Float :c13
  Float :c14
  Float :c15
  TrueClass :c16
  String :c17, :text=>true
  String :c18, :text=>true
  String :c19, :text=>true
  String :c20, :text=>true
  String :c21, :text=>true
  Date :c22
  DateTime :c23
  DateTime :c24
  Time :c25, :only_time=>true
  String :c26, :fixed=>true
  String :c27, :fixed=>true
  String :c28
  String :c29, :size=>255
  String :c30, :size=>30
  String :c31
  String :c32
  BigDecimal :c33, :size=>[19, 2]
  BigDecimal :c34
  BigDecimal :c35, :size=>[10, 2]
  BigDecimal :c36
  BigDecimal :c37, :size=>[15, 3]
  BigDecimal :c38
  File :c39
  File :c40
  File :c41
  File :c42
  File :c43
  File :c44
  File :c45, :size=>10
  File :c46
  File :c47, :size=>20
  Integer :c48
  Float :c49
  DateTime :c50
  DateTime :c51
  Time :c52, :only_time=>true
  Time :c53, :only_time=>true
  String :c54, :size=>20
  String :c55
  String :c56, :text=>true
  DateTime :c57
  BigDecimal :c58, :size=>[19, 2]
  File :c59
  File :c60
  String :c61, :fixed=>true
  DateTime :c62, :size=>6
  DateTime :c63, :size=>6
  Integer :c64
  Bignum :c65
end
END_MIG
    @d.dump_table_schema(:x).should == table.chomp
  end
end
