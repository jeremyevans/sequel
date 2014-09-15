require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Sequel::Schema::Generator dump methods" do
  before do
    @d = Sequel::Database.new.extension(:schema_dumper)
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

    g.dump_indexes(:add_index=>:t).should == (<<END_CODE).strip
add_index :t, [:a]
add_index :t, [:c, :e], :name=>:blah
add_index :t, [:b, :c], :unique=>true
END_CODE

    g.dump_indexes(:drop_index=>:t).should == (<<END_CODE).strip
drop_index :t, [:b, :c], :unique=>true
drop_index :t, [:c, :e], :name=>:blah
drop_index :t, [:a]
END_CODE
  end

  it "should raise an error if you try to dump a Generator that uses a constraint with a proc" do
    proc{@g.new(@d){check{a>1}}.dump_constraints}.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Database dump methods" do
  before do
    @d = Sequel::Database.new.extension(:schema_dumper)
    @d.meta_def(:tables){|o| [:t1, :t2]}
    @d.meta_def(:schema) do |t, *o|
      case t
      when :t1, 't__t1', Sequel.identifier(:t__t1)
        [[:c1, {:db_type=>'integer', :primary_key=>true, :allow_null=>false}],
         [:c2, {:db_type=>'varchar(20)', :allow_null=>true}]]
      when :t2
        [[:c1, {:db_type=>'integer', :primary_key=>true, :allow_null=>false}],
         [:c2, {:db_type=>'numeric', :primary_key=>true, :allow_null=>false}]]
      when :t5
        [[:c1, {:db_type=>'blahblah', :allow_null=>true}]]
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
    @d.dump_table_schema(Sequel.identifier(:t__t1)).should == "create_table(\"t__t1\") do\n  primary_key :c1\n  String :c2, :size=>20\nend"
  end

  it "should dump non-Integer primary key columns with explicit :type" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'bigint', :primary_key=>true, :allow_null=>true}]]}
    @d.dump_table_schema(:t6).should == "create_table(:t6) do\n  primary_key :c1, :type=>Bignum\nend"
  end

  it "should handle foreign keys" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer', :allow_null=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|*s| [{:columns=>[:c1], :table=>:t2, :key=>[:c2]}]}
    @d.dump_table_schema(:t6).should == "create_table(:t6) do\n  foreign_key :c1, :t2, :key=>[:c2]\nend"
  end

  it "should handle primary keys that are also foreign keys" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer', :primary_key=>true, :allow_null=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|*s| [{:columns=>[:c1], :table=>:t2, :key=>[:c2]}]}
    s = @d.dump_table_schema(:t6)
    s.should =~ /create_table\(:t6\) do\n  primary_key :c1, /
    s.should =~ /:table=>:t2/
    s.should =~ /:key=>\[:c2\]/
  end

  it "should handle foreign key options" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer', :allow_null=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|*s| [{:columns=>[:c1], :table=>:t2, :key=>[:c2], :on_delete=>:restrict, :on_update=>:set_null, :deferrable=>true}]}
    s = @d.dump_table_schema(:t6)
    s.should =~ /create_table\(:t6\) do\n  foreign_key :c1, :t2, /
    s.should =~ /:key=>\[:c2\]/
    s.should =~ /:on_delete=>:restrict/
    s.should =~ /:on_update=>:set_null/
    s.should =~ /:deferrable=>true/
  end

  it "should handle foreign key options in the primary key" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer', :primary_key=>true, :allow_null=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|*s| [{:columns=>[:c1], :table=>:t2, :key=>[:c2], :on_delete=>:restrict, :on_update=>:set_null, :deferrable=>true}]}
    s = @d.dump_table_schema(:t6)
    s.should =~ /create_table\(:t6\) do\n  primary_key :c1, /
    s.should =~ /:table=>:t2/
    s.should =~ /:key=>\[:c2\]/
    s.should =~ /:on_delete=>:restrict/
    s.should =~ /:on_update=>:set_null/
    s.should =~ /:deferrable=>true/
  end

  it "should omit foreign key options that are the same as defaults" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer', :allow_null=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|*s| [{:columns=>[:c1], :table=>:t2, :key=>[:c2], :on_delete=>:no_action, :on_update=>:no_action, :deferrable=>false}]}
    s = @d.dump_table_schema(:t6)
    s.should =~ /create_table\(:t6\) do\n  foreign_key :c1, :t2, /
    s.should =~ /:key=>\[:c2\]/
    s.should_not =~ /:on_delete/
    s.should_not =~ /:on_update/
    s.should_not =~ /:deferrable/
  end

  it "should omit foreign key options that are the same as defaults in the primary key" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer', :primary_key=>true, :allow_null=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|*s| [{:columns=>[:c1], :table=>:t2, :key=>[:c2], :on_delete=>:no_action, :on_update=>:no_action, :deferrable=>false}]}
    s = @d.dump_table_schema(:t6)
    s.should =~ /create_table\(:t6\) do\n  primary_key :c1, /
    s.should =~ /:table=>:t2/
    s.should =~ /:key=>\[:c2\]/
    s.should_not =~ /:on_delete/
    s.should_not =~ /:on_update/
    s.should_not =~ /:deferrable/
  end

  it "should dump primary key columns with explicit type equal to the database type when :same_db option is passed" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'somedbspecifictype', :primary_key=>true, :allow_null=>false}]]}
    @d.dump_table_schema(:t7, :same_db => true).should == "create_table(:t7) do\n  column :c1, \"somedbspecifictype\", :null=>false\n  \n  primary_key [:c1]\nend"
  end

  it "should use a composite primary_key calls if there is a composite primary key" do
    @d.dump_table_schema(:t2).should == "create_table(:t2) do\n  Integer :c1, :null=>false\n  BigDecimal :c2, :null=>false\n  \n  primary_key [:c1, :c2]\nend"
  end

  it "should use a composite foreign_key calls if there is a composite foreign key" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer'}], [:c2, {:db_type=>'integer'}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|*s| [{:columns=>[:c1, :c2], :table=>:t2, :key=>[:c3, :c4]}]}
    @d.dump_table_schema(:t1).should == "create_table(:t1) do\n  Integer :c1\n  Integer :c2\n  \n  foreign_key [:c1, :c2], :t2, :key=>[:c3, :c4]\nend"
  end

  it "should include index information if available" do
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_table_schema(:t1).should == "create_table(:t1, :ignore_index_errors=>true) do\n  primary_key :c1\n  String :c2, :size=>20\n  \n  index [:c1], :name=>:i1\n  index [:c2, :c1], :unique=>true\nend"
  end

  it "should support dumping the whole database as a migration" do
    @d.dump_schema_migration.should == <<-END_MIG
Sequel.migration do
  change do
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
end
END_MIG
  end

  it "should sort table names when dumping a migration" do
    @d.meta_def(:tables){|o| [:t2, :t1]}
    @d.dump_schema_migration.should == <<-END_MIG
Sequel.migration do
  change do
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
end
END_MIG
  end

  it "should sort table names topologically when dumping a migration with foreign keys" do
    @d.meta_def(:tables){|o| [:t1, :t2]}
    @d.meta_def(:schema) do |t|
      t == :t1 ? [[:c2, {:db_type=>'integer'}]] : [[:c1, {:db_type=>'integer', :primary_key=>true}]]
    end
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list) do |t|
      t == :t1 ? [{:columns=>[:c2], :table=>:t2, :key=>[:c1]}] : []
    end
    @d.dump_schema_migration.should == <<-END_MIG
Sequel.migration do
  change do
    create_table(:t2) do
      primary_key :c1
    end
    
    create_table(:t1) do
      foreign_key :c2, :t2, :key=>[:c1]
    end
  end
end
END_MIG
  end

  it "should handle circular dependencies when dumping a migration with foreign keys" do
    @d.meta_def(:tables){|o| [:t1, :t2]}
    @d.meta_def(:schema) do |t|
      t == :t1 ? [[:c2, {:db_type=>'integer'}]] : [[:c1, {:db_type=>'integer'}]]
    end
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list) do |t|
      t == :t1 ? [{:columns=>[:c2], :table=>:t2, :key=>[:c1]}] : [{:columns=>[:c1], :table=>:t1, :key=>[:c2]}]
    end
    @d.dump_schema_migration.should == <<-END_MIG
Sequel.migration do
  change do
    create_table(:t1) do
      Integer :c2
    end
    
    create_table(:t2) do
      foreign_key :c1, :t1, :key=>[:c2]
    end
    
    alter_table(:t1) do
      add_foreign_key [:c2], :t2, :key=>[:c1]
    end
  end
end
END_MIG
  end

  it "should sort topologically even if the database raises an error when trying to parse foreign keys for a non-existent table" do
    @d.meta_def(:tables){|o| [:t1, :t2]}
    @d.meta_def(:schema) do |t|
      t == :t1 ? [[:c2, {:db_type=>'integer'}]] : [[:c1, {:db_type=>'integer', :primary_key=>true}]]
    end
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list) do |t|
      raise Sequel::DatabaseError unless [:t1, :t2].include?(t)
      t == :t1 ? [{:columns=>[:c2], :table=>:t2, :key=>[:c1]}] : []
    end
    @d.dump_schema_migration.should == <<-END_MIG
Sequel.migration do
  change do
    create_table(:t2) do
      primary_key :c1
    end
    
    create_table(:t1) do
      foreign_key :c2, :t2, :key=>[:c1]
    end
  end
end
END_MIG
  end

  it "should honor the :same_db option to not convert types" do
    @d.dump_table_schema(:t1, :same_db=>true).should == "create_table(:t1) do\n  primary_key :c1\n  column :c2, \"varchar(20)\"\nend"
    @d.dump_schema_migration(:same_db=>true).should == <<-END_MIG
Sequel.migration do
  change do
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
end
END_MIG
  end

  it "should honor the :index_names => false option to not include names of indexes" do
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_table_schema(:t1, :index_names=>false).should == "create_table(:t1, :ignore_index_errors=>true) do\n  primary_key :c1\n  String :c2, :size=>20\n  \n  index [:c1]\n  index [:c2, :c1], :unique=>true\nend"
    @d.dump_schema_migration(:index_names=>false).should == <<-END_MIG
Sequel.migration do
  change do
    create_table(:t1, :ignore_index_errors=>true) do
      primary_key :c1
      String :c2, :size=>20
      
      index [:c1]
      index [:c2, :c1], :unique=>true
    end
    
    create_table(:t2, :ignore_index_errors=>true) do
      Integer :c1, :null=>false
      BigDecimal :c2, :null=>false
      
      primary_key [:c1, :c2]
      
      index [:c1]
      index [:c2, :c1], :unique=>true
    end
  end
end
END_MIG
  end
  
  it "should make :index_names => :namespace option a noop if there is a  global index namespace" do
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>false}}
    end
    @d.dump_table_schema(:t1, :index_names=>:namespace).should == "create_table(:t1, :ignore_index_errors=>true) do\n  primary_key :c1\n  String :c2, :size=>20\n  \n  index [:c1], :name=>:i1\n  index [:c2, :c1]\nend"
    @d.dump_schema_migration(:index_names=>:namespace).should == <<-END_MIG
Sequel.migration do
  change do
    create_table(:t1, :ignore_index_errors=>true) do
      primary_key :c1
      String :c2, :size=>20
      
      index [:c1], :name=>:i1
      index [:c2, :c1]
    end
    
    create_table(:t2, :ignore_index_errors=>true) do
      Integer :c1, :null=>false
      BigDecimal :c2, :null=>false
      
      primary_key [:c1, :c2]
      
      index [:c1], :name=>:i1
      index [:c2, :c1], :name=>:t1_c2_c1_index
    end
  end
end
END_MIG
  end

  it "should honor the :index_names => :namespace option to include names of indexes with prepended table name if there is no global index namespace" do
    @d.meta_def(:global_index_namespace?){false}
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>false}}
    end
    @d.dump_table_schema(:t1, :index_names=>:namespace).should == "create_table(:t1, :ignore_index_errors=>true) do\n  primary_key :c1\n  String :c2, :size=>20\n  \n  index [:c1], :name=>:t1_i1\n  index [:c2, :c1]\nend"
    @d.dump_schema_migration(:index_names=>:namespace).should == <<-END_MIG
Sequel.migration do
  change do
    create_table(:t1, :ignore_index_errors=>true) do
      primary_key :c1
      String :c2, :size=>20
      
      index [:c1], :name=>:t1_i1
      index [:c2, :c1]
    end
    
    create_table(:t2, :ignore_index_errors=>true) do
      Integer :c1, :null=>false
      BigDecimal :c2, :null=>false
      
      primary_key [:c1, :c2]
      
      index [:c1], :name=>:t2_i1
      index [:c2, :c1], :name=>:t2_t1_c2_c1_index
    end
  end
end
END_MIG
  end

  it "should honor the :indexes => false option to not include indexes" do
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_table_schema(:t1, :indexes=>false).should == "create_table(:t1) do\n  primary_key :c1\n  String :c2, :size=>20\nend"
    @d.dump_schema_migration(:indexes=>false).should == <<-END_MIG
Sequel.migration do
  change do
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
end
END_MIG
  end

  it "should have :indexes => false option disable foreign keys as well when dumping a whole migration" do
    @d.meta_def(:foreign_key_list) do |t|
      t == :t1 ? [{:columns=>[:c2], :table=>:t2, :key=>[:c1]}] : []
    end
    @d.dump_schema_migration(:indexes=>false).should_not =~ /foreign_key/
  end

  it "should have :foreign_keys option override :indexes => false disabling of foreign keys" do
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list) do |t|
      t == :t1 ? [{:columns=>[:c2], :table=>:t2, :key=>[:c1]}] : []
    end
    @d.dump_schema_migration(:indexes=>false, :foreign_keys=>true).should =~ /foreign_key/
  end

  it "should support dumping just indexes as a migration" do
    @d.meta_def(:tables){|o| [:t1]}
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_indexes_migration.should == <<-END_MIG
Sequel.migration do
  change do
    add_index :t1, [:c1], :ignore_errors=>true, :name=>:i1
    add_index :t1, [:c2, :c1], :ignore_errors=>true, :unique=>true
  end
end
END_MIG
  end

  it "should honor the :index_names => false option to not include names of indexes when dumping just indexes as a migration" do
    @d.meta_def(:tables){|o| [:t1]}
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>true}}
    end
    @d.dump_indexes_migration(:index_names=>false).should == <<-END_MIG
Sequel.migration do
  change do
    add_index :t1, [:c1], :ignore_errors=>true
    add_index :t1, [:c2, :c1], :ignore_errors=>true, :unique=>true
  end
end
END_MIG
  end

  it "should honor the :index_names => :namespace option be a noop if there is a global index namespace" do
    @d.meta_def(:tables){|o| [:t1, :t2]}
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>false}}
    end
    @d.dump_indexes_migration(:index_names=>:namespace).should == <<-END_MIG
Sequel.migration do
  change do
    add_index :t1, [:c1], :ignore_errors=>true, :name=>:i1
    add_index :t1, [:c2, :c1], :ignore_errors=>true
    
    add_index :t2, [:c1], :ignore_errors=>true, :name=>:i1
    add_index :t2, [:c2, :c1], :ignore_errors=>true, :name=>:t1_c2_c1_index
  end
end
END_MIG
  end

  it "should honor the :index_names => :namespace option to include names of indexes with prepended table name when dumping just indexes as a migration if there is no global index namespace" do
    @d.meta_def(:global_index_namespace?){false}
    @d.meta_def(:tables){|o| [:t1, :t2]}
    @d.meta_def(:supports_index_parsing?){true}
    @d.meta_def(:indexes) do |t|
      {:i1=>{:columns=>[:c1], :unique=>false},
       :t1_c2_c1_index=>{:columns=>[:c2, :c1], :unique=>false}}
    end
    @d.dump_indexes_migration(:index_names=>:namespace).should == <<-END_MIG
Sequel.migration do
  change do
    add_index :t1, [:c1], :ignore_errors=>true, :name=>:t1_i1
    add_index :t1, [:c2, :c1], :ignore_errors=>true
    
    add_index :t2, [:c1], :ignore_errors=>true, :name=>:t2_i1
    add_index :t2, [:c2, :c1], :ignore_errors=>true, :name=>:t2_t1_c2_c1_index
  end
end
END_MIG
  end

  it "should handle missing index parsing support when dumping index migration" do
    @d.meta_def(:tables){|o| [:t1]}
    @d.dump_indexes_migration.should == <<-END_MIG
Sequel.migration do
  change do
    
  end
end
END_MIG
  end

  it "should handle missing foreign key parsing support when dumping foreign key migration" do
    @d.meta_def(:tables){|o| [:t1]}
    @d.dump_foreign_key_migration.should == <<-END_MIG
Sequel.migration do
  change do
    
  end
end
END_MIG
  end

  it "should support dumping just foreign_keys as a migration" do
    @d.meta_def(:tables){|o| [:t1, :t2, :t3]}
    @d.meta_def(:schema) do |t|
      t == :t1 ? [[:c2, {:db_type=>'integer'}]] : [[:c1, {:db_type=>'integer'}]]
    end
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list) do |t, *a|
      case t
      when :t1
        [{:columns=>[:c2], :table=>:t2, :key=>[:c1]}]
      when :t2
        [{:columns=>[:c1, :c3], :table=>:t1, :key=>[:c2, :c4]}]
      else
        []
      end
    end
    @d.dump_foreign_key_migration.should == <<-END_MIG
Sequel.migration do
  change do
    alter_table(:t1) do
      add_foreign_key [:c2], :t2, :key=>[:c1]
    end
    
    alter_table(:t2) do
      add_foreign_key [:c1, :c3], :t1, :key=>[:c2, :c4]
    end
  end
end
END_MIG
  end

  it "should handle not null values and defaults" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'date', :default=>"'now()'", :allow_null=>true}], [:c2, {:db_type=>'datetime', :allow_null=>false}]]}
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
       [:c10, {:db_type=>'foo', :default=>"'6 weeks'", :type=>nil, :allow_null=>true}],
       [:c11, {:db_type=>'date', :default=>"CURRENT_DATE", :type=>:date, :allow_null=>true}],
       [:c12, {:db_type=>'timestamp', :default=>"now()", :type=>:datetime, :allow_null=>true}]]
      s.each{|_, c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      s
    end
    @d.dump_table_schema(:t4).gsub(/[+-]\d\d\d\d"\)/, '")').gsub(/\.0+/, '.0').should == "create_table(:t4) do\n  TrueClass :c1, :default=>false\n  String :c2, :default=>\"blah\"\n  Integer :c3, :default=>-1\n  Float :c4, :default=>1.0\n  BigDecimal :c5, :default=>BigDecimal.new(\"0.1005E3\")\n  File :c6, :default=>Sequel::SQL::Blob.new(\"blah\")\n  Date :c7, :default=>Date.new(2008, 10, 29)\n  DateTime :c8, :default=>DateTime.parse(\"2008-10-29T10:20:30.0\")\n  Time :c9, :default=>Sequel::SQLTime.parse(\"10:20:30.0\"), :only_time=>true\n  String :c10\n  Date :c11, :default=>Sequel::CURRENT_DATE\n  DateTime :c12, :default=>Sequel::CURRENT_TIMESTAMP\nend"
    @d.dump_table_schema(:t4, :same_db=>true).gsub(/[+-]\d\d\d\d"\)/, '")').gsub(/\.0+/, '.0').should == "create_table(:t4) do\n  column :c1, \"boolean\", :default=>false\n  column :c2, \"varchar\", :default=>\"blah\"\n  column :c3, \"integer\", :default=>-1\n  column :c4, \"float\", :default=>1.0\n  column :c5, \"decimal\", :default=>BigDecimal.new(\"0.1005E3\")\n  column :c6, \"blob\", :default=>Sequel::SQL::Blob.new(\"blah\")\n  column :c7, \"date\", :default=>Date.new(2008, 10, 29)\n  column :c8, \"datetime\", :default=>DateTime.parse(\"2008-10-29T10:20:30.0\")\n  column :c9, \"time\", :default=>Sequel::SQLTime.parse(\"10:20:30.0\")\n  column :c10, \"foo\", :default=>Sequel::LiteralString.new(\"'6 weeks'\")\n  column :c11, \"date\", :default=>Sequel::CURRENT_DATE\n  column :c12, \"timestamp\", :default=>Sequel::CURRENT_TIMESTAMP\nend"
  end
  
  it "should not use a literal string as a fallback if using MySQL with the :same_db option" do
    @d.meta_def(:database_type){:mysql}
    @d.meta_def(:supports_index_parsing?){false}
    @d.meta_def(:supports_foreign_key_parsing?){false}
    @d.meta_def(:schema) do |t, *os|
      s = [[:c10, {:db_type=>'foo', :default=>"'6 weeks'", :type=>nil, :allow_null=>true}]]
      s.each{|_, c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      s
    end
    @d.dump_table_schema(:t5, :same_db=>true).should == "create_table(:t5) do\n  column :c10, \"foo\"\nend"
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
      ["timestamp(6) without time zone", "timestamp(6) with time zone", 'mediumint(10) unsigned', 'int(9) unsigned',
       'int(10) unsigned', "int(12) unsigned", 'bigint unsigned', 'tinyint(3) unsigned', 'identity', 'int identity'] +
      %w"integer(10)"
    @d.meta_def(:schema) do |t, *o|
      i = 0
      types.map{|x| [:"c#{i+=1}", {:db_type=>x, :allow_null=>true}]}
    end
    @d.dump_table_schema(:x).should == (<<END_MIG).chomp
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
  Integer :c65
  Bignum :c66
  Bignum :c67
  Bignum :c68
  Integer :c69
  Integer :c70
  Integer :c71
  Integer :c72
  
  check Sequel::SQL::BooleanExpression.new(:>=, Sequel::SQL::Identifier.new(:c64), 0)
  check Sequel::SQL::BooleanExpression.new(:>=, Sequel::SQL::Identifier.new(:c65), 0)
  check Sequel::SQL::BooleanExpression.new(:>=, Sequel::SQL::Identifier.new(:c66), 0)
  check Sequel::SQL::BooleanExpression.new(:>=, Sequel::SQL::Identifier.new(:c67), 0)
  check Sequel::SQL::BooleanExpression.new(:>=, Sequel::SQL::Identifier.new(:c68), 0)
  check Sequel::SQL::BooleanExpression.new(:>=, Sequel::SQL::Identifier.new(:c69), 0)
end
END_MIG
  end

  it "should convert mysql types to ruby types" do
    types = ['double(15,2)', 'double(7,1) unsigned']
    @d.meta_def(:schema) do |t, *o|
      i = 0
      types.map{|x| [:"c#{i+=1}", {:db_type=>x, :allow_null=>true}]}
    end
    @d.dump_table_schema(:x).should == (<<END_MIG).chomp
create_table(:x) do
  Float :c1
  Float :c2
  
  check Sequel::SQL::BooleanExpression.new(:>=, Sequel::SQL::Identifier.new(:c2), 0)
end
END_MIG
  end

  it "should force specify :null option for MySQL timestamp columns when using :same_db" do
    @d.meta_def(:database_type){:mysql}
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'timestamp', :primary_key=>true, :allow_null=>true}]]}
    @d.dump_table_schema(:t3, :same_db=>true).should == "create_table(:t3) do\n  column :c1, \"timestamp\", :null=>true\n  \n  primary_key [:c1]\nend"

    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'timestamp', :primary_key=>true, :allow_null=>false}]]}
    @d.dump_table_schema(:t3, :same_db=>true).should == "create_table(:t3) do\n  column :c1, \"timestamp\", :null=>false\n  \n  primary_key [:c1]\nend"
  end

  it "should use separate primary_key call with non autoincrementable types" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'varchar(8)', :primary_key=>true}]]}
    @d.dump_table_schema(:t3).should == "create_table(:t3) do\n  String :c1, :size=>8\n  \n  primary_key [:c1]\nend"
    @d.dump_table_schema(:t3, :same_db=>true).should == "create_table(:t3) do\n  column :c1, \"varchar(8)\"\n  \n  primary_key [:c1]\nend"
  end

  it "should use explicit type for non integer foreign_key types" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'date', :primary_key=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|t, *a| [{:columns=>[:c1], :table=>:t3, :key=>[:c1]}] if t == :t4}
    ["create_table(:t4) do\n  foreign_key :c1, :t3, :type=>Date, :key=>[:c1]\n  \n  primary_key [:c1]\nend",
     "create_table(:t4) do\n  foreign_key :c1, :t3, :key=>[:c1], :type=>Date\n  \n  primary_key [:c1]\nend"].should include(@d.dump_table_schema(:t4))
    ["create_table(:t4) do\n  foreign_key :c1, :t3, :type=>\"date\", :key=>[:c1]\n  \n  primary_key [:c1]\nend",
     "create_table(:t4) do\n  foreign_key :c1, :t3, :key=>[:c1], :type=>\"date\"\n  \n  primary_key [:c1]\nend"].should include(@d.dump_table_schema(:t4, :same_db=>true))
  end

  it "should correctly handing autoincrementing primary keys that are also foreign keys" do
    @d.meta_def(:schema){|*s| [[:c1, {:db_type=>'integer', :primary_key=>true}]]}
    @d.meta_def(:supports_foreign_key_parsing?){true}
    @d.meta_def(:foreign_key_list){|t, *a| [{:columns=>[:c1], :table=>:t3, :key=>[:c1]}] if t == :t4}
    ["create_table(:t4) do\n  primary_key :c1, :table=>:t3, :key=>[:c1]\nend",
     "create_table(:t4) do\n  primary_key :c1, :key=>[:c1], :table=>:t3\nend"].should include(@d.dump_table_schema(:t4))
  end
end
