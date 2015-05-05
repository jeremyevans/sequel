SEQUEL_ADAPTER_TEST = :sqlanywhere

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

if DB.table_exists?(:test)
  DB.drop_table(:test)
end

describe "Convert smallint to boolean" do
  before do
    @db = DB
  end
  after do
    Sequel::SqlAnywhere.convert_smallint_to_bool = true
    @db.convert_smallint_to_bool = true
  end
  
  describe "Sequel::SqlAnywhere.convert_smallint_to_bool" do
    before do
      @db.create_table!(:booltest){column :b, 'smallint'; column :i, 'integer'}
      @ds = @db[:booltest]
    end
    after do
      @db.drop_table(:booltest)
    end

    it "should consider smallint datatypes as boolean if set, but if not, as larger smallints" do
      @db.create_table!(:booltest){column :b, 'smallint'; column :i, 'integer'}
      @db.schema(:booltest, :reload=>true).first.last[:type].must_equal :boolean
      @db.schema(:booltest, :reload=>true).first.last[:db_type].must_match /smallint/i

      Sequel::SqlAnywhere.convert_smallint_to_bool = false
      @db2 = Sequel.connect(DB.url)
      @db2.schema(:booltest, :reload=>true).first.last[:type].must_equal :integer
      @db2.schema(:booltest, :reload=>true).first.last[:db_type].must_match /smallint/i

      @db.schema(:booltest, :reload=>true).first.last[:type].must_equal :boolean
      @db.schema(:booltest, :reload=>true).first.last[:db_type].must_match /smallint/i

      @db2.disconnect
    end

    describe "datasets" do
      it "should return smallints as bools and integers as integers when set" do
        @ds.delete
        @ds << {:b=>true, :i=>10}
        @ds.all.must_equal [{:b=>true, :i=>10}]
        @ds.delete
        @ds << {:b=>false, :i=>0}
        @ds.all.must_equal [{:b=>false, :i=>0}]
        @ds.delete
        @ds << {:b=>true, :i=>1}
        @ds.all.must_equal [{:b=>true, :i=>1}]
      end

      it "should return all smallints as integers when unset" do
        Sequel::SqlAnywhere.convert_smallint_to_bool = false
        @db2 = Sequel.connect(DB.url)
        @ds2 = @db2[:booltest]
        @ds2.delete
        @ds2 << {:b=>true, :i=>10}
        @ds2.all.must_equal [{:b=>1, :i=>10}]
        @ds2.delete
        @ds2 << {:b=>false, :i=>0}
        @ds2.all.must_equal [{:b=>0, :i=>0}]
        
        @ds2.delete
        @ds2 << {:b=>1, :i=>10}
        @ds2.all.must_equal [{:b=>1, :i=>10}]
        @ds2.delete
        @ds2 << {:b=>0, :i=>0}
        @ds2.all.must_equal [{:b=>0, :i=>0}]

        @db2.disconnect
      end
    end
  end
  
  describe "Database#convert_smallint_to_bool" do
    before do
      @db.create_table!(:booltest){column :b, 'smallint'; column :i, 'integer'}
    end
    after do
      @db.drop_table(:booltest)
    end
  
    it "should consider smallint datatypes as boolean if set, but not larger smallints" do
      @db.schema(:booltest, :reload=>true).first.last[:type].must_equal :boolean
      @db.schema(:booltest, :reload=>true).first.last[:db_type].must_match /smallint/i
      @db.convert_smallint_to_bool = false
      @db.schema(:booltest, :reload=>true).first.last[:type].must_equal :integer
      @db.schema(:booltest, :reload=>true).first.last[:db_type].must_match /smallint/i
    end
  
    describe "datasets" do
      it "should return smallints as bools and integers as integers when set" do
        @ds = @db[:booltest]
        @ds.delete
        @ds << {:b=>true, :i=>10}
        @ds.all.must_equal [{:b=>true, :i=>10}]
        @ds.delete
        @ds << {:b=>false, :i=>0}
        @ds.all.must_equal [{:b=>false, :i=>0}]
        @ds.delete
        @ds << {:b=>true, :i=>1}
        @ds.all.must_equal [{:b=>true, :i=>1}]
      end
  
      it "should return all smallints as integers when unset" do
        @db2 = Sequel.connect(DB.url)
        @db2.convert_smallint_to_bool = false
        @ds2 = @db2[:booltest]
        @ds2.delete
        @ds2 << {:b=>true, :i=>10}
        @ds2.all.must_equal [{:b=>1, :i=>10}]
        @ds2.delete
        @ds2 << {:b=>false, :i=>0}
        @ds2.all.must_equal [{:b=>0, :i=>0}]
      
        @ds2.delete
        @ds2 << {:b=>1, :i=>10}
        @ds2.all.must_equal [{:b=>1, :i=>10}]
        @ds2.delete
        @ds2 << {:b=>0, :i=>0}
        @ds2.all.must_equal [{:b=>0, :i=>0}]

        @db2.disconnect
      end
    end
  end

  describe "Dataset#convert_smallint_to_bool" do
    before do
      @db.create_table!(:booltest){column :b, 'smallint'; column :i, 'integer'}
      @ds = @db[:booltest]
    end
    after do
      @db.drop_table(:booltest)
    end
    
    it "should return smallints as bools and integers as integers when set" do
      @ds.delete
      @ds << {:b=>true, :i=>10}
      @ds.all.must_equal [{:b=>true, :i=>10}]
      @ds.delete
      @ds << {:b=>false, :i=>0}
      @ds.all.must_equal [{:b=>false, :i=>0}]
      @ds.delete
      @ds << {:b=>true, :i=>1}
      @ds.all.must_equal [{:b=>true, :i=>1}]
    end

    it "should return all smallints as integers when unset" do
      @ds.convert_smallint_to_bool = false
      @ds.delete
      @ds << {:b=>true, :i=>10}
      @ds.all.must_equal [{:b=>1, :i=>10}]
      @ds.delete
      @ds << {:b=>false, :i=>0}
      @ds.all.must_equal [{:b=>0, :i=>0}]
    
      @ds.delete
      @ds << {:b=>1, :i=>10}
      @ds.all.must_equal [{:b=>1, :i=>10}]
      @ds.delete
      @ds << {:b=>0, :i=>0}
      @ds.all.must_equal [{:b=>0, :i=>0}]
    end
  end
end
