require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "identifier_mangling extension" do
  after do
    Sequel.quote_identifiers = false
    Sequel.identifier_input_method = nil
    Sequel.identifier_output_method = nil
  end
  
  it "should respect the :quote_identifiers option" do
    db = Sequel::Database.new(:quote_identifiers=>false, :identifier_mangling=>true)
    db.quote_identifiers?.must_equal false
    db = Sequel::Database.new(:quote_identifiers=>true, :identifier_mangling=>true)
    db.quote_identifiers?.must_equal true
  end

  it "should respect the :quote_identifiers setting" do
    db = Sequel::Database.new(:identifier_mangling=>true)
    db.quote_identifiers?.must_equal false
    db.quote_identifiers = true
    db.quote_identifiers?.must_equal true
  end

  it "should upcase on input and downcase on output by default" do
    db = Sequel::Database.new(:identifier_mangling=>true)
    db.send(:identifier_input_method_default).must_equal :upcase
    db.send(:identifier_output_method_default).must_equal :downcase
  end

  it "should respect the :identifier_input_method option" do
    Sequel.identifier_input_method = nil
    Sequel::Database.identifier_input_method.must_equal false
    db = Sequel::Database.new(:identifier_input_method=>nil, :identifier_mangling=>true)
    db.identifier_input_method.must_be_nil
    db.identifier_input_method = :downcase
    db.identifier_input_method.must_equal :downcase
    db = Sequel::Database.new(:identifier_input_method=>:upcase, :identifier_mangling=>true)
    db.identifier_input_method.must_equal :upcase
    db.identifier_input_method = nil
    db.identifier_input_method.must_be_nil
    Sequel.identifier_input_method = :downcase
    Sequel::Database.identifier_input_method.must_equal :downcase
    db = Sequel::Database.new(:identifier_input_method=>nil, :identifier_mangling=>true)
    db.identifier_input_method.must_be_nil
    db.identifier_input_method = :upcase
    db.identifier_input_method.must_equal :upcase
    db = Sequel::Database.new(:identifier_input_method=>:upcase, :identifier_mangling=>true)
    db.identifier_input_method.must_equal :upcase
    db.identifier_input_method = nil
    db.identifier_input_method.must_be_nil
  end
  
  it "should respect the :identifier_output_method option" do
    Sequel.identifier_output_method = nil
    Sequel::Database.identifier_output_method.must_equal false
    db = Sequel::Database.new(:identifier_output_method=>nil, :identifier_mangling=>true)
    db.identifier_output_method.must_be_nil
    db.identifier_output_method = :downcase
    db.identifier_output_method.must_equal :downcase
    db = Sequel::Database.new(:identifier_output_method=>:upcase, :identifier_mangling=>true)
    db.identifier_output_method.must_equal :upcase
    db.identifier_output_method = nil
    db.identifier_output_method.must_be_nil
    Sequel.identifier_output_method = :downcase
    Sequel::Database.identifier_output_method.must_equal :downcase
    db = Sequel::Database.new(:identifier_output_method=>nil, :identifier_mangling=>true)
    db.identifier_output_method.must_be_nil
    db.identifier_output_method = :upcase
    db.identifier_output_method.must_equal :upcase
    db = Sequel::Database.new(:identifier_output_method=>:upcase, :identifier_mangling=>true)
    db.identifier_output_method.must_equal :upcase
    db.identifier_output_method = nil
    db.identifier_output_method.must_be_nil
  end

  it "should use the default Sequel.quote_identifiers value" do
    Sequel.quote_identifiers = true
    Sequel::Database.new(:identifier_mangling=>true).quote_identifiers?.must_equal true
    Sequel.quote_identifiers = false
    Sequel::Database.new(:identifier_mangling=>true).quote_identifiers?.must_equal false
    Sequel::Database.quote_identifiers = true
    Sequel::Database.new(:identifier_mangling=>true).quote_identifiers?.must_equal true
    Sequel::Database.quote_identifiers = false
    Sequel::Database.new(:identifier_mangling=>true).quote_identifiers?.must_equal false
  end

  it "should use the default Sequel.identifier_input_method value" do
    Sequel.identifier_input_method = :downcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_input_method.must_equal :downcase
    Sequel.identifier_input_method = :upcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_input_method.must_equal :upcase
    Sequel::Database.identifier_input_method = :downcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_input_method.must_equal :downcase
    Sequel::Database.identifier_input_method = :upcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_input_method.must_equal :upcase
  end
  
  it "should use the default Sequel.identifier_output_method value" do
    Sequel.identifier_output_method = :downcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_output_method.must_equal :downcase
    Sequel.identifier_output_method = :upcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_output_method.must_equal :upcase
    Sequel::Database.identifier_output_method = :downcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_output_method.must_equal :downcase
    Sequel::Database.identifier_output_method = :upcase
    Sequel::Database.new(:identifier_mangling=>true).identifier_output_method.must_equal :upcase
  end

  it "should respect the quote_indentifiers_default method if Sequel.quote_identifiers = nil" do
    Sequel.quote_identifiers = nil
    Sequel::Database.new(:identifier_mangling=>true).quote_identifiers?.must_equal true
    x = Class.new(Sequel::Database){def quote_identifiers_default; false end}
    x.new(:identifier_mangling=>true).quote_identifiers?.must_equal false
    y = Class.new(Sequel::Database){def quote_identifiers_default; true end}
    y.new(:identifier_mangling=>true).quote_identifiers?.must_equal true
  end
  
  it "should respect the identifier_input_method_default method if Sequel.identifier_input_method is not called" do
    class Sequel::Database
      @identifier_input_method = nil
    end
    x = Class.new(Sequel::Database){def identifier_input_method_default; :downcase end}
    x.new(:identifier_mangling=>true).identifier_input_method.must_equal :downcase
    y = Class.new(Sequel::Database){def identifier_input_method_default; :camelize end}
    y.new(:identifier_mangling=>true).identifier_input_method.must_equal :camelize
  end
  
  it "should respect the identifier_output_method_default method if Sequel.identifier_output_method is not called" do
    class Sequel::Database
      @identifier_output_method = nil
    end
    x = Class.new(Sequel::Database){def identifier_output_method_default; :upcase end}
    x.new(:identifier_mangling=>true).identifier_output_method.must_equal :upcase
    y = Class.new(Sequel::Database){def identifier_output_method_default; :underscore end}
    y.new(:identifier_mangling=>true).identifier_output_method.must_equal :underscore
  end
end

describe "Database#input_identifier_meth" do
  it "should be the input_identifer method of a default dataset for this database" do
    db = Sequel::Database.new(:identifier_mangling=>true)
    db.send(:input_identifier_meth).call(:a).must_equal 'a'
    db.identifier_input_method = :upcase
    db.send(:input_identifier_meth).call(:a).must_equal 'A'
  end
end

describe "Database#output_identifier_meth" do
  it "should be the output_identifer method of a default dataset for this database" do
    db = Sequel::Database.new(:identifier_mangling=>true)
    db.send(:output_identifier_meth).call('A').must_equal :A
    db.identifier_output_method = :downcase
    db.send(:output_identifier_meth).call('A').must_equal :a
  end
end

describe "Database#metadata_dataset" do
  it "should be a dataset with the default settings for identifier_mangling" do
    ds = Sequel::Database.new(:identifier_mangling=>true).send(:metadata_dataset)
    ds.literal(:a).must_equal 'A'
    ds.send(:output_identifier, 'A').must_equal :a
  end
end

describe "Dataset" do
  before do
    @dataset = Sequel.mock(:identifier_mangling=>true).dataset
  end
  
  it "should get quote_identifiers default from database" do
    db = Sequel::Database.new(:quote_identifiers=>true, :identifier_mangling=>true)
    db[:a].quote_identifiers?.must_equal true
    db = Sequel::Database.new(:quote_identifiers=>false, :identifier_mangling=>true)
    db[:a].quote_identifiers?.must_equal false
  end

  it "should get identifier_input_method default from database" do
    db = Sequel::Database.new(:identifier_input_method=>:upcase, :identifier_mangling=>true)
    db[:a].identifier_input_method.must_equal :upcase
    db = Sequel::Database.new(:identifier_input_method=>:downcase, :identifier_mangling=>true)
    db[:a].identifier_input_method.must_equal :downcase
  end

  it "should get identifier_output_method default from database" do
    db = Sequel::Database.new(:identifier_output_method=>:upcase, :identifier_mangling=>true)
    db[:a].identifier_output_method.must_equal :upcase
    db = Sequel::Database.new(:identifier_output_method=>:downcase, :identifier_mangling=>true)
    db[:a].identifier_output_method.must_equal :downcase
  end
  
  # SEQUEL5: Remove
  unless Sequel.mock(:identifier_mangling=>true).dataset.frozen?
    it "should have quote_identifiers= method which changes literalization of identifiers" do
      @dataset.quote_identifiers = true
      @dataset.literal(:a).must_equal '"a"'
      @dataset.quote_identifiers = false
      @dataset.literal(:a).must_equal 'a'
    end
    
    it "should have identifier_input_method= method which changes literalization of identifiers" do
      @dataset.identifier_input_method = :upcase
      @dataset.literal(:a).must_equal 'A'
      @dataset.identifier_input_method = :downcase
      @dataset.literal(:A).must_equal 'a'
      @dataset.identifier_input_method = :reverse
      @dataset.literal(:at_b).must_equal 'b_ta'
    end
    
    it "should have identifier_output_method= method which changes identifiers returned from the database" do
      @dataset.send(:output_identifier, "at_b_C").must_equal :at_b_C
      @dataset.identifier_output_method = :upcase
      @dataset.send(:output_identifier, "at_b_C").must_equal :AT_B_C
      @dataset.identifier_output_method = :downcase
      @dataset.send(:output_identifier, "at_b_C").must_equal :at_b_c
      @dataset.identifier_output_method = :reverse
      @dataset.send(:output_identifier, "at_b_C").must_equal :C_b_ta
    end
  end
  
  it "should have with_quote_identifiers method which returns cloned dataset with changed literalization of identifiers" do
    @dataset.with_quote_identifiers(true).literal(:a).must_equal '"a"'
    @dataset.with_quote_identifiers(false).literal(:a).must_equal 'a'
    ds = @dataset.freeze.with_quote_identifiers(false)
    ds.literal(:a).must_equal 'a'
    ds.frozen?.must_equal true
  end
  
  it "should have with_identifier_input_method method which returns cloned dataset with changed literalization of identifiers" do
    @dataset.with_identifier_input_method(:upcase).literal(:a).must_equal 'A'
    @dataset.with_identifier_input_method(:downcase).literal(:A).must_equal 'a'
    @dataset.with_identifier_input_method(:reverse).literal(:at_b).must_equal 'b_ta'
    ds = @dataset.freeze.with_identifier_input_method(:reverse)
    ds.frozen?.must_equal true
    ds.literal(:at_b).must_equal 'b_ta'
  end
  
  it "should have with_identifier_output_method method which returns cloned dataset with changed identifiers returned from the database" do
    @dataset.send(:output_identifier, "at_b_C").must_equal :at_b_C
    @dataset.with_identifier_output_method(:upcase).send(:output_identifier, "at_b_C").must_equal :AT_B_C
    @dataset.with_identifier_output_method(:downcase).send(:output_identifier, "at_b_C").must_equal :at_b_c
    @dataset.with_identifier_output_method(:reverse).send(:output_identifier, "at_b_C").must_equal :C_b_ta
    ds = @dataset.freeze.with_identifier_output_method(:reverse)
    ds.send(:output_identifier, "at_b_C").must_equal :C_b_ta
    ds.frozen?.must_equal true
  end
  
  it "should have output_identifier handle empty identifiers" do
    @dataset.send(:output_identifier, "").must_equal :untitled
    @dataset.with_identifier_output_method(:upcase).send(:output_identifier, "").must_equal :UNTITLED
    @dataset.with_identifier_output_method(:downcase).send(:output_identifier, "").must_equal :untitled
    @dataset.with_identifier_output_method(:reverse).send(:output_identifier, "").must_equal :deltitnu
  end
end

describe "Frozen Datasets" do
  before do
    @ds = Sequel.mock(:identifier_mangling=>true)[:test].freeze
  end

  it "should raise an error when calling mutation methods" do
    proc{@ds.identifier_input_method = :a}.must_raise RuntimeError
    proc{@ds.identifier_output_method = :a}.must_raise RuntimeError
    proc{@ds.quote_identifiers = false}.must_raise RuntimeError
  end
end

describe "identifier_mangling extension" do
  it "should be able to load dialects based on the database name" do
    begin
      qi = class Sequel::Database; @quote_identifiers; end
      ii = class Sequel::Database; @identifier_input_method; end
      io = class Sequel::Database; @identifier_output_method; end
      Sequel.quote_identifiers = nil
      class Sequel::Database; @identifier_input_method=nil; end
      class Sequel::Database; @identifier_output_method=nil; end
      Sequel.mock(:host=>'access').select(Date.new(2011, 12, 13)).sql.must_equal 'SELECT #2011-12-13#'
      Sequel.mock(:host=>'cubrid').from(:a).offset(1).sql.must_equal 'SELECT * FROM "a" LIMIT 1,4294967295'
      Sequel.mock(:host=>'db2').select(1).sql.must_equal 'SELECT 1 FROM "SYSIBM"."SYSDUMMY1"'
      Sequel.mock(:host=>'firebird')[:a].distinct.limit(1, 2).sql.must_equal 'SELECT DISTINCT FIRST 1 SKIP 2 * FROM "A"'
      Sequel.mock(:host=>'informix')[:a].distinct.limit(1, 2).sql.must_equal 'SELECT SKIP 2 FIRST 1 DISTINCT * FROM A'
      Sequel.mock(:host=>'mssql')[:a].full_text_search(:b, 'c').sql.must_equal "SELECT * FROM [A] WHERE (CONTAINS ([B], 'c'))"
      Sequel.mock(:host=>'mysql')[:a].full_text_search(:b, 'c').sql.must_equal "SELECT * FROM `a` WHERE (MATCH (`b`) AGAINST ('c'))"
      Sequel.mock(:host=>'oracle')[:a].limit(1).sql.must_equal 'SELECT * FROM (SELECT * FROM "A") "T1" WHERE (ROWNUM <= 1)'
      Sequel.mock(:host=>'postgres')[:a].full_text_search(:b, 'c').sql.must_equal "SELECT * FROM \"a\" WHERE (to_tsvector(CAST('simple' AS regconfig), (COALESCE(\"b\", ''))) @@ to_tsquery(CAST('simple' AS regconfig), 'c'))"
      Sequel.mock(:host=>'sqlanywhere').from(:a).offset(1).sql.must_equal 'SELECT TOP 2147483647 START AT (1 + 1) * FROM "A"'
      Sequel.mock(:host=>'sqlite')[:a___b].sql.must_equal "SELECT * FROM `a` AS 'b'"
    ensure
      Sequel.quote_identifiers = qi
      Sequel::Database.send(:instance_variable_set, :@identifier_input_method, ii)
      Sequel::Database.send(:instance_variable_set, :@identifier_output_method, io)
    end
  end
end

describe Sequel::Model, ".[] optimization" do
  before do
    @db = Sequel.mock(:identifier_mangling=>true, :quote_identifiers=>true)
    def @db.schema(*) [[:id, {:primary_key=>true}]] end
    def @db.supports_schema_parsing?() true end
    @c = Class.new(Sequel::Model(@db))
    @ds = @db.dataset.with_quote_identifiers(true)
  end

  it "should have simple_pk and simple_table respect dataset's identifier input methods" do
    ds = @db.from(:ab).with_identifier_input_method(:reverse)
    @c.set_dataset ds
    @c.simple_table.must_equal '"ba"'
    @c.set_primary_key :cd
    @c.simple_pk.must_equal '"dc"'

    @c.set_dataset ds.from(:ef__gh)
    @c.simple_table.must_equal '"fe"."hg"'
  end
end
