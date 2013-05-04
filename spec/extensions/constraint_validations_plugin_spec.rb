require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::ConstraintValidations" do
  def model_class(opts={})
    return @c if @c
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.columns :name
    @db.sqls
    set_fetch(opts)
    @c.plugin :constraint_validations
    @c
  end

  def set_fetch(opts)
    @db.fetch = {:table=>'items', :message=>nil, :allow_nil=>nil, :constraint_name=>nil, :validation_type=>'presence', :argument=>nil, :column=>'name'}.merge(opts)
  end

  before do
    @db = Sequel.mock
    set_fetch({})
    @ds = @db[:items]
    @ds.instance_variable_set(:@columns, [:name])
    @ds2 = Sequel.mock[:items2]
    @ds2.instance_variable_set(:@columns, [:name])
  end

  it "should load the validation_helpers plugin into the class" do
    model_class.new.should respond_to(:validates_presence)
  end

  it "should parse constraint validations when loading plugin" do
    @c = model_class
    @db.sqls.should == ["SELECT * FROM sequel_constraint_validations"]
    @db.constraint_validations.should == {'items'=>[[:validates_presence, :name]]}
    @c.constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should parse constraint validations with a custom constraint validations table" do
    c = Class.new(Sequel::Model(@db[:items]))
    @db.sqls
    c.plugin :constraint_validations, :constraint_validations_table=>:foo
    @db.sqls.should == ["SELECT * FROM foo"]
    @db.constraint_validations.should == {'items'=>[[:validates_presence, :name]]}
    c.constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should populate constraint_validations when subclassing" do
    c = Class.new(Sequel::Model(@db))
    c.plugin :constraint_validations
    @db.sqls.should == ["SELECT * FROM sequel_constraint_validations"]
    sc = Class.new(c)
    sc.set_dataset @ds
    @db.sqls.should == []
    sc.constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should populate constraint_validations when changing the model's dataset" do
    c = Class.new(Sequel::Model(@db[:foo]))
    c.columns :name
    @db.sqls
    c.plugin :constraint_validations
    @db.sqls.should == ["SELECT * FROM sequel_constraint_validations"]
    sc = Class.new(c)
    sc.set_dataset @ds
    @db.sqls.should == []
    sc.constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should reparse constraint validations when changing the model's database" do
    c = Class.new(Sequel::Model(@ds2))
    c.plugin :constraint_validations
    @ds2.db.sqls.should == ["SELECT * FROM sequel_constraint_validations"]
    sc = Class.new(c)
    sc.set_dataset @ds
    @db.sqls.should == ["SELECT * FROM sequel_constraint_validations"]
    sc.constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should reparse constraint validations when changing the model's database with a custom constraint validations table" do
    c = Class.new(Sequel::Model(@ds2))
    c.plugin :constraint_validations, :constraint_validations_table=>:foo
    @ds2.db.sqls.should == ["SELECT * FROM foo"]
    sc = Class.new(c)
    sc.set_dataset @ds
    @db.sqls.should == ["SELECT * FROM foo"]
    sc.constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should correctly retrieve :message option from constraint validations table" do
    model_class(:message=>'foo').constraint_validations.should == [[:validates_presence, :name, {:message=>'foo'}]]
  end

  it "should correctly retrieve :allow_nil option from constraint validations table" do
    model_class(:allow_nil=>true).constraint_validations.should == [[:validates_presence, :name, {:allow_nil=>true}]]
  end

  it "should handle presence validation" do
    model_class(:validation_type=>'presence').constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should handle exact_length validation" do
    model_class(:validation_type=>'exact_length', :argument=>'5').constraint_validations.should == [[:validates_exact_length, 5, :name]]
  end

  it "should handle min_length validation" do
    model_class(:validation_type=>'min_length', :argument=>'5').constraint_validations.should == [[:validates_min_length, 5, :name]]
  end

  it "should handle max_length validation" do
    model_class(:validation_type=>'max_length', :argument=>'5').constraint_validations.should == [[:validates_max_length, 5, :name]]
  end

  it "should handle length_range validation" do
    model_class(:validation_type=>'length_range', :argument=>'3..5').constraint_validations.should == [[:validates_length_range, 3..5, :name]]
  end

  it "should handle length_range validation with an exclusive end" do
    model_class(:validation_type=>'length_range', :argument=>'3...5').constraint_validations.should == [[:validates_length_range, 3...5, :name]]
  end

  it "should handle format validation" do
    model_class(:validation_type=>'format', :argument=>'^foo.*').constraint_validations.should == [[:validates_format, /^foo.*/, :name]]
  end

  it "should handle format validation with case insensitive format" do
    model_class(:validation_type=>'iformat', :argument=>'^foo.*').constraint_validations.should == [[:validates_format, /^foo.*/i, :name]]
  end

  it "should handle includes validation with array of strings" do
    model_class(:validation_type=>'includes_str_array', :argument=>'a,b,c').constraint_validations.should == [[:validates_includes, %w'a b c', :name]]
  end

  it "should handle includes validation with array of integers" do
    model_class(:validation_type=>'includes_int_array', :argument=>'1,2,3').constraint_validations.should == [[:validates_includes, [1, 2, 3], :name]]
  end

  it "should handle includes validation with inclusive range of integers" do
    model_class(:validation_type=>'includes_int_range', :argument=>'3..5').constraint_validations.should == [[:validates_includes, 3..5, :name]]
  end

  it "should handle includes validation with exclusive range of integers" do
    model_class(:validation_type=>'includes_int_range', :argument=>'3...5').constraint_validations.should == [[:validates_includes, 3...5, :name]]
  end

  it "should handle like validation" do
    model_class(:validation_type=>'like', :argument=>'foo').constraint_validations.should == [[:validates_format, /\Afoo\z/, :name]]
  end

  it "should handle ilike validation" do
    model_class(:validation_type=>'ilike', :argument=>'foo').constraint_validations.should == [[:validates_format, /\Afoo\z/i, :name]]
  end

  it "should handle like validation with % metacharacter" do
    model_class(:validation_type=>'like', :argument=>'%foo%').constraint_validations.should == [[:validates_format, /\A.*foo.*\z/, :name]]
  end

  it "should handle like validation with %% metacharacter" do
    model_class(:validation_type=>'like', :argument=>'%%foo%%').constraint_validations.should == [[:validates_format, /\A%foo%\z/, :name]]
  end

  it "should handle like validation with _ metacharacter" do
    model_class(:validation_type=>'like', :argument=>'f_o').constraint_validations.should == [[:validates_format, /\Af.o\z/, :name]]
  end

  it "should handle like validation with Regexp metacharacter" do
    model_class(:validation_type=>'like', :argument=>'\wfoo\d').constraint_validations.should == [[:validates_format, /\A\\wfoo\\d\z/, :name]]
  end

  it "should handle unique validation" do
    model_class(:validation_type=>'unique').constraint_validations.should == [[:validates_unique, [:name]]]
  end

  it "should handle unique validation with multiple columns" do
    model_class(:validation_type=>'unique', :column=>'name,id').constraint_validations.should == [[:validates_unique, [:name, :id]]]
  end

  it "should used parsed constraint validations when validating" do
    o = model_class.new
    o.valid?.should == false
    o.errors.full_messages.should == ['name is not present']
  end

  it "should handle a table name specified as SQL::Identifier" do
    set_fetch(:table=>'sch__items')
    c = Class.new(Sequel::Model(@db[Sequel.identifier(:sch__items)]))
    c.plugin :constraint_validations
    c.constraint_validations.should == [[:validates_presence, :name]]
  end

  it "should handle a table name specified as SQL::QualifiedIdentifier" do
    set_fetch(:table=>'sch.items')
    c = Class.new(Sequel::Model(@db[Sequel.qualify(:sch, :items)]))
    c.plugin :constraint_validations
    c.constraint_validations.should == [[:validates_presence, :name]]
  end
end
