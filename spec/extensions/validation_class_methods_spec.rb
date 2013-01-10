require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

model_class = proc do |klass, &block|
  c = Class.new(klass)
  c.plugin :validation_class_methods
  c.class_eval(&block) if block
  c
end

describe Sequel::Model do
  before do
    @c = model_class.call Sequel::Model do
      def self.validates_coolness_of(attr)
        validates_each(attr) {|o, a, v| o.errors[a] << 'is not cool' if v != :cool}
      end
    end
  end
  
  specify "should respond to validations, has_validations?, and validation_reflections" do
    @c.should respond_to(:validations)
    @c.should respond_to(:has_validations?)
    @c.should respond_to(:validation_reflections)
  end
  
  specify "should be able to reflect on validations" do
    @c.validation_reflections.should == {}
    @c.validates_acceptance_of(:a)
    @c.validation_reflections.should == {:a=>[[:acceptance, {:tag=>:acceptance, :message=>"is not accepted", :allow_nil=>true, :accept=>"1"}]]}
    @c.validates_presence_of(:a)
    @c.validation_reflections[:a].length.should == 2
    @c.validation_reflections[:a].last.should == [:presence, {:tag=>:presence, :message=>"is not present"}]
  end

  specify "should handle validation reflections correctly when subclassing" do
    @c.validates_acceptance_of(:a)
    c = Class.new(@c)
    c.validation_reflections.map{|k,v| k}.should == [:a]
    c.validates_presence_of(:a)
    @c.validation_reflections.should == {:a=>[[:acceptance, {:tag=>:acceptance, :message=>"is not accepted", :allow_nil=>true, :accept=>"1"}]]}
    c.validation_reflections[:a].last.should == [:presence, {:tag=>:presence, :message=>"is not present"}]
  end

  specify "should acccept validation definitions using validates_each" do
    @c.validates_each(:xx, :yy) {|o, a, v| o.errors[a] << 'too low' if v < 50}
    o = @c.new
    o.should_receive(:xx).once.and_return(40)
    o.should_receive(:yy).once.and_return(60)
    o.valid?.should == false
    o.errors.full_messages.should == ['xx too low']
  end

  specify "should return true/false for has_validations?" do
    @c.has_validations?.should == false
    @c.validates_each(:xx) {1}
    @c.has_validations?.should == true
  end
  
  specify "should validate multiple attributes at once" do
    o = @c.new
    def o.xx
      1
    end
    def o.yy
      2
    end
    vals = nil
    atts = nil
    @c.validates_each([:xx, :yy]){|obj,a,v| atts=a; vals=v}
    o.valid?
    vals.should == [1,2]
    atts.should == [:xx, :yy]
  end
  
  specify "should respect allow_missing option when using multiple attributes" do
    o = @c.new
    def o.xx
      self[:xx]
    end
    def o.yy
      self[:yy]
    end
    vals = nil
    atts = nil
    @c.validates_each([:xx, :yy], :allow_missing=>true){|obj,a,v| atts=a; vals=v}

    o.values[:xx] = 1
    o.valid?
    vals.should == [1,nil]
    atts.should == [:xx, :yy]

    vals = nil
    atts = nil
    o.values.clear
    o.values[:yy] = 2
    o.valid?
    vals.should == [nil, 2]
    atts.should == [:xx, :yy]

    vals = nil
    atts = nil
    o.values.clear
    o.valid?.should == true
    vals.should == nil
    atts.should == nil
  end
  
  specify "should overwrite existing validation with the same tag and attribute" do
    @c.validates_each(:xx, :xx, :tag=>:low) {|o, a, v| o.xxx; o.errors[a] << 'too low' if v < 50}
    @c.validates_each(:yy, :yy) {|o, a, v| o.yyy; o.errors[a] << 'too low' if v < 50}
    @c.validates_presence_of(:zz, :zz)
    @c.validates_length_of(:aa, :aa, :tag=>:blah)
    o = @c.new
    def o.zz
      @a ||= 0
      @a += 1
    end
    def o.aa
      @b ||= 0
      @b += 1
    end
    o.should_receive(:xx).once.and_return(40)
    o.should_receive(:yy).once.and_return(60)
    o.should_receive(:xxx).once
    o.should_receive(:yyy).twice
    o.valid?.should == false
    o.zz.should == 2
    o.aa.should == 2
    o.errors.full_messages.should == ['xx too low']
  end

  specify "should provide a validates method that takes block with validation definitions" do
    @c.validates do
      coolness_of :blah
    end
    @c.validations[:blah].should_not be_empty
    o = @c.new
    o.should_receive(:blah).once.and_return(nil)
    o.valid?.should == false
    o.errors.full_messages.should == ['blah is not cool']
  end

  specify "should have the validates block have appropriate respond_to?" do
    c = nil
    @c.validates{c = respond_to?(:foo)}
    c.should be_false
    @c.validates{c = respond_to?(:length_of)}
    c.should be_true
  end if RUBY_VERSION >= '1.9'
end

describe Sequel::Model do
  before do
    @c = model_class.call Sequel::Model do
      columns :score
      validates_each :score do |o, a, v|
        o.errors[a] << 'too low' if v < 87
      end
    end
    
    @o = @c.new
  end
  
  specify "should supply a #valid? method that returns true if validations pass" do
    @o.score = 50
    @o.should_not be_valid
    @o.score = 100
    @o.should be_valid
  end
  
  specify "should provide an errors object" do
    @o.score = 100
    @o.should be_valid
    @o.errors.should be_empty
    
    @o.score = 86
    @o.should_not be_valid
    @o.errors[:score].should == ['too low']
    @o.errors[:blah].should be_empty
  end
end

describe "Sequel::Plugins::ValidationClassMethods::ClassMethods::Generator" do
  before do
    $testit = nil
    
    @c = model_class.call Sequel::Model do
      def self.validates_blah
        $testit = 1324
      end
    end
  end
  
  specify "should instance_eval the block, sending everything to its receiver" do
    @c.validates do
      blah
    end
    $testit.should == 1324
  end
end

describe Sequel::Model do
  before do
    @c = model_class.call Sequel::Model do
      columns :value
      
      def self.filter(*args)
        o = Object.new
        def o.count; 2; end
        o
      end

      def skip; false; end
      def dont_skip; true; end
    end
    @m = @c.new
  end

  specify "should validate acceptance_of" do
    @c.validates_acceptance_of :value
    @m.should be_valid
    @m.value = '1'
    @m.should be_valid
  end
  
  specify "should validate acceptance_of with accept" do
    @c.validates_acceptance_of :value, :accept => 'true'
    @m.value = '1'
    @m.should_not be_valid
    @m.value = 'true'
    @m.should be_valid
  end
  
  specify "should validate acceptance_of with allow_nil => false" do
    @c.validates_acceptance_of :value, :allow_nil => false
    @m.should_not be_valid
  end

  specify "should validate acceptance_of with allow_missing => true" do
    @c.validates_acceptance_of :value, :allow_missing => true
    @m.should be_valid
  end

  specify "should validate acceptance_of with allow_missing => true and allow_nil => false" do
    @c.validates_acceptance_of :value, :allow_missing => true, :allow_nil => false
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
  end

  specify "should validate acceptance_of with if => true" do
    @c.validates_acceptance_of :value, :if => :dont_skip
    @m.value = '0'
    @m.should_not be_valid
  end

  specify "should validate acceptance_of with if => false" do
    @c.validates_acceptance_of :value, :if => :skip
    @m.value = '0'
    @m.should be_valid
  end

  specify "should validate acceptance_of with if proc that evaluates to true" do
    @c.validates_acceptance_of :value, :if => proc{true}
    @m.value = '0'
    @m.should_not be_valid
  end

  specify "should validate acceptance_of with if proc that evaluates to false" do
    @c.validates_acceptance_of :value, :if => proc{false}
    @m.value = '0'
    @m.should be_valid
  end

  specify "should raise an error if :if option is not a Symbol, Proc, or nil" do
    @c.validates_acceptance_of :value, :if => 1
    @m.value = '0'
    proc{@m.valid?}.should raise_error(Sequel::Error)
  end

  specify "should validate confirmation_of" do
    @c.send(:attr_accessor, :value_confirmation)
    @c.validates_confirmation_of :value
    
    @m.value = 'blah'
    @m.should_not be_valid
    
    @m.value_confirmation = 'blah'
    @m.should be_valid
  end
  
  specify "should validate confirmation_of with if => true" do
    @c.send(:attr_accessor, :value_confirmation)
    @c.validates_confirmation_of :value, :if => :dont_skip

    @m.value = 'blah'
    @m.should_not be_valid
  end

  specify "should validate confirmation_of with if => false" do
    @c.send(:attr_accessor, :value_confirmation)
    @c.validates_confirmation_of :value, :if => :skip

    @m.value = 'blah'
    @m.should be_valid
  end

  specify "should validate confirmation_of with allow_missing => true" do
    @c.send(:attr_accessor, :value_confirmation)
    @c.validates_acceptance_of :value, :allow_missing => true
    @m.should be_valid
    @m.value_confirmation = 'blah'
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
  end

  specify "should validate format_of" do
    @c.validates_format_of :value, :with => /.+_.+/
    @m.value = 'abc_'
    @m.should_not be_valid
    @m.value = 'abc_def'
    @m.should be_valid
  end
  
  specify "should raise for validate_format_of without regexp" do
    proc {@c.validates_format_of :value}.should raise_error(ArgumentError)
    proc {@c.validates_format_of :value, :with => :blah}.should raise_error(ArgumentError)
  end
  
  specify "should validate format_of with if => true" do
    @c.validates_format_of :value, :with => /_/, :if => :dont_skip

    @m.value = 'a'
    @m.should_not be_valid
  end

  specify "should validate format_of with if => false" do
    @c.validates_format_of :value, :with => /_/, :if => :skip

    @m.value = 'a'
    @m.should be_valid
  end
  
  specify "should validate format_of with allow_missing => true" do
    @c.validates_format_of :value, :allow_missing => true, :with=>/./
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
  end

  specify "should validate length_of with maximum" do
    @c.validates_length_of :value, :maximum => 5
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '123456'
    @m.should_not be_valid
    @m.errors[:value].should == ['is too long']
    @m.value = nil
    @m.should_not be_valid
    @m.errors[:value].should == ['is not present']
  end

  specify "should validate length_of with maximum using customized error messages" do
    @c.validates_length_of :value, :maximum => 5, :too_long=>'tl', :nil_message=>'np'
    @m.value = '123456'
    @m.should_not be_valid
    @m.errors[:value].should == ['tl']
    @m.value = nil
    @m.should_not be_valid
    @m.errors[:value].should == ['np']
  end

  specify "should validate length_of with minimum" do
    @c.validates_length_of :value, :minimum => 5
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '1234'
    @m.should_not be_valid
  end

  specify "should validate length_of with within" do
    @c.validates_length_of :value, :within => 2..5
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '1'
    @m.should_not be_valid
    @m.value = '123456'
    @m.should_not be_valid
  end

  specify "should validate length_of with is" do
    @c.validates_length_of :value, :is => 3
    @m.should_not be_valid
    @m.value = '123'
    @m.should be_valid
    @m.value = '12'
    @m.should_not be_valid
    @m.value = '1234'
    @m.should_not be_valid
  end
  
  specify "should validate length_of with allow_nil" do
    @c.validates_length_of :value, :is => 3, :allow_nil => true
    @m.should be_valid
  end

  specify "should validate length_of with if => true" do
    @c.validates_length_of :value, :is => 3, :if => :dont_skip

    @m.value = 'a'
    @m.should_not be_valid
  end

  specify "should validate length_of with if => false" do
    @c.validates_length_of :value, :is => 3, :if => :skip

    @m.value = 'a'
    @m.should be_valid
  end

  specify "should validate length_of with allow_missing => true" do
    @c.validates_length_of :value, :allow_missing => true, :minimum => 5
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
  end

  specify "should allow multiple calls to validates_length_of with different options without overwriting" do
    @c.validates_length_of :value, :maximum => 5
    @c.validates_length_of :value, :minimum => 5
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '123456'
    @m.should_not be_valid
    @m.value = '12345'
    @m.should be_valid
    @m.value = '1234'
    @m.should_not be_valid
  end

  specify "should validate numericality_of" do
    @c.validates_numericality_of :value
    @m.value = 'blah'
    @m.should_not be_valid
    @m.value = '123'
    @m.should be_valid
    @m.value = '123.1231'
    @m.should be_valid
    @m.value = '+1'
    @m.should be_valid
    @m.value = '-1'
    @m.should be_valid
    @m.value = '+1.123'
    @m.should be_valid
    @m.value = '-0.123'
    @m.should be_valid
    @m.value = '-0.123E10'
    @m.should be_valid
    @m.value = '32.123e10'
    @m.should be_valid
    @m.value = '+32.123E10'
    @m.should be_valid
    @m.should be_valid
    @m.value = '.0123'
  end

  specify "should validate numericality_of with only_integer" do
    @c.validates_numericality_of :value, :only_integer => true
    @m.value = 'blah'
    @m.should_not be_valid
    @m.value = '123'
    @m.should be_valid
    @m.value = '123.1231'
    @m.should_not be_valid
  end
  
  specify "should validate numericality_of with if => true" do
    @c.validates_numericality_of :value, :if => :dont_skip

    @m.value = 'a'
    @m.should_not be_valid
  end

  specify "should validate numericality_of with if => false" do
    @c.validates_numericality_of :value, :if => :skip

    @m.value = 'a'
    @m.should be_valid
  end

  specify "should validate numericality_of with allow_missing => true" do
    @c.validates_numericality_of :value, :allow_missing => true
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
  end

  specify "should validate presence_of" do
    @c.validates_presence_of :value
    @m.should_not be_valid
    @m.value = ''
    @m.should_not be_valid
    @m.value = 1234
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
    @m.value = true
    @m.should be_valid
    @m.value = false
    @m.should be_valid
  end
  
  specify "should validate inclusion_of with an array" do
    @c.validates_inclusion_of :value, :in => [1,2]
    @m.should_not be_valid
    @m.value = 1
    @m.should be_valid
    @m.value = 1.5
    @m.should_not be_valid
    @m.value = 2
    @m.should be_valid    
    @m.value = 3
    @m.should_not be_valid 
  end
  
  specify "should validate inclusion_of with a range" do
    @c.validates_inclusion_of :value, :in => 1..4
    @m.should_not be_valid
    @m.value = 1
    @m.should be_valid
    @m.value = 1.5
    @m.should be_valid
    @m.value = 0
    @m.should_not be_valid
    @m.value = 5
    @m.should_not be_valid    
  end
  
  specify "should raise an error if inclusion_of doesn't receive a valid :in option" do
    lambda{@c.validates_inclusion_of :value}.should raise_error(ArgumentError)
    lambda{@c.validates_inclusion_of :value, :in => 1}.should raise_error(ArgumentError)
  end
  
  specify "should raise an error if inclusion_of handles :allow_nil too" do
    @c.validates_inclusion_of :value, :in => 1..4, :allow_nil => true
    @m.value = nil
    @m.should be_valid
    @m.value = 0
    @m.should_not be_valid
  end

  specify "should validate presence_of with if => true" do
    @c.validates_presence_of :value, :if => :dont_skip
    @m.should_not be_valid
  end

  specify "should validate presence_of with if => false" do
    @c.validates_presence_of :value, :if => :skip
    @m.should be_valid
  end

  specify "should validate presence_of with allow_missing => true" do
    @c.validates_presence_of :value, :allow_missing => true
    @m.should be_valid
    @m.value = nil
    @m.should_not be_valid
  end

  specify "should validate uniqueness_of with if => true" do
    @c.validates_uniqueness_of :value, :if => :dont_skip

    @m.value = 'a'
    @m.should_not be_valid
  end

  specify "should validate uniqueness_of with if => false" do
    @c.validates_uniqueness_of :value, :if => :skip
    @m.value = 'a'
    @m.should be_valid
  end
  
  specify "should validate uniqueness_of with allow_missing => true" do
    @c.validates_uniqueness_of :value, :allow_missing => true
    @m.should be_valid
    @m.value = 1
    @m.should_not be_valid
  end
end

describe "Superclass validations" do
  before do
    @c1 = model_class.call Sequel::Model do
      columns :value
      validates_length_of :value, :minimum => 5
    end
    
    @c2 = Class.new(@c1)
    @c2.class_eval do
      columns :value
      validates_format_of :value, :with => /^[a-z]+$/
    end
  end
  
  specify "should be checked when validating" do
    o = @c2.new
    o.value = 'ab'
    o.valid?.should == false
    o.errors.full_messages.should == ['value is too short']

    o.value = '12'
    o.valid?.should == false
    o.errors.full_messages.should == ['value is too short', 'value is invalid']

    o.value = 'abcde'
    o.valid?.should be_true
  end
  
  specify "should be skipped if skip_superclass_validations is called" do
    @c2.skip_superclass_validations

    o = @c2.new
    o.value = 'ab'
    o.valid?.should be_true

    o.value = '12'
    o.valid?.should == false
    o.errors.full_messages.should == ['value is invalid']

    o.value = 'abcde'
    o.valid?.should be_true
  end
end

describe ".validates with block" do
  specify "should support calling .each" do
    @c = model_class.call Sequel::Model do
      columns :vvv
      validates do
        each :vvv do |o, a, v|
          o.errors[a] << "is less than zero" if v.to_i < 0
        end
      end
    end
    
    o = @c.new
    o.vvv = 1
    o.should be_valid
    o.vvv = -1
    o.should_not be_valid
  end
end

describe Sequel::Model, "Validations" do

  before(:all) do
    class ::Person < Sequel::Model
      plugin :validation_class_methods
      columns :id,:name,:first_name,:last_name,:middle_name,:initials,:age, :terms
    end

    class ::Smurf < Person
    end

    class ::Can < Sequel::Model
      plugin :validation_class_methods
      columns :id, :name
    end
    
    class ::Cow < Sequel::Model
      plugin :validation_class_methods
      columns :id, :name, :got_milk
    end

    class ::User < Sequel::Model
      plugin :validation_class_methods
      columns :id, :username, :password
    end
    
    class ::Address < Sequel::Model
      plugin :validation_class_methods
      columns :id, :zip_code
    end
  end
  after(:all) do
    [:Person, :Smurf, :Cow, :User, :Address].each{|c| Object.send(:remove_const, c)}
  end
  
  it "should validate the acceptance of a column" do
    class ::Cow < Sequel::Model
      validations.clear
      validates_acceptance_of :got_milk, :accept => 'blah', :allow_nil => false
    end
    
    @cow = Cow.new
    @cow.should_not be_valid
    @cow.errors.full_messages.should == ["got_milk is not accepted"]
    
    @cow.got_milk = "blah"
    @cow.should be_valid
  end
  
  it "should validate the confirmation of a column" do
    class ::User < Sequel::Model
      def password_confirmation
        "test"
      end
      
      validations.clear
      validates_confirmation_of :password
    end
    
    @user = User.new
    @user.should_not be_valid
    @user.errors.full_messages.should == ["password is not confirmed"]
    
    @user.password = "test"
    @user.should be_valid
  end
  
  it "should validate format of column" do
    class ::Person < Sequel::Model
      validates_format_of :first_name, :with => /^[a-zA-Z]+$/
    end

    @person = Person.new :first_name => "Lancelot99"
    @person.valid?.should be_false
    @person = Person.new :first_name => "Anita"
    @person.valid?.should be_true
  end
  
  it "should validate length of column" do
    class ::Person < Sequel::Model
      validations.clear
      validates_length_of :first_name, :maximum => 30
      validates_length_of :last_name, :minimum => 30
      validates_length_of :middle_name, :within => 1..5
      validates_length_of :initials, :is => 2
    end
    
    @person = Person.new(
      :first_name => "Anamethatiswaytofreakinglongandwayoverthirtycharacters",
      :last_name => "Alastnameunderthirtychars",
      :initials => "LGC",
      :middle_name => "danger"
    )
    
    @person.should_not be_valid
    @person.errors.full_messages.size.should == 4
    @person.errors.full_messages.should include(
      'first_name is too long',
      'last_name is too short',
      'middle_name is the wrong length',
      'initials is the wrong length'
    )
    
    @person.first_name  = "Lancelot"
    @person.last_name   = "1234567890123456789012345678901"
    @person.initials    = "LC"
    @person.middle_name = "Will"
    @person.should be_valid
  end
  
  it "should validate that a column doesn't have a string value" do
    p = model_class.call Sequel::Model do
      columns :age, :price, :confirmed
      self.raise_on_typecast_failure = false
      validates_not_string :age
      validates_not_string :confirmed
      validates_not_string :price, :message=>'is not valid'
      @db_schema = {:age=>{:type=>:integer}}
    end
    
    @person = p.new
    @person.should be_valid

    @person.confirmed = 't'
    @person.should_not be_valid
    @person.errors.full_messages.should == ['confirmed is a string']
    @person.confirmed = true
    @person.should be_valid

    @person.age = 'a'
    @person.should_not be_valid
    @person.errors.full_messages.should == ['age is not a valid integer']
    @person.db_schema[:age][:type] = :datetime
    @person.should_not be_valid
    @person.errors.full_messages.should == ['age is not a valid datetime']
    @person.age = 20
    @person.should be_valid

    @person.price = 'a'
    @person.should_not be_valid
    @person.errors.full_messages.should == ['price is not valid']
    @person.price = 20
    @person.should be_valid
  end
  
  it "should validate numericality of column" do
    class ::Person < Sequel::Model
      validations.clear
      validates_numericality_of :age
    end
    
    @person = Person.new :age => "Twenty"
    @person.should_not be_valid
    @person.errors.full_messages.should == ['age is not a number']
    
    @person.age = 20
    @person.should be_valid
  end
  
  it "should validate the presence of a column" do
    class ::Cow < Sequel::Model
      validations.clear
      validates_presence_of :name
    end
    
    @cow = Cow.new
    @cow.should_not be_valid
    @cow.errors.full_messages.should == ['name is not present']
    
    @cow.name = "Betsy"
    @cow.should be_valid
  end
 
  it "should validate the uniqueness of a column" do
    class ::User < Sequel::Model
      validations.clear
      validates do
        uniqueness_of :username
      end
    end
    User.dataset._fetch = proc do |sql|
      case sql
      when /COUNT.*username = '0records'/
        {:v => 0}
      when /COUNT.*username = '2records'/
        {:v => 2}
      when /COUNT.*username = '1record'/
        {:v => 1}
      when /username = '1record'/
        {:id => 3, :username => "1record", :password => "test"}
      end
    end
    
    @user = User.new(:username => "2records", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username is already taken']

    @user = User.new(:username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username is already taken']

    @user = User.load(:id=>4, :username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username is already taken']

    @user = User.load(:id=>3, :username => "1record", :password => "anothertest")
    @user.should be_valid
    @user.errors.full_messages.should == []

    @user = User.new(:username => "0records", :password => "anothertest")
    @user.should be_valid
    @user.errors.full_messages.should == []

    User.db.sqls
    @user = User.new(:password => "anothertest")
    @user.should be_valid
    @user.errors.full_messages.should == []
    User.db.sqls.should == []
  end
  
  it "should validate the uniqueness of multiple columns" do
    class ::User < Sequel::Model
      validations.clear
      validates do
        uniqueness_of [:username, :password]
      end
    end
    User.dataset._fetch = proc do |sql|
      case sql
      when /COUNT.*username = '0records'/
        {:v => 0}
      when /COUNT.*username = '2records'/
        {:v => 2}
      when /COUNT.*username = '1record'/
        {:v => 1}
      when /username = '1record'/
        if sql =~ /password = 'anothertest'/
          {:id => 3, :username => "1record", :password => "anothertest"}
        else
          {:id => 4, :username => "1record", :password => "test"}
        end
      end
    end
    
    @user = User.new(:username => "2records", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username and password is already taken']

    @user = User.new(:username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username and password is already taken']

    @user = User.load(:id=>4, :username => "1record", :password => "anothertest")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username and password is already taken']

    @user = User.load(:id=>3, :username => "1record", :password => "test")
    @user.should_not be_valid
    @user.errors.full_messages.should == ['username and password is already taken']

    @user = User.load(:id=>3, :username => "1record", :password => "anothertest")
    @user.should be_valid
    @user.errors.full_messages.should == []

    @user = User.new(:username => "0records", :password => "anothertest")
    @user.should be_valid
    @user.errors.full_messages.should == []

    User.db.sqls
    @user = User.new(:password => "anothertest")
    @user.should be_valid
    @user.errors.full_messages.should == []
    @user = User.new(:username => "0records")
    @user.should be_valid
    @user.errors.full_messages.should == []
    @user = User.new
    @user.should be_valid
    @user.errors.full_messages.should == []
    User.db.sqls.should == []
  end
  
  it "should have a validates block that contains multiple validations" do
    class ::Person < Sequel::Model
      validations.clear
      validates do
        format_of :first_name, :with => /^[a-zA-Z]+$/
        length_of :first_name, :maximum => 30
      end
    end

    Person.validations[:first_name].size.should == 2
    
    @person = Person.new :first_name => "Lancelot99"
    @person.valid?.should be_false
    
    @person2 = Person.new :first_name => "Wayne"
    @person2.valid?.should be_true
  end

  it "should allow 'longhand' validations direcly within the model." do
    lambda {
      class ::Person < Sequel::Model
        validations.clear
        validates_length_of :first_name, :maximum => 30
      end
    }.should_not raise_error
    Person.validations.length.should eql(1)
  end

  it "should define a has_validations? method which returns true if the model has validations, false otherwise" do
    class ::Person < Sequel::Model
      validations.clear
      validates do
        format_of :first_name, :with => /\w+/
        length_of :first_name, :maximum => 30
      end
    end

    class ::Smurf < Person
      validations.clear
    end

    Person.should have_validations
    Smurf.should_not have_validations
  end

  it "should validate correctly instances initialized with string keys" do
    class ::Can < Sequel::Model
      validates_length_of :name, :minimum => 4
    end
    
    Can.new('name' => 'ab').should_not be_valid
    Can.new('name' => 'abcd').should be_valid
  end
  
end

describe "Model#save" do
  before do
    @c = model_class.call Sequel::Model(:people) do
      columns :id, :x

      validates_each :x do |o, a, v|
        o.errors[a] << 'blah' unless v == 7
      end
    end
    @m = @c.load(:id => 4, :x=>6)
    MODEL_DB.reset
  end

  specify "should save only if validations pass" do
    @m.raise_on_save_failure = false
    @m.should_not be_valid
    @m.save
    MODEL_DB.sqls.should be_empty
    
    @m.x = 7
    @m.should be_valid
    @m.save.should_not be_false
    MODEL_DB.sqls.should == ['UPDATE people SET x = 7 WHERE (id = 4)']
  end
  
  specify "should skip validations if the :validate=>false option is used" do
    @m.raise_on_save_failure = false
    @m.should_not be_valid
    @m.save(:validate=>false)
    MODEL_DB.sqls.should == ['UPDATE people SET x = 6 WHERE (id = 4)']
  end
    
  specify "should raise error if validations fail and raise_on_save_faiure is true" do
    proc{@m.save}.should raise_error(Sequel::ValidationFailed)
  end
  
  specify "should return nil if validations fail and raise_on_save_faiure is false" do
    @m.raise_on_save_failure = false
    @m.save.should == nil
  end
end
