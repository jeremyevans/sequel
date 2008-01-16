require File.join(File.dirname(__FILE__), "spec_helper")

class Sequel::TrahLahLah < Sequel::Validation
end

describe "A subclass of Sequel::Validation" do
  specify "should supply its validation name underscored and symbolized" do
    Sequel::TrahLahLah.validation_name.should == :trah_lah_lah
  end
  
  specify "should be retrievable using Sequel::Validation[]" do
    Sequel::Validation[:trah_lah_lah].should == Sequel::TrahLahLah
  end
  
  specify "should initialize using attribute and opts" do
    t = Sequel::TrahLahLah.new(:big, :bad => 1, :wolf => 2)
    t.attribute.should == :big
    t.opts.should == {:bad => 1, :wolf => 2}
  end
  
  specify "should initialize using attribute only" do
    t = Sequel::TrahLahLah.new(:big)
    t.attribute.should == :big
    t.opts.should == {}
  end
  
  specify "should initialize using opts only" do
    t = Sequel::TrahLahLah.new(:bad => 1, :wolf => 2)
    t.attribute.should == nil
    t.opts.should == {:bad => 1, :wolf => 2}
  end
  
  specify "should initialize using no arguments" do
    t = Sequel::TrahLahLah.new
    t.attribute.should == nil
    t.opts.should == {}
  end
  
  specify "should accept a block" do
    t = Sequel::TrahLahLah.new {'blah'}
    t.block.should be_a_kind_of(Proc)
    t.block.call.should == 'blah'
  end
  
  specify "should accept a block through :logic option" do
    t = Sequel::TrahLahLah.new(:logic => proc {'bbbbb'})
    t.block.should be_a_kind_of(Proc)
    t.block.call.should == 'bbbbb'
  end

  specify "should merge opts with default options" do
    Sequel::TrahLahLah.default :baby => 333
    
    Sequel::TrahLahLah.new.opts.should == {:baby => 333}
    Sequel::TrahLahLah.new(:sss).opts.should == {:baby => 333}
    Sequel::TrahLahLah.new(:sss, :blow => 1).opts.should == {:baby => 333, :blow => 1}
    Sequel::TrahLahLah.new(:sss, :blow => 1, :baby => 444).opts.should == {:baby => 444, :blow => 1}

    Sequel::TrahLahLah.new(:blow => 1).opts.should == {:baby => 333, :blow => 1}
    Sequel::TrahLahLah.new(:baby => 444).opts.should == {:baby => 444}
  end
  
  specify "should provide direct access to options using .option" do
    Sequel::TrahLahLah.option :bbb
    Sequel::TrahLahLah.new.should respond_to(:bbb)
    t = Sequel::TrahLahLah.new
    t.bbb.should == nil
    t = Sequel::TrahLahLah.new(:bbb => 1234)
    t.bbb.should == 1234
  end

  specify "should provide a failed_message defaulting to @opts[:message] or 'xxx validation failed'" do
    t = Sequel::TrahLahLah.new(:blah)
    t.failed_message(nil).should == 'blah is invalid'

    t = Sequel::TrahLahLah.new
    t.failed_message(nil).should == 'trah_lah_lah validation failed'
      
    t = Sequel::TrahLahLah.new(:message => 'blah blah')
    t.failed_message(nil).should == 'blah blah'
  end

  specify "should check required options when creating new instances" do
    Sequel::TrahLahLah.required_option :blah
    proc {Sequel::TrahLahLah.new}.should raise_error(Sequel::Error)
    proc {Sequel::TrahLahLah.new(:blah => 3)}.should_not raise_error(Sequel::Error)
  end
end

describe Sequel::Validation::Generator do
  setup do
    @c = Class.new do
      @@validations = []
      
      def self.validates(*args)
        @@validations << args
      end
      
      def self.validations
        @@validations
      end
    end
  end
  
  specify "should instance_eval the block, sending everything to its receiver" do
    Sequel::Validation::Generator.new(@c) do
      presence_of :blah
      more_blah :blah => 'blah'
    end
    @c.validations.should == [
      [:presence_of, :blah],
      [:more_blah, {:blah => 'blah'}]
    ]
  end
end

class HighnessOf < Sequel::Validation
  default :threshold => 100
  option :threshold
  
  def valid?(o)
    v = o.send(attribute)
    v && (v >= threshold)
  end
  
  def failed_message(o)
    "#{attribute} is too low (#{threshold})"
  end
end

describe Sequel::Validatable do
  setup do
    @c = Class.new do
      include Sequel::Validatable
    end
  end
  
  specify "should respond to validates, validations, has_validations?" do
    @c.should respond_to(:validates)
    @c.should respond_to(:validations)
    @c.should respond_to(:has_validations?)
  end
  
  specify "should respond to validates_xxx methods" do
    @c.should respond_to(:validates_highness_of)
  end
  
  specify "should acccept validation definitions using .validates ..." do
    @c.validates :highness_of, :blah
    
    @c.validations.size.should == 1
    @c.validations.first.should be_a_kind_of(HighnessOf)
    @c.validations.first.attribute.should == :blah
  end

  specify "should acccept validation definitions using .validates {...}" do
    @c.validates do
      highness_of :miu
      highness_of :hey => 1
    end
    
    @c.validations.size.should == 2
    @c.validations.first.should be_a_kind_of(HighnessOf)
    @c.validations.last.should be_a_kind_of(HighnessOf)
    @c.validations.first.attribute.should == :miu
    @c.validations.last.opts.should == {:hey => 1, :threshold => 100}
  end

  specify "should acccept validation definitions using .validates_xxx" do
    @c.validates_highness_of :ohai
    
    @c.validations.size.should == 1
    @c.validations.first.should be_a_kind_of(HighnessOf)
    @c.validations.first.attribute.should == :ohai
  end
  
  specify "should acccept validation definitions using .validates_xxx with block" do
    @c.validates_highness_of {'blah'}

    @c.validations.size.should == 1
    @c.validations.first.should be_a_kind_of(HighnessOf)
    @c.validations.first.block.call.should == 'blah'
  end

  specify "should return true/false for has_validations?" do
    @c.has_validations?.should == false
    @c.validates_highness_of :ohai
    @c.has_validations?.should == true
  end
  
  specify "should raise Sequel::Error for unknown validation" do
    proc {@c.validates :blahblah}.should raise_error(Sequel::Error)
  end
end

describe "A Validatable instance" do
  setup do
    @c = Class.new do
      attr_accessor :score
      
      include Sequel::Validatable
      
      validates_highness_of :score, :threshold => 87
    end
    
    @o = @c.new
  end
  
  specify "should supply a #valid? method that returns true if validations pass" do
    @o.score = 50
    @o.should_not be_valid
    @o.score = 100
    @o.should be_valid
  end
  
  specify "should give a list of error messages if validations fail" do
    @o.score = 100
    @o.valid?
    @o.errors.should == []
    
    @c.send(:attr_accessor, :blah)
    @c.validates_highness_of :blah
    
    @o = @c.new
    @o.score = 20
    @o.blah = 30
    
    @o.valid?
    @o.errors.should == [
      'score is too low (87)',
      'blah is too low (100)'
    ]
  end
end

describe "Sequel validations" do
  setup do
    @c = Class.new do
      attr_accessor :value
      include Sequel::Validatable
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

  specify "should validate confirmation_of" do
    @c.send(:attr_accessor, :value_confirmation)
    @c.validates_confirmation_of :value
    
    @m.value = 'blah'
    @m.should_not be_valid
    
    @m.value_confirmation = 'blah'
    @m.should be_valid
  end

  specify "should validate confirmation_of without case sensitivity" do
    @c.send(:attr_accessor, :value_confirmation)
    @c.validates_confirmation_of :value, :case_sensitive => false

    @m.value = 'blah'
    @m.value_confirmation = 'BLAH'
    @m.should be_valid
  end
  
  specify "should validate format_of" do
    @c.validates_format_of :value, :with => /.+_.+/
    @m.value = 'abc_'
    @m.should_not be_valid
    @m.value = 'abc_def'
    @m.should be_valid
  end
  
  specify "should validate each (with custom block)" do
    @c.validates_each {errors << "error" unless value == 1111}
    @m.value = 1234
    @m.should_not be_valid
    @m.value = 1111
    @m.should be_valid
  end

  specify "should validate length_of with maximum" do
    @c.validates_length_of :value, :maximum => 5
    @m.should be_valid #=> nil is taken as a length of 0
    @m.value = '12345'
    @m.should be_valid
    @m.value = '123456'
    @m.should_not be_valid
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

  specify "should validate numericality_of" do
    @c.validates_numericality_of :value
    @m.value = 'blah'
    @m.should_not be_valid
    @m.value = '123'
    @m.should be_valid
    @m.value = '123.1231'
    @m.should be_valid
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
  
  specify "should validate presence_of" do
    @c.validates_presence_of :value
    @m.should_not be_valid
    @m.value = ''
    @m.should_not be_valid
    @m.value = 1234
    @m.should be_valid
  end
  
  specify "should validate true_for" do
    @c.validates_true_for :value, :logic => proc {value == true}
    @m.should_not be_valid
    @m.value = 'blah'
    @m.should_not be_valid
    @m.value = 1
    @m.should_not be_valid
    @m.value = true
    @m.should be_valid
  end
end

describe Sequel::Model, "Validations" do

  before(:all) do
    class Person < Sequel::Model(:people)
      def columns
        [:id,:name,:first_name,:last_name,:middle_name,:initials,:age, :terms]
      end
    end

    class Smurf < Person
    end
    
    class Cow < Sequel::Model(:cows)
      def columns
        [:id, :name, :got_milk]
      end
    end

    class User < Sequel::Model(:users)
      def columns
        [:id, :username, :password]
      end
    end
    
    class Address < Sequel::Model(:addresses)
      def columns
        [:id, :zip_code]
      end
    end
  end
  
  it "should have a hook before validating" do
    class Person < Sequel::Model(:people)
      before_validation do
        self.name = "default name"
      end
      validations.clear
      validates_presence_of :name
    end

    @person = Person.new
    @person.should be_valid
  end
  
  # it "should include errors from other models" do
  #   pending("Waiting for Wayne's amazing associations!")
  # end
  
  it "should validate the acceptance of a column" do
    class Cow < Sequel::Model(:cows)
      validations.clear
      validates_acceptance_of :got_milk, :accept => 'blah', :allow_nil => false
    end
    
    @cow = Cow.new
    @cow.should_not be_valid
    @cow.errors.should == ["got_milk must be accepted"]
    
    @cow.got_milk = "blah"
    @cow.should be_valid
  end
  
  it "should validate the confirmation of a column" do
    class User < Sequel::Model(:users)      
      def password_confirmation
        "test"
      end
      
      validations.clear
      validates_confirmation_of :password
    end
    
    @user = User.new
    @user.should_not be_valid
    @user.errors.should == ["password must be confirmed"]
    
    @user.password = "test"
    @user.should be_valid
  end
  
  it "should validate each" do
    class ZipCodeService
      def self.allows(zip); zip == '48104'; end
    end
    
    class Address < Sequel::Model(:addresses)
      validations.clear
      validates_each {errors << "zip_code is invalid" unless ZipCodeService.allows(zip_code)}
    end
    
    @address = Address.new :zip_code => "48108"
    @address.should_not be_valid
    @address.errors.should == ["zip_code is invalid"]
    
    @address2 = Address.new :zip_code => "48104"
    @address2.should be_valid
  end
  
  it "should validate format of column" do
    class Person < Sequel::Model(:people)  
      validates_format_of :first_name, :with => /^[a-zA-Z]+$/
    end

    @person = Person.new :first_name => "Lancelot99"
    @person.valid?.should be_false
    @person = Person.new :first_name => "Anita"
    @person.valid?.should be_true
  end
  
  # it "should allow for :with_exactly => /[a-zA-Z]/, which wraps the supplied regex with ^<regex>$" do
  #   pending("TODO: Add this option to Validatable#validates_format_of")
  # end

  it "should validate length of column" do
    class Person < Sequel::Model(:people)
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
    @person.errors.should == [
      'first_name is invalid',
      'last_name is invalid',
      'middle_name is invalid',
      'initials is invalid'
    ]
    
    @person.first_name  = "Lancelot"
    @person.last_name   = "1234567890123456789012345678901"
    @person.initials    = "LC"
    @person.middle_name = "Will"
    @person.should be_valid
  end
  
  it "should validate numericality of column" do
    class Person < Sequel::Model(:people)
      validations.clear
      validates_numericality_of :age
    end
    
    @person = Person.new :age => "Twenty"
    @person.should_not be_valid
    @person.errors.should == ['age must be a number']
    
    @person.age = 20
    @person.should be_valid
  end
  
  it "should validate the presence of a column" do
    class Cow < Sequel::Model(:cows)
      validations.clear
      validates_presence_of :name
    end
    
    @cow = Cow.new
    @cow.should_not be_valid
    @cow.errors.should == ['name must be present']
    
    @cow.name = "Betsy"
    @cow.should be_valid
  end
  
  it "should validate true for a column" do
    class Person < Sequel::Model(:people)
      validations.clear
      validates_true_for(:first_name) {first_name == "Alison"}
    end

    @person = Person.new :first_name => "Nina"
    @person.should_not be_valid
    @person.errors.should == ['first_name is invalid']
    
    @person.first_name = "Alison"
    @person.should be_valid
  end
    
  it "should have a validates block that calls multple validations" do
    class Person < Sequel::Model(:people)
      validations.clear
      validates do
        format_of :first_name, :with => /^[a-zA-Z]+$/
        length_of :first_name, :maximum => 30
      end
    end

    Person.validations.length.should eql(2)
    
    @person = Person.new :first_name => "Lancelot99"
    @person.valid?.should be_false
    
    @person2 = Person.new :first_name => "Wayne"
    @person2.valid?.should be_true
  end

  it "should allow 'longhand' validations direcly within the model." do
    lambda {
      class Person < Sequel::Model(:people)
        validations.clear
        validates_length_of :first_name, :maximum => 30
      end
    }.should_not raise_error
    Person.validations.length.should eql(1)
  end

  it "should validates do should allow shorthand method for every longhand validates_* method" do
    class Person
      validations.clear
      validates do
        format_of       :first_name, :with => /^[a-zA-Z]+$/
        length_of       :first_name, :maximum => 30
        presence_of     :first_name
        numericality_of :age
        acceptance_of   :terms
        confirmation_of :password
        true_for        :first_name, :blah => 1
      end
    end
    Person.validations.map {|v| v.class}.should == [
      Sequel::Validation::FormatOf,
      Sequel::Validation::LengthOf,
      Sequel::Validation::PresenceOf,
      Sequel::Validation::NumericalityOf,
      Sequel::Validation::AcceptanceOf,
      Sequel::Validation::ConfirmationOf,
      Sequel::Validation::TrueFor
    ]
    Person.validations.map {|v| v.attribute}.should == [
      :first_name,
      :first_name,
      :first_name,
      :age,
      :terms,
      :password,
      :first_name
    ]
    Person.validations.map {|v| v.opts}.should == [
      {:with => /^[a-zA-Z]+$/},
      {:maximum => 30},
      {},
      {},
      {:accept=>"1", :allow_nil=>true},
      {:case_sensitive=>true},
      {:blah => 1}
    ]
  end

  it "should define a has_validations? method which returns true if the model has validations, false otherwise" do
    class Person < Sequel::Model(:people)
      validations.clear
      validates do
        format_of :first_name, :with => /\w+/
        length_of :first_name, :maximum => 30
      end
    end

    class Smurf < Person
      validations.clear
    end

    Person.should have_validations
    Smurf.should_not have_validations
  end
end

describe "Model#save!" do
  setup do
    @c = Class.new(Sequel::Model(:people)) do
      def columns; [:id]; end
      
      validates_each {errors << "blah" unless id == 5}
    end
    @m = @c.new(:id => 4)
    MODEL_DB.reset
  end
  
  specify "should save regardless of validations" do
    @m.should_not be_valid
    @m.save!
    MODEL_DB.sqls.should == ['UPDATE people SET id = 4 WHERE (id = 4)']
  end
end

describe "Model#save!" do
  setup do
    @c = Class.new(Sequel::Model(:people)) do
      def columns; [:id]; end

      validates_each {errors << "blah" unless id == 5}
    end
    @m = @c.new(:id => 4)
    MODEL_DB.reset
  end

  specify "should save only if validations pass" do
    @m.should_not be_valid
    @m.save
    MODEL_DB.sqls.should be_empty
    
    @m.id = 5
    @m.should be_valid
    @m.save
    MODEL_DB.sqls.should == ['UPDATE people SET id = 5 WHERE (id = 5)']
  end
  
  specify "should return false if validations fail" do
    @m.save.should == false
  end
end