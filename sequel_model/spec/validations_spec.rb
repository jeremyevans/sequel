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
    t.failed_message(nil).should == 'trah_lah_lah blah validation failed'
      
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
  
  specify "should return true/false for has_validations?" do
    @c.has_validations?.should be_nil
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
    @o.validation_errors.should == []
    
    @c.send(:attr_accessor, :blah)
    @c.validates_highness_of :blah
    
    @o = @c.new
    @o.score = 20
    @o.blah = 30
    
    @o.valid?
    @o.validation_errors.should == [
      'score is too low (87)',
      'blah is too low (100)'
    ]
  end
end



__END__

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
    @person.valid?.should be_true
  end
  
  it "should include errors from other models" do
    pending("Waiting for Wayne's amazing associations!")
  end
  
  it "should validate the acceptance of a column" do
    class Cow < Sequel::Model(:cows)      
      validations.clear
      validates_acceptance_of :got_milk
    end
    
    @cow = Cow.new
    @cow.valid?.should be_false
    @cow.errors.on(:got_milk).should == "must be accepted"
    
    @cow.got_milk = "true"
    @cow.valid?.should be_true
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
    @user.valid?.should be_false
    @user.errors.on(:password).should == "doesn't match confirmation"
    
    @user.password = "test"
    @user.valid?.should be_true
  end
  
  it "should validate each with logic" do
    class ZipCodeService; end
    
    class Address < Sequel::Model(:addresses)      
      validations.clear
      validates_each :zip_code, :logic => lambda { errors.add(:zip_code, "is not valid") unless ZipCodeService.allows(zip_code) }
    end
    
    @address = Address.new :zip_code => "48108"
    ZipCodeService.should_receive(:allows).with("48108").and_return(false)
    @address.valid?.should be_false
    @address.errors.on(:zip_code).should == "is not valid"
    
    @address2 = Address.new :zip_code => "48104"
    ZipCodeService.should_receive(:allows).with("48104").and_return(true)
    @address2.valid?.should be_true
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
  
  it "should allow for :with_exactly => /[a-zA-Z]/, which wraps the supplied regex with ^<regex>$" do
    pending("TODO: Add this option to Validatable#validates_format_of")
  end

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
    
    @person.valid?.should be_false
    @person.errors.on(:first_name).should  == "is invalid"
    @person.errors.on(:last_name).should   == "is invalid"
    @person.errors.on(:initials).should    == "is invalid"
    @person.errors.on(:middle_name).should == "is invalid"
    
    @person.first_name  = "Lancelot"
    @person.last_name   = "1234567890123456789012345678901"
    @person.initials    = "LC"
    @person.middle_name = "Will"
    @person.valid?.should be_true
  end
  
  it "should validate numericality of column" do
    class Person < Sequel::Model(:people)
      validations.clear
      validates_numericality_of :age
    end
    
    @person = Person.new :age => "Twenty"
    @person.valid?.should be_false
    @person.errors.on(:age).should == "must be a number"
    
    @person.age = 20
    @person.valid?.should be_true
  end
  
  it "should validate the presence of a column" do
    class Cow < Sequel::Model(:cows)      
      validations.clear
      validates_presence_of :name
    end
    
    @cow = Cow.new
    @cow.valid?.should be_false
    @cow.errors.on(:name).should == "can't be empty"
    @cow.errors.full_messages.first.should == "Name can't be empty"
    
    @cow.name = "Betsy"
    @cow.valid?.should be_true
  end
  
  it "should validate true for a column" do
    class Person < Sequel::Model(:people)
      validations.clear
      validates_true_for :first_name, :logic => lambda { first_name == "Alison" }
    end

    @person = Person.new :first_name => "Nina"
    @person.valid?.should be_false
    @person.errors.on(:first_name).should == "is invalid"
    
    @person.first_name = "Alison"
    @person.valid?.should be_true
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

  it "should require and include the validatable gem" do
    Gem.loaded_specs["validatable"].should_not be_nil
    Sequel::Model.should respond_to(:validates_format_of) # validatable gem
    Sequel::Model.should respond_to(:validations)         # Validations module
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
        true_for        :first_name, :logic => lambda { first_name == "Alison" }
        #validates_each :last_name, :logic => lambda { errors.add(:zip_code, "is not valid") unless ZipCodeService.allows(zip_code) }
        #base
      end
      
      # Now check to make sure that each validation exists in the model's validations.
    end
    pending("finish this spec for each case")
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

describe Sequel::Model, "validates" do
  

  before(:all) do
    class Person < Sequel::Model(:people)
      def columns
        [:id,:name,:first_name,:last_name,:middle_name,:initials,:age]
      end
    end
  end
  
  it "should runs the validations block on the model & store as :default when only a validations block is passed in"
  it "should store the block under the name passed in when both a name and a validations block are passed in"
  it "should return the stored validations block corresponding to the name given, if only a name is given (no block)"
  it "should return true or false based on if validations exist on the model if no arguments are given"
end
