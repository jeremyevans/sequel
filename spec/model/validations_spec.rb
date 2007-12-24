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
  
  it "should allow for :with_exactly => /[a-zA-Z/, which wraps the supplied regex with ^<regex>$" do
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
  
  it "should description" do
    Sequel::Model.should_receive(:require).with("validatable").and_raise(LoadError)
    STDERR.should_receive(:puts)
    load File.join(File.dirname(__FILE__), "../../lib/sequel/model/validations.rb")
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
  
  it "should runs the validations block on the model & store as :default when only a validations block is passed in" do
    pending
  end

  it "should store the block under the name passed in when both a name and a validations block are passed in" do
    pending
  end

  it "should return the stored validations block corresponding to the name given, if only a name is given (no block)" do
    pending
  end

  it "should return true or false based on if validations exist on the model if no arguments are given" do
    pending
  end

end
