require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "Validations" do

  before(:all) do
    class Person < Sequel::Model
      def columns
        [:id,:name,:first_name,:last_name,:middle_name,:initials,:age, :terms]
      end
    end

    class Smurf < Person
    end
    
    class Cow < Sequel::Model
      def columns
        [:id, :name, :got_milk]
      end
    end

    class User < Sequel::Model
      def columns
        [:id, :username, :password]
      end
    end
    
    class Address < Sequel::Model
      def columns
        [:id, :zip_code]
      end
    end
  end
  
  it "should validate the acceptance of a column" do
    class Cow < Sequel::Model
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
    class User < Sequel::Model
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
    class Person < Sequel::Model
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
    class Person < Sequel::Model
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
  
  it "should validate numericality of column" do
    class Person < Sequel::Model
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
    class Cow < Sequel::Model
      validations.clear
      validates_presence_of :name
    end
    
    @cow = Cow.new
    @cow.should_not be_valid
    @cow.errors.full_messages.should == ['name is not present']
    
    @cow.name = "Betsy"
    @cow.should be_valid
  end
  
  it "should have a validates block that calls multple validations" do
    class Person < Sequel::Model
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
      class Person < Sequel::Model
        validations.clear
        validates_length_of :first_name, :maximum => 30
      end
    }.should_not raise_error
    Person.validations.length.should eql(1)
  end

  it "should define a has_validations? method which returns true if the model has validations, false otherwise" do
    class Person < Sequel::Model
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
      
      validates_each :id do |o, a, v|
        o.errors[a] << 'blah' unless v == 5
      end
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

      validates_each :id do |o, a, v|
        o.errors[a] << 'blah' unless v == 5
      end
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