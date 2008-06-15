require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model::Validation::Errors do
  setup do
    @errors = Sequel::Model::Validation::Errors.new
  end
  
  specify "should be clearable using #clear" do
    @errors.add(:a, 'b')
    @errors.should == {:a=>['b']}
    @errors.clear
    @errors.should == {}
  end
  
  specify "should be empty if no errors are added" do
    @errors.should be_empty
    @errors[:blah] << "blah"
    @errors.should_not be_empty
  end
  
  specify "should return errors for a specific attribute using #on or #[]" do
    @errors[:blah].should == []
    @errors.on(:blah).should == []

    @errors[:blah] << 'blah'
    @errors[:blah].should == ['blah']
    @errors.on(:blah).should == ['blah']

    @errors[:bleu].should == []
    @errors.on(:bleu).should == []
  end
  
  specify "should accept errors using #[] << or #add" do
    @errors[:blah] << 'blah'
    @errors[:blah].should == ['blah']
    
    @errors.add :blah, 'zzzz'
    @errors[:blah].should == ['blah', 'zzzz']
  end
  
  specify "should return full messages using #full_messages" do
    @errors.full_messages.should == []
    
    @errors[:blow] << 'blieuh'
    @errors[:blow] << 'blich'
    @errors[:blay] << 'bliu'
    msgs = @errors.full_messages
    msgs.size.should == 3
    msgs.should include('blow blieuh', 'blow blich', 'blay bliu')
  end
end

describe Sequel::Model do
  setup do
    @c = Class.new(Sequel::Model) do
      def self.validates_coolness_of(attr)
        validates_each(attr) {|o, a, v| o.errors[a] << 'is not cool' if v != :cool}
      end
    end
  end
  
  specify "should respond to validates, validations, has_validations?" do
    @c.should respond_to(:validations)
    @c.should respond_to(:has_validations?)
  end
  
  specify "should acccept validation definitions using validates_each" do
    @c.validates_each(:xx, :yy) {|o, a, v| o.errors[a] << 'too low' if v < 50}
    
    @c.validations[:xx].size.should == 1
    @c.validations[:yy].size.should == 1
    
    o = @c.new
    @c.validations[:xx].first.call(o, :aa, 40)
    @c.validations[:yy].first.call(o, :bb, 60)
    
    o.errors.full_messages.should == ['aa too low']
  end

  specify "should return true/false for has_validations?" do
    @c.has_validations?.should == false
    @c.validates_each(:xx) {1}
    @c.has_validations?.should == true
  end
  
  specify "should provide a validates method that takes block with validation definitions" do
    @c.validates do
      coolness_of :blah
    end
    @c.validations[:blah].should_not be_empty

    o = @c.new
    @c.validations[:blah].first.call(o, :ttt, 40)
    o.errors.full_messages.should == ['ttt is not cool']
    o.errors.clear
    @c.validations[:blah].first.call(o, :ttt, :cool)
    o.errors.should be_empty
  end
end

describe Sequel::Model do
  setup do
    @c = Class.new(Sequel::Model) do
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

describe Sequel::Model::Validation::Generator do
  setup do
    $testit = nil
    
    @c = Class.new(Sequel::Model) do
      def self.validates_blah
        $testit = 1324
      end
    end
  end
  
  specify "should instance_eval the block, sending everything to its receiver" do
    Sequel::Model::Validation::Generator.new(@c) do
      blah
    end
    $testit.should == 1324
  end
end

describe Sequel::Model do
  setup do
    @c = Class.new(Sequel::Model) do
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
  
  specify "should validate length_of with maximum" do
    @c.validates_length_of :value, :maximum => 5
    @m.should_not be_valid
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

  specify "should validate presence_of" do
    @c.validates_presence_of :value
    @m.should_not be_valid
    @m.value = ''
    @m.should_not be_valid
    @m.value = 1234
    @m.should be_valid
  end

  specify "should validate presence_of with if => true" do
    @c.validates_presence_of :value, :if => :dont_skip
    @m.should_not be_valid
  end

  specify "should validate presence_of with if => false" do
    @c.validates_presence_of :value, :if => :skip
    @m.should be_valid
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
  
  specify "should validate with :if => block" do
    @c.validates_presence_of :value, :if => proc {false}
    
    @m.should be_valid
  end
end

context "Superclass validations" do
  setup do
    @c1 = Class.new(Sequel::Model) do
      columns :value
      validates_length_of :value, :minimum => 5
    end
    
    @c2 = Class.new(@c1) do
      columns :value
      validates_format_of :value, :with => /^[a-z]+$/
    end
  end
  
  specify "should be checked when validating" do
    o = @c2.new
    o.value = 'ab'
    o.valid?.should == false
    o.errors.full_messages.should == [
      'value is too short'
    ]

    o.value = '12'
    o.valid?.should == false
    o.errors.full_messages.should == [
      'value is too short',
      'value is invalid'
    ]

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
    o.errors.full_messages.should == [
      'value is invalid'
    ]

    o.value = 'abcde'
    o.valid?.should be_true
  end
end

context ".validates with block" do
  specify "should support calling .each" do
    @c = Class.new(Sequel::Model) do
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
    class Person < Sequel::Model
      columns :id,:name,:first_name,:last_name,:middle_name,:initials,:age, :terms
    end

    class Smurf < Person
    end
    
    class Cow < Sequel::Model
      columns :id, :name, :got_milk
    end

    class User < Sequel::Model
      columns :id, :username, :password
    end
    
    class Address < Sequel::Model
      columns :id, :zip_code
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
 
  it "should validate the uniqueness of a column" do
    class User < Sequel::Model
      validations.clear
      validates do
        uniqueness_of :username
      end
    end
    User.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        
        case sql
        when /COUNT.*username = '0records'/
        when /COUNT.*username = '2records'/
          yield({:v => 2})
        when /COUNT.*username = '1record'/
          yield({:v => 1})
        when /username = '1record'/
          yield({:id => 3, :username => "1record", :password => "test"})
        end
      end
    })
    
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
  end
  
  it "should have a validates block that contains multiple validations" do
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

  it "should validate correctly instances initialized with string keys" do
    class Can < Sequel::Model
      columns :id, :name
      
      validates_length_of :name, :minimum => 4
    end
    
    Can.new('name' => 'ab').should_not be_valid
    Can.new('name' => 'abcd').should be_valid
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
    @m = @c.load(:id => 4)
    MODEL_DB.reset
  end
  
  specify "should save regardless of validations" do
    @m.should_not be_valid
    @m.save!
    MODEL_DB.sqls.should == ['UPDATE people SET id = 4 WHERE (id = 4)']
  end
end

describe "Model#save" do
  setup do
    @c = Class.new(Sequel::Model(:people)) do
      columns :id

      validates_each :id do |o, a, v|
        o.errors[a] << 'blah' unless v == 5
      end
    end
    @m = @c.load(:id => 4)
    MODEL_DB.reset
  end

  specify "should save only if validations pass" do
    @m.should_not be_valid
    @m.save
    MODEL_DB.sqls.should be_empty
    
    @m.id = 5
    @m.should be_valid
    @m.save.should_not be_false
    MODEL_DB.sqls.should == ['UPDATE people SET id = 5 WHERE (id = 5)']
  end
  
  specify "should return false if validations fail" do
    @m.save.should == false
  end
end
