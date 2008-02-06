require File.dirname(__FILE__) + '/../lib/sequel_validated'

describe Sequel::Plugins::Validated::Errors do
  setup do
    @errors = Sequel::Plugins::Validated::Errors.new
    class Sequel::Plugins::Validated::Errors
      attr_accessor :errors
    end
  end
  
  specify "should be clearable using #clear" do
    @errors.errors = {1 => 2, 3 => 4}
    @errors.clear
    @errors.errors.should == {}
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

describe Sequel::Plugins::Validated do
  setup do
    @c = Class.new(Sequel::Model) do
      is :validated
      
      def self.validates_coolness_of(attr)
        validates_each(attr) {|o, a, v| o.errors[a] << 'is not cool' if v != :cool}
      end
    end
    
    @d = Class.new do
      attr_accessor :errors
      def initialize; @errors = Sequel::Plugins::Validated::Errors.new; end
    end
  end
  
  specify "should respond to validates, validations, has_validations?" do
    @c.should respond_to(:validations)
    @c.should respond_to(:has_validations?)
  end
  
  specify "should acccept validation definitions using validates_each" do
    @c.validates_each(:xx, :yy) {|o, a, v| o.errors[a] << 'too low' if v < 50}
    
    @c.validations[:update][:xx].length.should == 1
    @c.validations[:update][:yy].length.should == 1
    @c.validations[:create][:xx].length.should == 1
    @c.validations[:create][:yy].length.should == 1
    
    o = @d.new
    @c.validations[:update][:xx].first.call(o, :aa, 40)
    @c.validations[:update][:yy].first.call(o, :bb, 60)
    @c.validations[:create][:xx].first.call(o, :aa, 40)
    @c.validations[:create][:yy].first.call(o, :bb, 60)
    
    o.errors.full_messages.should == ['aa too low'] * 2
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
    @c.validations[:create][:blah].should_not be_empty
    @c.validations[:update][:blah].should_not be_empty

    o = @d.new
    @c.validations[:update][:blah].first.call(o, :ttt, 40)
    @c.validations[:create][:blah].first.call(o, :ttt, 40)
    o.errors.full_messages.should == ['ttt is not cool'] * 2
    o.errors.clear
    @c.validations[:update][:blah].first.call(o, :ttt, :cool)
    @c.validations[:create][:blah].first.call(o, :ttt, :cool)
    o.errors.should be_empty
  end
end

describe "A Validation instance" do
  setup do
    @c = Class.new(Sequel::Model) do
      attr_accessor :score
      
      is :validated
      
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

describe "Validations" do
  setup do
    @c = Class.new(Sequel::Model) do
      attr_accessor :value
      is :validated
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
end
