require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model::Errors do
  before do
    @errors = Sequel::Model::Errors.new
  end
  
  specify "should be clearable using #clear" do
    @errors.add(:a, 'b')
    @errors.should == {:a=>['b']}
    @errors.clear
    @errors.should == {}
  end
  
  specify "should be empty if there are no errors" do
    @errors.should be_empty
  end
  
  specify "should not be empty if there are errors" do
    @errors.add(:blah, "blah")
    @errors.should_not be_empty
  end
  
  qspecify "should return errors for a specific attribute using #[]" do
    @errors[:blah].should == []
    @errors[:blah] << 'blah'
    @errors[:blah].should == ['blah']

    @errors[:bleu].should == []
  end
  
  specify "should return an array of errors for a specific attribute using #on if there are errors" do
    @errors.add(:blah, 'blah')
    @errors.on(:blah).should == ['blah']
  end
  
  specify "should return nil using #on if there are no errors for that attribute" do
    @errors.on(:blah).should == nil
  end
  
  qspecify "should accept errors using #[] <<" do
    @errors[:blah] << 'blah'
    @errors[:blah].should == ['blah']
  end
    
  specify "should accept errors using #add" do
    @errors.add :blah, 'zzzz'
    @errors[:blah].should == ['zzzz']
  end
  
  specify "should return full messages using #full_messages" do
    @errors.full_messages.should == []
    
    @errors.add(:blow, 'blieuh')
    @errors.add(:blow, 'blich')
    @errors.add(:blay, 'bliu')
    msgs = @errors.full_messages
    msgs.size.should == 3
    msgs.should include('blow blieuh', 'blow blich', 'blay bliu')
  end

  specify "should not add column names for LiteralStrings" do
    @errors.full_messages.should == []
    
    @errors.add(:blow, 'blieuh')
    @errors.add(:blow, Sequel.lit('blich'))
    @errors.add(:blay, 'bliu')
    msgs = @errors.full_messages
    msgs.size.should == 3
    msgs.should include('blow blieuh', 'blich', 'blay bliu')
  end

  specify "should return the number of error messages using #count" do
    @errors.count.should == 0
    @errors.add(:a, 'b')
    @errors.count.should == 1
    @errors.add(:a, 'c')
    @errors.count.should == 2
    @errors.add(:b, 'c')
    @errors.count.should == 3
  end

  specify "should return the array of error messages for a given attribute using #on" do
    @errors.add(:a, 'b')
    @errors.on(:a).should == ['b']
    @errors.add(:a, 'c')
    @errors.on(:a).should == ['b', 'c']
    @errors.add(:b, 'c')
    @errors.on(:a).should == ['b', 'c']
  end

  specify "should return nil if there are no error messages for a given attribute using #on" do
    @errors.on(:a).should == nil
    @errors.add(:b, 'b')
    @errors.on(:a).should == nil
  end
end

describe Sequel::Model do
  before do
    @c = Class.new(Sequel::Model) do
      columns :score
      def validate
        errors.add(:score, 'too low') if score < 87
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
    @o.errors.on(:blah).should be_nil
  end
  
  specify "should allow raising of ValidationFailed with a Model instance with errors" do
    @o.errors.add(:score, 'is too low')
    begin
      raise Sequel::ValidationFailed, @o
    rescue Sequel::ValidationFailed => e
    end
    e.model.should equal(@o)
    e.errors.should equal(@o.errors)
    e.message.should == 'score is too low'
  end
  
  specify "should allow raising of ValidationFailed with an Errors instance" do
    @o.errors.add(:score, 'is too low')
    begin
      raise Sequel::ValidationFailed, @o.errors
    rescue Sequel::ValidationFailed => e
    end
    e.model.should be_nil
    e.errors.should equal(@o.errors)
    e.message.should == 'score is too low'
  end

  specify "should allow raising of ValidationFailed with a string" do
    proc{raise Sequel::ValidationFailed, "no reason"}.should raise_error(Sequel::ValidationFailed, "no reason")
  end
end

describe "Model#save" do
  before do
    @c = Class.new(Sequel::Model(:people)) do
      columns :id, :x

      def validate
        errors.add(:id, 'blah') unless x == 7
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

  specify "should raise error if validations fail and raise_on_save_failure is true" do
    proc{@m.save}.should(raise_error(Sequel::ValidationFailed) do |e|
      e.model.should equal(@m)
      e.errors.should equal(@m.errors)
    end)
  end

  specify "should raise error if validations fail and :raise_on_failure option is true" do
    @m.raise_on_save_failure = false
    proc{@m.save(:raise_on_failure => true)}.should raise_error(Sequel::ValidationFailed)
  end

  specify "should return nil if validations fail and raise_on_save_faiure is false" do
    @m.raise_on_save_failure = false
    @m.save.should == nil
  end
end
