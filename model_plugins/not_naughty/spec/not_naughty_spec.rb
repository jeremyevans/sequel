require "#{ File.dirname(__FILE__) }/spec_helper.rb"

module subject::Builder
  def self.extended(base) end
end
module subject::InstanceMethods
  def self.included(base) end
end

describe subject do
  
  it "should load necessary files" do
    subject::should be_const_defined(:Validation)
    subject::should be_const_defined(:Validator)
    subject::should be_const_defined(:Builder)
    subject::should be_const_defined(:InstanceMethods)
    subject::should be_const_defined(:Errors)
  end
  it "should define a validator" do
    validated = Class.new(Object).extend subject
    validated.should respond_to(:validator)
    validated.validator.should be_an_instance_of(subject::Validator)
    
    validator = Class.new(subject::Validator)
    validated = Class.new(Object).extend subject
    validated.validator validator, :create, :update
    validated.validator.should be_an_instance_of(validator)
    validated.validator.states.keys.should include(:create, :update)
  end
  it "should extend the receiver if validator is defined" do
    validated = Class.new(Object)
    
    subject::Builder.should_receive(:extended).with(validated)
    subject::InstanceMethods.should_receive(:included).with(validated)
    
    validated.extend subject
  end
  it "should deep-clone the validator if inherited" do
    super_validated = Class.new(Object).extend subject
    super_validated.validator.add_validation(:name) {|o, a, v|}
    validated = Class.new(super_validated)
    
    validated.validator.should_not == super_validated.validator
    validated.validator.should have_validations
    validated.validator.add_validation(:name) {|o, a, v|}
    
    super_validated.validator.get_state.validations[:name].length.
    should < validated.validator.get_state.validations[:name].length
  end
  it "should prepend a validation before any method" do
    validated = Class.new(Object).extend subject
    
    validated.validated_before :clone
    instance = validated.new
    instance.should_receive(:valid?).once.and_return(true)
    instance.should_receive(:clone_without_validations)
    instance.clone
  end
  it "should raise an exception if invalid and method called" do
    validated = Class.new(Object).extend subject
    
    validated.validated_before :clone
    instance = validated.new
    instance.should_receive(:valid?).once.and_return(false)
    lambda { instance.clone }.should raise_error(subject::ValidationException)
  end
  it "should return false if invalid and method called" do
    validated = Class.new(Object).extend subject
    
    validated.validated_before :clone, :without => :exception
    instance = validated.new
    instance.should_receive(:valid?).once.and_return(false)
    instance.clone.should == false
  end
  it "should add Builder to Validation observers" do
    subject::Validation.instance_variable_get(:@observer_peers).
    should include(subject::Builder)
  end
  
end
