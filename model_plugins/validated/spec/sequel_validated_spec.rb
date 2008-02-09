require "#{ File.dirname(__FILE__) }/sequel_spec_helper.rb"

describe sequel do
  
  before(:each) do
    @obj = Class.new(Sequel::Model) { is :validated }
  end
  
  it "should delegate validation methods in receiver" do
    @instance = @obj.new

    @obj.validator.should be_an_instance_of(sequel)
    @obj.validator.should_receive(:states).and_return({})
    @obj.validator.should_receive(:has_validations?)
    @obj.validator.should_receive(:invoke).with(@instance)
    
    @instance.errors.should be_an_instance_of(Validated::Errors)
    @instance.should respond_to(:validate)
    @instance.should respond_to(:valid?)
    @instance.should respond_to(:save_without_validations)

    @obj.validations
    @obj.has_validations?
    @obj.validate @instance
  end
  it "should return validations" do
    @obj.validations.should be_an_instance_of(Hash)
  end
  it "should not have validations" do
    @obj.has_validations?.should == false
  end
  it "should add_validation instance of validation" do
    validation = Class.new(Validated::Validation)
    
    @obj.validator.add_validation validation, :attribute
    
    validations = @obj.validator.states[:create][:attribute]
    validations.length.should == 1
    validations.first.should be_an_instance_of(validation)
    
    validations = @obj.validator.states[:update][:attribute]
    validations.length.should == 1
    validations.first.should be_an_instance_of(validation)
    
    @obj.validator.add_validation validation, :attribute, :on => :create
    
    validations = @obj.validator.states[:create][:attribute]
    validations.length.should == 2
    validations[0].should be_an_instance_of(validation)
    validations[1].should be_an_instance_of(validation)
    
    validations = @obj.validator.states[:update][:attribute]
    validations.length.should == 1
    validations.first.should be_an_instance_of(validation)
  end
  it "should have validations" do
    validation = Class.new(Validated::Validation)
    @obj.validator.add_validation validation, :attribute
    
    @obj.has_validations?.should == true
  end
  it "should add_validation blocks as Validation" do
    @obj.validator.add_validation(:attribute) { |o, a, v| }
    
    @obj.validator.states[:create][:attribute].first.
    should be_kind_of(Validated::Validation)
    @obj.validator.states[:update][:attribute].first.
    should be_kind_of(Validated::Validation)
  end
  it "should run validations on object when it's invoked" do
    probe = mock 'Probe', :new? => true
    probe.should_receive(:attribute).and_return(1)
    probe.should_receive(:test).with(:attribute, 1)
    
    @obj.validator.add_validation(:attribute) { |o, a, v| o.test a, v }
    @obj.validate probe
  end
  it "should validate if saved" do
    instance = @obj.new
    instance.stub!(:save_without_validations).and_return(false)
    @obj.validator.should_receive(:invoke).with(instance)
    instance.save.should be_false
  end
  it "should run hooks if validated" do
    instance = @obj.new
    instance.should_receive(:before_validate)
    instance.should_receive(:after_validate)
    instance.validate
  end
  it "should not run hooks if validated" do
    i = Class.new(Sequel::Model) { is :validated, :without => :hooks }.new
    i.should_not_receive(:before_validate)
    i.should_not_receive(:after_validate)
    i.validate
  end
  
end
