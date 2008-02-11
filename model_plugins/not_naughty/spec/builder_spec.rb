require "#{ File.dirname(__FILE__) }/spec_helper.rb"

describe subject::Builder do
  
  before(:each) do
    @validatable = Class.new(Object) do
      def self.validator=(validator) @validator = validator end
      def self.validator() @validator end
      def initialize(state) @state = state end
      def validation_state() state end
    end
    @validatable.validator = mock 'Validator'
    @validatable.extend(subject::Builder)
  end
  
  it "should have a delegator class" do
    subject::Builder.constants.
      should include('ValidationDelegator')
    subject::Builder.const_get('ValidationDelegator').
      should < SimpleDelegator
  end
  it "should provide the :validates builder method" do
    @validatable.should respond_to(:validates)
  end
  it "should add validation for :name via :validates" do
    @validatable.validator.should_receive(:add_validation).
      with(NotNaughty::PresenceValidation, :name, {}).twice
    @validatable.validates { presence_of :name }
    @validatable.validates(:name) { presence }
  end
  it "should add validation for :name on update via :validates" do
    @validatable.validator.should_receive(:add_validation).
      with(NotNaughty::PresenceValidation, :name, {:on => :update}).twice
    @validatable.validates { presence_of :name, :on => :update }
    @validatable.validates(:on => :update) { presence_of :name }
  end
  it "should add validation for :firstname and :lastname via :validates" do
    @validatable.validator.should_receive(:add_validation).
      with(NotNaughty::PresenceValidation, :firstname, :lastname, {}).twice
    @validatable.validates { presence_of :firstname, :lastname }
    @validatable.validates(:firstname, :lastname) { presence :name }
  end
  it "should register validation" do
    validation = Class.new(subject::Validation) do
      def self.name() 'TestValidation' end
      def initialize(opts, &block) end
      def call(obj, attr, value) end
    end

    @validatable.should respond_to(:validates_test_of)
  end
  it "should provide the :validates_each builder method" do
    @validatable.should respond_to(:validates_each)
  end
  it "should build the Validations with :validates_each" do
    @validatable.validator = mock 'Validator'
    @validatable.validator.
      should_receive(:add_validation).
      with(:a, :b)
    @validatable.validates_each(:a, :b) {|o, a, v|}
    
    pending 'expect a block'
  end
  it "should raise a NoMethodError is builder method does not exist" do
    lambda { @validatable.validates() { bunch_of :holy_crap } }.
    should raise_error(NoMethodError)
  end
  
end
