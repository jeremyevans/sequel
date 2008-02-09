require "#{ File.dirname(__FILE__) }/spec_helper.rb"

describe subject::Validator, 'with default state' do
  
  before(:each) { @validator = subject::Validator.new }
  
  it "should have atleast the default state" do
    @validator.instance_variable_get(:@states).keys.should include(:default)
  end
  it "should shoult return default state" do
    @validator.get_state(nil).name.should == :default
  end

end

describe Validated::Validator, 'with custom states' do
  
  before(:each) do
    @states = [:create, :update]
    @validator = subject::Validator.new(*@states)
  end
  
  it "should assign states dynamically" do
    @validator.states.keys.should include(*@states)
  end
  it "should have an initial state" do
    @validator.instance_variable_get(:@initial_state).name.
    should == @states[0]
  end
  it "should add validations to all states" do
    @validator.add_validation :firstname, :lastname
    
    @validator.states.each do |name, state|
      state.validations.should include(:firstname, :lastname)
    end
  end
  it "should add validations to :create state" do
    @validator.add_validation :firstname, :lastname, :on => :create
    
    @validator.states[:create].validations.keys.
    should include(:firstname, :lastname)
    @validator.states[:update].validations.keys.
    should_not include(:firstname, :lastname)
  end
  it "should add validations to :create and :update states" do
    @validator.add_validation :firstname, :lastname, :on => [:create, :update]
    
    @validator.states.each do |name, state|
      state.validations.should include(:firstname, :lastname)
    end
  end
  it "should return initial state" do
    @validator.get_state(nil).name.should == :create
  end
  it "should not have validations" do
    @validator.should_not have_validations
  end
  it "should have validations" do
    @validator.add_validation :firstname, :lastname
    @validator.should have_validations
  end
  it "should have validations on initial state" do
    @validator.add_validation :firstname, :lastname, :on => :create
    @validator.should have_validations('')
  end
  it "should not have validations on initial state" do
    @validator.add_validation :firstname, :lastname, :on => :update
    @validator.should_not have_validations('')
  end
  it "should send! attributes to probe if invoked" do
    block = proc {|o, a, v|}

    probe = mock 'Probe'
    probe.should_receive(:send!).with(:firstname)
    probe.should_receive(:send!).with(:lastname)

    @validator.add_validation :firstname, :lastname, &block
    @validator.invoke probe
  end
  it "should call validations with object, attribute and value if invoked" do
    block = proc {|o, a, v|}

    probe = mock 'Probe'
    value = mock 'Value'
    probe.stub!(:send!).and_return(value)

    @validator.add_validation :firstname, :lastname, &block
    @validator.get_state.validations
    @validator.invoke probe
  end
  it "should clone states as well" do
    validator_clone = @validator.clone
    validator_clone.states.length == @validator.states.length
    validator_clone.states.should_not != @validator.states
  end
  
end

describe Validated::Validator::State do
  
  before(:each) { @state = Validated::Validator::State.new }
  
  it "should initialize with name and validations" do
    @state.name.should == :default
    @state.validations.should be_an_instance_of(Hash)
    
    @state = Validated::Validator::State.new :foo
    @state.name.should == :foo
  end
  it "should add validation" do
    @state.add_validation(:firstname, :lastname, :on => :default) {|o, a, v|}
    @state.validations.keys.should include(:firstname, :lastname)
  end
  it "should return validation for an attribute" do
    @state.validations[:foo] = :bar
    @state[:foo].should == :bar
  end
  it "should have validations" do
    @state.validations[:foo] = [:bar]
    @state.should have_validations
  end
  
end
