require "#{ File.dirname(__FILE__) }/spec_helper.rb"

::NotNaughty::Validation.load(
  :acceptance, :confirmation, :format,
  :length, :numericality, :presence
)

describe subject::LengthValidation do
  
  before(:each) { @receiver, @errors = mock('Receiver'), mock('Errors') }
  
  it "should return the 'precise' block" do
    validation = subject::LengthValidation.new :is => 8, :within => 10..12
    
    probe = mock 'Probe', :length => 8, :nil? => false
    validation.call @receiver, :probe, probe
    
    @receiver.should_receive(:errors).and_return(@errors)
    @errors.should_receive(:add).with(:probe, an_instance_of(String))
    
    probe = mock 'Probe', :length => 11, :nil? => false
    validation.call @receiver, :probe, probe
  end
  it "should return the 'range' block" do
    validation = subject::LengthValidation.
      new :within => 10..12, :maximum => 9
    
    probe = mock 'Probe', :length => 10, :nil? => false
    validation.call @receiver, :probe, probe
    
    @receiver.should_receive(:errors).and_return(@errors)
    @errors.should_receive(:add).with(:probe, an_instance_of(String))
    
    probe = mock 'Probe', :length => 9, :nil? => false
    validation.call @receiver, :probe, probe
  end
  it "should return the 'maximum' block" do
    validation = subject::LengthValidation.
      new :maximum => 9
    
    probe = mock 'Probe', :length => 9, :nil? => false
    validation.call @receiver, :probe, probe
    
    @receiver.should_receive(:errors).and_return(@errors)
    @errors.should_receive(:add).with(:probe, an_instance_of(String))
    
    probe = mock 'Probe', :length => 10, :nil? => false
    validation.call @receiver, :probe, probe
  end
  it "should return the 'minimum' block" do
    validation = subject::LengthValidation.
      new :minimum => 9
    
    probe = mock 'Probe', :length => 9, :nil? => false
    validation.call @receiver, :probe, probe
    
    @receiver.should_receive(:errors).and_return(@errors)
    @errors.should_receive(:add).with(:probe, an_instance_of(String))
    
    probe = mock 'Probe', :length => 8, :nil? => false
    validation.call @receiver, :probe, probe
  end
  it "should raise an ArgumentError" do
    lambda { subject::LengthValidation.new }.
    should raise_error(ArgumentError)
  end
  
end

LengthExample = Struct.new(:name).extend(subject)
describe LengthExample do
  
  before(:each) { @example = LengthExample.clone }
  
  it "should always allow nil " do
    @example.validates_length_of :name, :is => 1, :allow_nil => false
    @example.new(nil).should be_valid
    @example.new('').should_not be_valid
    @example.new('a').should be_valid
    @example.new('ab').should_not be_valid
  end
  it "should allow blank" do
    @example.validates_length_of :name, :is => 1, :allow_blank => true
    @example.new(nil).should be_valid
    @example.new('').should be_valid
    @example.new('a').should be_valid
    @example.new('ab').should_not be_valid
  end
  
end

FormatExample = Struct.new(:email).extend(subject)
describe FormatExample do
  
  before(:each) { @example = FormatExample.clone }
  
  it "claims to match 99% of all e-mail addresses out there..." do
    # Regexp was taken from: http://www.regular-expressions.info/email.html
    @example.validates_format_of :email,
      :with => /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b/i
    @example.new('"Foo Bar" <foo@bar.com>').should be_valid
    @example.new('foo@bar.com').should be_valid
    @example.new('foobarcom').should_not be_valid
    @example.new(nil).should_not be_valid
    @example.new('').should_not be_valid
  end
  it "should allow nil e-mail addresses" do
    @example.validates_format_of :email, :allow_nil => true,
      :with => /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b/i
    @example.new('"Foo Bar" <foo@bar.com>').should be_valid
    @example.new('foo@bar.com').should be_valid
    @example.new('foobarcom').should_not be_valid
    @example.new(nil).should be_valid
    @example.new('').should_not be_valid
  end
  it "should allow blank e-mail addresses" do
    @example.validates_format_of :email, :allow_blank => true,
      :with => /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b/i
    @example.new('"Foo Bar" <foo@bar.com>').should be_valid
    @example.new('foo@bar.com').should be_valid
    @example.new('foobarcom').should_not be_valid
    @example.new(nil).should be_valid
    @example.new('').should be_valid
  end
  it "should raise an ArgumentError if format does not respond to :match" do
    lambda { @example.validates_format_of :email }.
    should raise_error(ArgumentError)
    lambda { @example.validates_format_of :email, :with => 1 }.
    should raise_error(ArgumentError)
    lambda { @example.validates_format_of :email, :with => '' }.
    should_not raise_error(ArgumentError)
    lambda { @example.validates_format_of :email, :with => // }.
    should_not raise_error(ArgumentError)
  end
  
end

PresenceExample = Struct.new(:name).extend(subject)
describe PresenceExample do
  
  before(:each) { @example = PresenceExample.clone }
  
  it "should be present" do
    @example.validates_presence_of :name
    @example.new(0).should be_valid
    @example.new([0]).should be_valid
    @example.new('0').should be_valid
  end
  it "should not be present" do
    @example.validates_presence_of :name
    @example.new(nil).should_not be_valid
    @example.new([]).should_not be_valid
    @example.new('').should_not be_valid
  end
  
end

AcceptanceExample = Struct.new(:conditions).extend(subject)
describe AcceptanceExample do
  
  before(:each) { @example = AcceptanceExample.clone }
  
  it "should accept '1' and allows nil by default" do
    @example.validates_acceptance_of :conditions
    @example.new(nil).should be_valid
    @example.new('').should_not be_valid
    @example.new(true).should_not be_valid
    @example.new(false).should_not be_valid
    @example.new('0').should_not be_valid
    @example.new('1').should be_valid
  end
  it "should accept true and allows nil by default" do
    @example.validates_acceptance_of :conditions, :accept => true
    @example.new(nil).should be_valid
    @example.new('').should_not be_valid
    @example.new(true).should be_valid
    @example.new(false).should_not be_valid
    @example.new('0').should_not be_valid
    @example.new('1').should_not be_valid
  end
  it "should accept '1' and disallows nil" do
    @example.validates_acceptance_of :conditions, :accept => true,
      :allow_nil => false
    
    @example.new(nil).should_not be_valid
    @example.new('').should_not be_valid
    @example.new(true).should be_valid
    @example.new(false).should_not be_valid
    @example.new('0').should_not be_valid
    @example.new('1').should_not be_valid
  end
  it "should accept '1' and allow blank" do
    @example.validates_acceptance_of :conditions, :accept => true,
      :allow_blank => true
    
    @example.new(nil).should be_valid
    @example.new('').should be_valid
    @example.new(true).should be_valid
    @example.new(false).should be_valid
    @example.new('0').should_not be_valid
    @example.new('1').should_not be_valid
  end
  
end

ConfirmationExample = Struct.new(:name, :name_confirmation).extend(subject)
describe ConfirmationExample do
  
  before(:each) { @example = ConfirmationExample.clone }
  
  it "should be confirmed without allowing neither :nil nor :blank" do
    @example.validates_confirmation_of :name
    
    @example.new(nil, 'foo').should_not be_valid
    @example.new('', 'foo').should_not be_valid

    @example.new('foo', 'foo').should be_valid
    @example.new('foo', 'bar').should_not be_valid
  end
  it "should be confirmed with allowing :nil" do
    @example.validates_confirmation_of :name, :allow_nil => true
    
    @example.new(nil, 'foo').should be_valid
    @example.new('', 'foo').should_not be_valid
    
    @example.new('foo', 'foo').should be_valid
    @example.new('foo', 'bar').should_not be_valid
  end
  it "should be confirmed with allowing :blank" do
    @example.validates_confirmation_of :name, :allow_blank => true
    
    @example.new(nil, 'foo').should be_valid
    @example.new('', 'foo').should be_valid
    
    @example.new('foo', 'foo').should be_valid
    @example.new('foo', 'bar').should_not be_valid
  end
  
end

NumericalityExample = Struct.new(:weight).extend(subject)
describe NumericalityExample do
  
  before(:each) { @example = NumericalityExample.clone }
  
  it "should be matched with number pattern" do
    @example.validates_numericality_of :weight
    
    @example.new('-123.56').should be_valid
    
    @example.new('+123').should be_valid
    @example.new('-123').should be_valid
    @example.new('123').should be_valid
    @example.new('abc').should_not be_valid
  end
  it "should be matched with integer pattern" do
    @example.validates_numericality_of :weight, :only_integer => true
    
    @example.new('-123.45').should_not be_valid
    
    @example.new('+123').should be_valid
    @example.new('-123').should be_valid
    @example.new('123').should be_valid
    @example.new('abc').should_not be_valid
  end
  
end
