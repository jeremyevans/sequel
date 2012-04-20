require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::Dirty" do
  before do
    @db = Sequel.mock(:fetch=>{:initial=>'i', :initial_changed=>'ic'}, :numrows=>1)
    @c = Class.new(Sequel::Model(@db[:c]))
    @c.plugin :dirty
    @c.columns :initial, :initial_changed, :missing, :missing_changed
  end

  shared_examples_for "dirty plugin" do
    it "initial_value should be the current value if value has not changed" do
      @o.initial_value(:initial).should == 'i'
      @o.initial_value(:missing).should == nil
    end

    it "initial_value should be the intial value if value has changed" do
      @o.initial_value(:initial_changed).should == 'ic'
      @o.initial_value(:missing_changed).should == nil
    end

    it "initial_value should handle case where initial value is reassigned later" do
      @o.initial_changed = 'ic'
      @o.initial_value(:initial_changed).should == 'ic'
      @o.missing_changed = nil
      @o.initial_value(:missing_changed).should == nil
    end

    it "changed_columns should handle case where initial value is reassigned later" do
      @o.changed_columns.should == [:initial_changed, :missing_changed]
      @o.initial_changed = 'ic'
      @o.changed_columns.should == [:missing_changed]
      @o.missing_changed = nil
      @o.changed_columns.should == [:missing_changed]
    end

    it "column_change should give initial and current values if there has been a change made" do
      @o.column_change(:initial_changed).should == ['ic', 'ic2']
      @o.column_change(:missing_changed).should == [nil, 'mc2']
    end

    it "column_change should be nil if no change has been made" do
      @o.column_change(:initial).should == nil
      @o.column_change(:missing).should == nil
    end

    it "column_changed? should return whether the column has changed" do
      @o.column_changed?(:initial).should == false
      @o.column_changed?(:initial_changed).should == true
      @o.column_changed?(:missing).should == false
      @o.column_changed?(:missing_changed).should == true
    end

    it "column_changed? should handle case where initial value is reassigned later" do
      @o.initial_changed = 'ic'
      @o.column_changed?(:initial_changed).should == false
      @o.missing_changed = nil
      @o.column_changed?(:missing_changed).should == false
    end

    it "changed_columns should handle case where initial value is reassigned later" do
      @o.changed_columns.should == [:initial_changed, :missing_changed]
      @o.initial_changed = 'ic'
      @o.changed_columns.should == [:missing_changed]
      @o.missing_changed = nil
      @o.changed_columns.should == [:missing_changed]
    end

    it "column_changes should give initial and current values" do
      @o.column_changes.should == {:initial_changed=>['ic', 'ic2'], :missing_changed=>[nil, 'mc2']}
    end

    it "reset_column should reset the column to its initial value" do
      @o.reset_column(:initial)
      @o.initial.should == 'i'
      @o.reset_column(:initial_changed)
      @o.initial_changed.should == 'ic'
      @o.reset_column(:missing)
      @o.missing.should == nil
      @o.reset_column(:missing_changed)
      @o.missing_changed.should == nil
    end

    it "reset_column should remove missing values from the values" do
      @o.reset_column(:missing)
      @o.values.has_key?(:missing).should == false
      @o.reset_column(:missing_changed)
      @o.values.has_key?(:missing_changed).should == false
    end
    
    it "refresh should clear the cached initial values" do
      @o.refresh
      @o.column_changes.should == {}
    end
    
    it "will_change_column should be used to signal in-place modification to column" do
      @o.will_change_column(:initial)
      @o.initial << 'b'
      @o.column_change(:initial).should == ['i', 'ib']
      @o.will_change_column(:initial_changed)
      @o.initial_changed << 'b'
      @o.column_change(:initial_changed).should == ['ic', 'ic2b']
      @o.will_change_column(:missing)
      @o.values[:missing] = 'b'
      @o.column_change(:missing).should == [nil, 'b']
      @o.will_change_column(:missing_changed)
      @o.missing_changed << 'b'
      @o.column_change(:missing_changed).should == [nil, 'mc2b']
    end

    it "will_change_column should different types of existing objects" do
      [nil, true, false, Class.new{undef_method :clone}.new, Class.new{def clone; raise TypeError; end}.new].each do |v|
        o = @c.new(:initial=>v)
        o.will_change_column(:initial)
        o.initial = 'a'
        o.column_change(:initial).should == [v, 'a']
      end
    end

    it "save should clear the cached initial values" do
      @o.save
      @o.column_changes.should == {}
    end

    it "save_changes should clear the cached initial values" do
      @o.save_changes
      @o.column_changes.should == {}
    end
  end

  describe "with new instance" do
    before do
      @o = @c.new(:initial=>'i', :initial_changed=>'ic')
      @o.initial_changed = 'ic2'
      @o.missing_changed = 'mc2'
    end

    it_should_behave_like "dirty plugin"
  end

  describe "with existing instance" do
    before do
      @o = @c[1]
      @o.initial_changed = 'ic2'
      @o.missing_changed = 'mc2'
    end

    it_should_behave_like "dirty plugin"

    it "previous_changes should be the previous changes after saving" do
      @o.save
      @o.previous_changes.should == {:initial_changed=>['ic', 'ic2'], :missing_changed=>[nil, 'mc2']}
    end
  end
end
