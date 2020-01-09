require_relative "spec_helper"

describe "empty_failure_backtraces plugin" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      plugin :empty_failure_backtraces
      columns :x
      set_primary_key :x
      unrestrict_primary_key
      def before_create
        super
        cancel_action 'bc' if x == 2
      end
      def before_destroy
        super
        cancel_action 'bd' if x == 2
      end
      def validate
        super
        errors.add(:x, "3") if x == 3
      end
    end
    DB.reset
  end
  
  it "should work normally if no exceptions are thrown/raised" do
    o = @c.create(:x=>1)
    o.must_be_kind_of @c
    o.valid?.must_equal true
    o.destroy.must_equal o
  end

  it "should work normally when not rescuing exceptions internally when calling save" do
    @c.new.set(:x => 2).save(:raise_on_failure=>false).must_be_nil
    @c.raise_on_save_failure = false
    @c.create(:x => 2).must_be_nil
    @c.load(:x => 2).destroy(:raise_on_failure=>false).must_be_nil
  end
    
  it "should work normally when not rescuing exceptions internally when calling valid?" do
    @c.send(:define_method, :before_validation){cancel_action "bv"}
    @c.new(:x => 2).valid?.must_equal false
  end

  it "should raise exceptions with empty backtraces" do
    begin
      @c.create(:x => 2)
    rescue Sequel::HookFailed => e 
      e.backtrace.must_be_empty
      1
    end.must_equal 1

    begin
      @c.create(:x => 3)
    rescue Sequel::ValidationFailed => e 
      e.backtrace.must_be_empty
      1
    end.must_equal 1
  end
end
