require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

begin
  require 'active_model'
  require 'test/unit'
  if Test::Unit.respond_to?(:run=)
    Test::Unit.run = false
    require 'test/unit/testresult'
  elsif defined?(MiniTest::Unit)
    class << MiniTest::Unit
      def autorun; end
    end
  end
rescue LoadError => e
  skip_warn "active_model plugin: can't load active_model (#{e.class}: #{e})"
else
describe "ActiveModel plugin" do
  specify "should be compliant to the ActiveModel spec" do
    tc = Class.new(Test::Unit::TestCase)
    tc.class_eval do
      define_method(:setup) do
        class ::AMLintTest < Sequel::Model
          set_primary_key :id
          columns :id, :id2
          def delete; end
        end
        @c = AMLintTest
        @c.plugin :active_model
        @m = @model = @c.new
        @o = @c.load({})
        super()
      end
      def teardown
        super
        Object.send(:remove_const, :AMLintTest)
      end
      include ActiveModel::Lint::Tests

      # Should return self, not a proxy object
      def test__to_model
        assert_equal @m.to_model.object_id.should, @m.object_id
      end
      
      def test__to_key
        assert_equal nil, @m.to_key
        @o.id = 1
        assert_equal [1], @o.to_key
        @o.id = nil
        assert_equal nil, @o.to_key

        @c.set_primary_key [:id2, :id]
        assert_equal nil, @o.to_key
        @o.id = 1
        @o.id2 = 2
        assert_equal [2, 1], @o.to_key
        @o.destroy
        assert_equal [2, 1], @o.to_key
        @o.id = nil
        assert_equal nil, @o.to_key
      end
      
      def test__to_param
        assert_equal nil, @m.to_param
        @o.id = 1
        assert_equal '1', @o.to_param
        @c.set_primary_key [:id2, :id]
        @o.id2 = 2
        assert_equal '2-1', @o.to_param
        @o.meta_def(:to_param_joiner){'|'}
        assert_equal '2|1', @o.to_param
        @o.destroy
        assert_equal nil, @o.to_param
      end

      def test__persisted?
        assert_equal false, @m.persisted?
        assert_equal true, @o.persisted?
        @m.destroy
        @o.destroy
        assert_equal false, @m.persisted?
        assert_equal false, @o.persisted?
      end
    end
    if defined?(MiniTest::Unit)
      tc.instance_methods.map{|x| x.to_s}.reject{|n| n !~ /\Atest_/}.each do |m|
        i = tc.new(m)
        i.setup
        i.send(m)
        i.teardown
      end
    else
      res = ::Test::Unit::TestResult.new
      tc.suite.run(res){}
      if res.failure_count > 0
        puts res.instance_variable_get(:@failures)
      end
      res.failure_count.should == 0
    end
  end
end 
end
