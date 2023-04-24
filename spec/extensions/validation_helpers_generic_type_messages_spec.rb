require_relative "spec_helper"

describe "Sequel::Plugins::ValidationHelpers" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db)) do
      def self.set_validations(&block)
        define_method(:validate, &block)
        alias_method(:validate, :validate)
      end
      columns :value
    end
    @c.plugin :validation_helpers_generic_type_messages
    @m = @c.new
  end

  it "should generic validation error messages for validates_schema_types" do
    @c.set_validations{validates_schema_types}
    set_type = lambda do |type|
      @m.define_singleton_method(:db_schema){{:value=>{:type=>type}}}
      @m.singleton_class.send(:alias_method, :db_schema, :db_schema)
    end

    @m.value = 123
    @m.must_be :valid?
    @m.value = '123'
    @m.must_be :valid?

    set_type.call(:integer)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not an integer']

    set_type.call(:string)
    @m.must_be :valid?
    @m.values[:value] = 123
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not a string']

    set_type.call(:date)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not a valid date']

    set_type.call(:datetime)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not a valid timestamp']

    set_type.call(:time)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not a valid time']

    set_type.call(:float)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not a number']

    set_type.call(:decimal)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not a number']

    set_type.call(:boolean)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not true or false']

    set_type.call(:blob)
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not a blob']

    set_type.call(:foo)
    def @db.schema_type_class(x)
      x == :foo ? Hash : super 
    end
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is not the expected type']

    @c.set_validations{validates_schema_types(:value, :message=>'is bad')}
    @m.wont_be :valid?
    @m.errors.full_messages.must_equal ['value is bad']
  end
end
