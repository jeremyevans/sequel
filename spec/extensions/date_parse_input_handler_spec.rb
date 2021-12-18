require_relative "spec_helper"

describe "datetime_parse_to_time extension" do
  after do
    # Can't undo the adding of the module to Sequel, so removing the
    # method in Sequel is the only way to restore behavior. This
    # won't break anything, since it will fallback to the implementation
    # in Sequel::DateParseInputHandler
    Sequel.singleton_class.send(:remove_method, :handle_date_parse_input)
  end
  
  it "should be called by Sequel.string_to_*" do
    Sequel.database_to_application_timestamp("2020-11-12 10:20:30").must_equal Time.local(2020, 11, 12, 10, 20, 30)

    Sequel.extension :date_parse_input_handler
    Sequel.date_parse_input_handler do |string|
      raise Sequel::InvalidValue if string.bytesize > 128
      "2020-" + string
    end

    small = "11-12 10:20:30" + " " * 100
    Sequel.string_to_date(small).must_equal Date.new(2020, 11, 12)
    Sequel.string_to_datetime(small).must_equal Time.local(2020, 11, 12, 10, 20, 30)
    Sequel.string_to_time(small).strftime("%H %M %S").must_equal "10 20 30"
    Sequel.send(:_date_parse, small).must_equal(:hour=>10, :min=>20, :sec=>30, :year=>2020, :mon=>11, :mday=>12)

    large = "11-12 10:20:30" + " " * 128
    proc{Sequel.string_to_date(large)}.must_raise Sequel::InvalidValue
    proc{Sequel.string_to_datetime(large)}.must_raise Sequel::InvalidValue
    proc{Sequel.string_to_time(large)}.must_raise Sequel::InvalidValue
    proc{Sequel.send(:_date_parse, large)}.must_raise Sequel::InvalidValue

    Sequel.date_parse_input_handler do |string|
      string
    end

    small = "2020-11-12 10:20:30"
    Sequel.string_to_date(small).must_equal Date.new(2020, 11, 12)
    Sequel.string_to_datetime(small).must_equal Time.local(2020, 11, 12, 10, 20, 30)
    Sequel.string_to_time(small).strftime("%H %M %S").must_equal "10 20 30"
    Sequel.send(:_date_parse, small).must_equal(:hour=>10, :min=>20, :sec=>30, :year=>2020, :mon=>11, :mday=>12)
  end
end
