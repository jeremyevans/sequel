require_relative "spec_helper"

require 'stringio'

describe "stdio_logger extension: Sequel::StdioLogger" do
  before do
    Sequel.extension :stdio_logger
    @output = StringIO.new
    @logger = Sequel::StdioLogger.new(@output)
  end

  it "#debug should not log" do
    @logger.debug("foo").must_be_nil
    @output.rewind
    @output.read.must_equal ''
  end

  [:info, :warn, :error].each do |level|
    it "##{level} should log message with given level" do
      @logger.send(level, "foo").must_be_nil
      @output.rewind
      @output.read.must_match(/ #{level.to_s.upcase}: foo\n\z/)
    end
  end
end
