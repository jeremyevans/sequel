require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

require 'stringio'
Sequel.extension :pretty_table

describe "Dataset#print" do
  before do
    @output = StringIO.new
    @orig_stdout = $stdout
    $stdout = @output
    @dataset = Sequel.mock(:fetch=>[{:a=>1, :b=>2}, {:a=>3, :b=>4}, {:a=>5, :b=>6}])[:items].extension(:pretty_table)
  end

  after do
    $stdout = @orig_stdout
  end

  it "should print out a table with the values" do
    @dataset.print(:a, :b)
    @output.rewind
    @output.read.must_equal \
      "+-+-+\n|a|b|\n+-+-+\n|1|2|\n|3|4|\n|5|6|\n+-+-+\n"
  end

  it "should default to the dataset's columns" do
    @dataset.meta_def(:columns) {[:a, :b]}
    @dataset.print
    @output.rewind
    @output.read.must_equal \
      "+-+-+\n|a|b|\n+-+-+\n|1|2|\n|3|4|\n|5|6|\n+-+-+\n"
  end
end

describe "PrettyTable" do
  before do
    @data1 = [
      {:x => 3, :y => 4}
    ]
    
    @data2 = [
      {:a => 23, :b => 45},
      {:a => 45, :b => 2377}
    ]

    @data3 = [
      {:aaa => 1},
      {:bb => 2},
      {:c => 3.1}
    ]

    @output = StringIO.new
    @orig_stdout = $stdout
    $stdout = @output
  end

  after do
    $stdout = @orig_stdout
  end
  
  it "should infer the columns if not given" do
    Sequel::PrettyTable.print(@data1)
    @output.rewind
    @output.read.must_match(/\n(\|x\|y\|)|(\|y\|x\|)\n/)
  end
  
  it "should have #string return the string without printing" do
    Sequel::PrettyTable.string(@data1).must_match(/\n(\|x\|y\|)|(\|y\|x\|)\n/)
    @output.rewind
    @output.read.must_equal ''
  end
  
  it "should calculate the maximum width of each column correctly" do
    Sequel::PrettyTable.print(@data2, [:a, :b])
    @output.rewind
    @output.read.must_equal \
      "+--+----+\n|a |b   |\n+--+----+\n|23|  45|\n|45|2377|\n+--+----+\n"
  end

  it "should also take header width into account" do
    Sequel::PrettyTable.print(@data3, [:aaa, :bb, :c])
    @output.rewind
    @output.read.must_equal \
      "+---+--+---+\n|aaa|bb|c  |\n+---+--+---+\n|  1|  |   |\n|   | 2|   |\n|   |  |3.1|\n+---+--+---+\n"
  end
  
  it "should print only the specified columns" do
    Sequel::PrettyTable.print(@data2, [:a])
    @output.rewind
    @output.read.must_equal \
      "+--+\n|a |\n+--+\n|23|\n|45|\n+--+\n"
  end
end
