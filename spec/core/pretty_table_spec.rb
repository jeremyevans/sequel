require File.join(File.dirname(__FILE__), 'spec_helper')

require 'stringio'

context "PrettyTable" do
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
      {:c => 3}
    ]

    @output = StringIO.new
    @orig_stdout = $stdout
    $stdout = @output
  end

  after do
    $stdout = @orig_stdout
  end
  
  specify "should infer the columns if not given" do
    Sequel::PrettyTable.print(@data1)
    @output.rewind
    @output.read.should =~ \
      /\n(\|x\|y\|)|(\|y\|x\|)\n/
  end
  
  specify "should calculate the maximum width of each column correctly" do
    Sequel::PrettyTable.print(@data2, [:a, :b])
    @output.rewind
    @output.read.should == \
      "+--+----+\n|a |b   |\n+--+----+\n|23|  45|\n|45|2377|\n+--+----+\n"
  end

  specify "should also take header width into account" do
    Sequel::PrettyTable.print(@data3, [:aaa, :bb, :c])
    @output.rewind
    @output.read.should == \
      "+---+--+-+\n|aaa|bb|c|\n+---+--+-+\n|  1|  | |\n|   | 2| |\n|   |  |3|\n+---+--+-+\n"
  end
  
  specify "should print only the specified columns" do
    Sequel::PrettyTable.print(@data2, [:a])
    @output.rewind
    @output.read.should == \
      "+--+\n|a |\n+--+\n|23|\n|45|\n+--+\n"
  end
end
