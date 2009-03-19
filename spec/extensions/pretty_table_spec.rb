require File.join(File.dirname(__FILE__), 'spec_helper')

require 'stringio'

context "Dataset#print" do
  setup do
    @output = StringIO.new
    @orig_stdout = $stdout
    $stdout = @output
    @dataset = Sequel::Dataset.new(nil).from(:items)
    def @dataset.fetch_rows(sql)
      yield({:a=>1, :b=>2})
      yield({:a=>3, :b=>4})
      yield({:a=>5, :b=>6})
    end
  end

  teardown do
    $stdout = @orig_stdout
  end

  specify "should print out a table with the values" do
    @dataset.print(:a, :b)
    @output.rewind
    @output.read.should == \
      "+-+-+\n|a|b|\n+-+-+\n|1|2|\n|3|4|\n|5|6|\n+-+-+\n"
  end

  specify "should default to the dataset's columns" do
    @dataset.meta_def(:columns) {[:a, :b]}
    @dataset.print
    @output.rewind
    @output.read.should == \
      "+-+-+\n|a|b|\n+-+-+\n|1|2|\n|3|4|\n|5|6|\n+-+-+\n"
  end
end

context "PrettyTable" do
  setup do
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

  teardown do
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
