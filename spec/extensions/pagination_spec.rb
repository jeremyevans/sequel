require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

context "A paginated dataset" do
  before do
    @d = Sequel::Dataset.new(nil)
    @d.meta_def(:count) {153}
    
    @paginated = @d.paginate(1, 20)
  end
  
  specify "should raise an error if the dataset already has a limit" do
    proc{@d.limit(10).paginate(1,10)}.should raise_error(Sequel::Error)
    proc{@paginated.paginate(2,20)}.should raise_error(Sequel::Error)
  end
  
  specify "should set the limit and offset options correctly" do
    @paginated.opts[:limit].should == 20
    @paginated.opts[:offset].should == 0
  end
  
  specify "should set the page count correctly" do
    @paginated.page_count.should == 8
    @d.paginate(1, 50).page_count.should == 4
  end
  
  specify "should set the current page number correctly" do
    @paginated.current_page.should == 1
    @d.paginate(3, 50).current_page.should == 3
  end
  
  specify "should return the next page number or nil if we're on the last" do
    @paginated.next_page.should == 2
    @d.paginate(4, 50).next_page.should be_nil
  end
  
  specify "should return the previous page number or nil if we're on the first" do
    @paginated.prev_page.should be_nil
    @d.paginate(4, 50).prev_page.should == 3
  end
  
  specify "should return the page range" do
    @paginated.page_range.should == (1..8)
    @d.paginate(4, 50).page_range.should == (1..4)
  end
  
  specify "should return the record range for the current page" do
    @paginated.current_page_record_range.should == (1..20)
    @d.paginate(4, 50).current_page_record_range.should == (151..153)
    @d.paginate(5, 50).current_page_record_range.should == (0..0)
  end

  specify "should return the record count for the current page" do
    @paginated.current_page_record_count.should == 20
    @d.paginate(3, 50).current_page_record_count.should == 50
    @d.paginate(4, 50).current_page_record_count.should == 3
    @d.paginate(5, 50).current_page_record_count.should == 0
  end

  specify "should know if current page is last page" do
    @paginated.last_page?.should be_false
    @d.paginate(2, 20).last_page?.should be_false
    @d.paginate(5, 30).last_page?.should be_false
    @d.paginate(6, 30).last_page?.should be_true
  end

  specify "should know if current page is first page" do
    @paginated.first_page?.should be_true
    @d.paginate(1, 20).first_page?.should be_true
    @d.paginate(2, 20).first_page?.should be_false
  end

  specify "should work with fixed sql" do
    ds = @d.clone(:sql => 'select * from blah')
    ds.meta_def(:count) {150}
    ds.paginate(2, 50).sql.should == 'SELECT * FROM (select * from blah) AS t1 LIMIT 50 OFFSET 50'
  end
end

context "Dataset#each_page" do
  before do
    @d = Sequel::Dataset.new(nil).from(:items)
    @d.meta_def(:count) {153}
  end
  
  specify "should raise an error if the dataset already has a limit" do
    proc{@d.limit(10).each_page(10){}}.should raise_error(Sequel::Error)
  end
  
  specify "should iterate over each page in the resultset as a paginated dataset" do
    a = []
    @d.each_page(50) {|p| a << p}
    a.map {|p| p.sql}.should == [
      'SELECT * FROM items LIMIT 50 OFFSET 0',
      'SELECT * FROM items LIMIT 50 OFFSET 50',
      'SELECT * FROM items LIMIT 50 OFFSET 100',
      'SELECT * FROM items LIMIT 50 OFFSET 150',
    ]
  end
end
