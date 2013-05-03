require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

asd = begin
  require 'active_support/duration'
  true
rescue LoadError => e
  skip_warn "date_arithmetic extension (partial): can't load active_support/duration (#{e.class}: #{e})"
  false
end

Sequel.extension :date_arithmetic

describe "date_arithmetic extension" do
  dbf = lambda do |db_type|
    db = Sequel.connect("mock://#{db_type}")
    db.extension :date_arithmetic
    db
  end

  before do
    @h0 = {:days=>0}
    @h1 = {:days=>1, :years=>nil, :hours=>0}
    @h2 = {:years=>1, :months=>1, :days=>1, :hours=>1, :minutes=>1, :seconds=>1}
  end

  it "should have Sequel.date_add with an interval hash return an appropriate Sequel::SQL::DateAdd expression" do
    da = Sequel.date_add(:a, :days=>1)
    da.should be_a_kind_of(Sequel::SQL::DateAdd)
    da.expr.should == :a
    da.interval.should == {:days=>1}
    Sequel.date_add(:a, :years=>1, :months=>2, :days=>3, :hours=>1, :minutes=>1, :seconds=>1).interval.should == {:years=>1, :months=>2, :days=>3, :hours=>1, :minutes=>1, :seconds=>1}
  end

  it "should have Sequel.date_sub with an interval hash return an appropriate Sequel::SQL::DateAdd expression" do
    da = Sequel.date_sub(:a, :days=>1)
    da.should be_a_kind_of(Sequel::SQL::DateAdd)
    da.expr.should == :a
    da.interval.should == {:days=>-1}
    Sequel.date_sub(:a, :years=>1, :months=>2, :days=>3, :hours=>1, :minutes=>1, :seconds=>1).interval.should == {:years=>-1, :months=>-2, :days=>-3, :hours=>-1, :minutes=>-1, :seconds=>-1}
  end

  it "should have Sequel.date_* with an interval hash handle nil values" do
    Sequel.date_sub(:a, :days=>1, :hours=>nil).interval.should == {:days=>-1}
  end

  it "should raise an error if given string values in an interval hash" do
    lambda{Sequel.date_add(:a, :days=>'1')}.should raise_error(Sequel::InvalidValue)
  end

  if asd
    it "should have Sequel.date_add with an ActiveSupport::Duration return an appropriate Sequel::SQL::DateAdd expression" do
      da = Sequel.date_add(:a, ActiveSupport::Duration.new(1, [[:days, 1]]))
      da.should be_a_kind_of(Sequel::SQL::DateAdd)
      da.expr.should == :a
      da.interval.should == {:days=>1}
      Sequel.date_add(:a, ActiveSupport::Duration.new(1, [[:years, 1], [:months, 1], [:days, 1], [:minutes, 61], [:seconds, 1]])).interval.should == {:years=>1, :months=>1, :days=>1, :minutes=>61, :seconds=>1}
    end

    it "should have Sequel.date_sub with an ActiveSupport::Duration return an appropriate Sequel::SQL::DateAdd expression" do
      da = Sequel.date_sub(:a, ActiveSupport::Duration.new(1, [[:days, 1]]))
      da.should be_a_kind_of(Sequel::SQL::DateAdd)
      da.expr.should == :a
      da.interval.should == {:days=>-1}
      Sequel.date_sub(:a, ActiveSupport::Duration.new(1, [[:years, 1], [:months, 1], [:days, 1], [:minutes, 61], [:seconds, 1]])).interval.should == {:years=>-1, :months=>-1, :days=>-1, :minutes=>-61, :seconds=>-1}
    end
  end

  it "should correctly literalize on Postgres" do
    db = dbf.call(:postgres)
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(a AS timestamp)"
    db.literal(Sequel.date_add(:a, @h1)).should == "(CAST(a AS timestamp) + CAST('1 days ' AS interval))"
    db.literal(Sequel.date_add(:a, @h2)).should == "(CAST(a AS timestamp) + CAST('1 years 1 months 1 days 1 hours 1 minutes 1 seconds ' AS interval))"
  end

  it "should correctly literalize on SQLite" do
    db = dbf.call(:sqlite)
    db.literal(Sequel.date_add(:a, @h0)).should == "datetime(a)"
    db.literal(Sequel.date_add(:a, @h1)).should == "datetime(a, '1 days')"
    db.literal(Sequel.date_add(:a, @h2)).should == "datetime(a, '1 years', '1 months', '1 days', '1 hours', '1 minutes', '1 seconds')"
  end

  it "should correctly literalize on MySQL" do
    db = dbf.call(:mysql)
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(a AS DATETIME)"
    db.literal(Sequel.date_add(:a, @h1)).should == "DATE_ADD(a, INTERVAL 1 DAY)"
    db.literal(Sequel.date_add(:a, @h2)).should == "DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(a, INTERVAL 1 YEAR), INTERVAL 1 MONTH), INTERVAL 1 DAY), INTERVAL 1 HOUR), INTERVAL 1 MINUTE), INTERVAL 1 SECOND)"
  end

  it "should correctly literalize on HSQLDB" do
    db = Sequel.mock
    def db.database_type; :hsqldb end
    db.extension :date_arithmetic
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(CAST(a AS timestamp) AS timestamp)"
    db.literal(Sequel.date_add(:a, @h1)).should == "DATE_ADD(CAST(a AS timestamp), INTERVAL 1 DAY)"
    db.literal(Sequel.date_add(:a, @h2)).should == "DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(CAST(a AS timestamp), INTERVAL 1 YEAR), INTERVAL 1 MONTH), INTERVAL 1 DAY), INTERVAL 1 HOUR), INTERVAL 1 MINUTE), INTERVAL 1 SECOND)"
  end

  it "should correctly literalize on MSSQL" do
    db = dbf.call(:mssql)
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(a AS datetime)"
    db.literal(Sequel.date_add(:a, @h1)).should == "DATEADD(day, 1, a)"
    db.literal(Sequel.date_add(:a, @h2)).should == "DATEADD(second, 1, DATEADD(minute, 1, DATEADD(hour, 1, DATEADD(day, 1, DATEADD(month, 1, DATEADD(year, 1, a))))))"
  end

  it "should correctly literalize on H2" do
    db = Sequel.mock
    def db.database_type; :h2 end
    db.extension :date_arithmetic
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(a AS timestamp)"
    db.literal(Sequel.date_add(:a, @h1)).should == "DATEADD('day', 1, a)"
    db.literal(Sequel.date_add(:a, @h2)).should == "DATEADD('second', 1, DATEADD('minute', 1, DATEADD('hour', 1, DATEADD('day', 1, DATEADD('month', 1, DATEADD('year', 1, a))))))"
  end

  it "should correctly literalize on access" do
    db = dbf.call(:access)
    db.literal(Sequel.date_add(:a, @h0)).should == "CDate(a)"
    db.literal(Sequel.date_add(:a, @h1)).should == "DATEADD('d', 1, a)"
    db.literal(Sequel.date_add(:a, @h2)).should == "DATEADD('s', 1, DATEADD('n', 1, DATEADD('h', 1, DATEADD('d', 1, DATEADD('m', 1, DATEADD('yyyy', 1, a))))))"
  end

  it "should correctly literalize on Derby" do
    db = Sequel.mock
    def db.database_type; :derby end
    db.extension :date_arithmetic
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(a AS timestamp)"
    db.literal(Sequel.date_add(:a, @h1)).should == "{fn timestampadd(SQL_TSI_DAY, 1, timestamp(a))}"
    db.literal(Sequel.date_add(:a, @h2)).should == "{fn timestampadd(SQL_TSI_SECOND, 1, timestamp({fn timestampadd(SQL_TSI_MINUTE, 1, timestamp({fn timestampadd(SQL_TSI_HOUR, 1, timestamp({fn timestampadd(SQL_TSI_DAY, 1, timestamp({fn timestampadd(SQL_TSI_MONTH, 1, timestamp({fn timestampadd(SQL_TSI_YEAR, 1, timestamp(a))}))}))}))}))}))}"
    db.literal(Sequel.date_add(Date.civil(2012, 11, 12), @h1)).should == "{fn timestampadd(SQL_TSI_DAY, 1, timestamp((CAST('2012-11-12' AS varchar(255)) || ' 00:00:00')))}"
  end

  it "should correctly literalize on Oracle" do
    db = dbf.call(:oracle)
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(a AS timestamp)"
    db.literal(Sequel.date_add(:a, @h1)).should == "(a + INTERVAL '1' DAY)"
    db.literal(Sequel.date_add(:a, @h2)).should == "(a + INTERVAL '1' YEAR + INTERVAL '1' MONTH + INTERVAL '1' DAY + INTERVAL '1' HOUR + INTERVAL '1' MINUTE + INTERVAL '1' SECOND)"
  end

  it "should correctly literalize on DB2" do
    db = dbf.call(:db2)
    db.literal(Sequel.date_add(:a, @h0)).should == "CAST(a AS timestamp)"
    db.literal(Sequel.date_add(:a, @h1)).should == "(CAST(a AS timestamp) + 1 days)"
    db.literal(Sequel.date_add(:a, @h2)).should == "(CAST(a AS timestamp) + 1 years + 1 months + 1 days + 1 hours + 1 minutes + 1 seconds)"
  end

  qspecify "should raise error if literalizing on an unsupported database" do
    db = Sequel.mock
    db.extension :date_arithmetic
    lambda{db.literal(Sequel.date_add(:a, @h0))}.should raise_error(Sequel::NotImplemented)
  end
end
