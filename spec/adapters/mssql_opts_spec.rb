require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

if RUBY_PLATFORM =~ /java/
  describe "JDBC" do
    context "mssql_unicode_strings is default/true" do
      let(:db) { Sequel.jdbc("jdbc:sqlserver://localhost") }
      subject  { db[:tb1].filter(:col1 => "test").sql }

      it { should == "SELECT * FROM TB1 WHERE (COL1 = N'test')" }
    end

    context "mssql_unicode_strings is false" do
      let(:db) { Sequel.jdbc("jdbc:sqlserver://localhost",
                            :mssql_unicode_strings => false) }
      subject  { db[:tb1].filter(:col1 => "test").sql }

      it { should == "SELECT * FROM TB1 WHERE (COL1 = 'test')" }
    end
  end
else
  describe "ADO" do
    context "mssql_unicode_strings is default/true" do
      let(:db) { Sequel.ado(:conn_string => "dummy connection string") }
      subject  { db[:tb1].filter(:col1 => "test").sql }

      it { should == "SELECT * FROM TB1 WHERE (COL1 = N'test')" }
    end

    context "mssql_unicode_strings is false" do
      let(:db) { Sequel.ado(:conn_string => "dummy connection string",
                            :mssql_unicode_strings => false) }
      subject  { db[:tb1].filter(:col1 => "test").sql }

      it { should == "SELECT * FROM TB1 WHERE (COL1 = 'test')" }
    end
  end

  describe "ODBC" do
    context "mssql_unicode_strings is default/true" do
      let(:db) { Sequel.odbc("dummy", :db_type => "mssql") }
      subject  { db[:tb1].filter(:col1 => "test").sql }

      it { should == "SELECT * FROM TB1 WHERE (COL1 = N'test')" }
    end

    context "mssql_unicode_strings is false" do
      let(:db) { Sequel.odbc("dummy", :db_type => "mssql",
                            :mssql_unicode_strings => false) }
      subject  { db[:tb1].filter(:col1 => "test").sql }

      it { should == "SELECT * FROM TB1 WHERE (COL1 = 'test')" }
    end
  end
end
