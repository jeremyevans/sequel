require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(ADO_DB)
  ADO_DB = Sequel.connect(:adapter => 'ado', :driver => "{Microsoft Access Driver (*.mdb)}; DBQ=c:\\Nwind.mdb")
end

context "An ADO dataset" do
  setup do
    ADO_DB.create_table!(:items) { text :name }
  end

  specify "should not raise exceptions when working with empty datasets" do
    lambda {
      ADO_DB[:items].all
    }.should_not raise_error
  end
end
