require_relative "spec_helper"

describe "Skip Saving Generated Columns" do
  before do
    User = Class.new(Sequel::Model(:users))
    Donation = Class.new(Sequel::Model(:donations))

    User.class_eval do
      columns :id
      @db_schema = {
        :id=>{:type=>:integer}
      }
      one_to_many :donations, :class=>Donation
    end

    Donation.class_eval do
      plugin :skip_saving_generated_columns
      columns :id, :user_id, :name, :search
      @db_schema = {
        :id=>{:type=>:integer},
        :user_id=>{:type=>:integer},
        :search=>{:type=>:text, :generated=>true}
      }
      many_to_one :user, :class=>User
      db.reset
    end
  end

  after do
    [:User, :Donation].each { |s| Object.send(:remove_const, s) if Object.const_defined?(s) }
  end

  it "should not include the search columns when assigning to a user" do
    u = User.load(id: 1)
    d = Donation.load(id: 2, search: 'search data')
    u.add_donation(d)
    Donation.db.sqls.must_equal ["UPDATE donations SET user_id = 1 WHERE (id = 2)"]
  end
end
