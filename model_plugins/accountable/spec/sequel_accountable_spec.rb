require File.join(File.dirname(__FILE__), "spec_helper")

# Assumptions:
# * You have a concept of a 'current_user' who has an id.

describe Sequel::Plugins::Accountable do

  it "should introduce a created_by field to the schema"

  it "should introduce a updated_by field to the schema"

  it "should introduce a deleted_by field to the schema, if the :deleted => true option is specified"

  it "should not introduce a deleted_by field to the schema, if the :deleted option is not specified"

  it "should introduce a deleted_at field to the schema, if the :deleted => true option is specified"

  it "should not introduce a deleted_at field to the schema, if the :deleted option is not specified"

  it "should allow customizatin of the current_user's id" do

    pending("Figure out which method makes the most sense for getting the current_user.id and allow for customization of the user id field")

  end

end

describe Sequel::Plugins::Accountable, "#created_by" do

  it "should set created_by to the current_user.id on INSERT"

  it "should set updated_by to to the current_user.id on INSERT"

end

describe Sequel::Plugins::Accountable, "#created_at" do

  it "should set created_at to Time.now on INSERT"

  it "should set updated_at to Time.now on INSERT"

end

describe Sequel::Plugins::Accountable, "#updated_by" do

  it "should set updated_by to current_user.id upon UPDATE"

end

describe Sequel::Plugins::Accountable, "#updated_at" do

  it "should set updated_at to Time.now upon UPDATE"

end

describe Sequel::Plugins::Accountable, "when option :deleted => true" do

  it "should set deleted_by to current_user.id upon DELETE"

  it "should not delete the record"

  it "should set deleted_at to Time.now upon DELETE"

  it "should not delete the record"

end
