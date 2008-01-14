require File.join(File.dirname(__FILE__), "spec_helper")

# it "should save a new object with the old data, and save the new data in a new object"
#...

describe Sequel::Plugins::Versioned do

  it "should allow for customization of the schema field"
  
  it "should start at version 1 after create"

  it "should create a new record with the new data and an incremented 'version' number on update(save)"

  it "should update an existing record when :update => false is used" do
    pending("Have to figure out the exact interface for this.")
  end

  # race condition
  # using the dates totally avoids race conditions in which
  # two authors of the same thing save at nearly same time
  # second saver should gets an error
  # The new record gets timestamped with the time they load revision they are working on. When they go to save we can check if there is a newer created_at for the 'record'.
  it "should return an error if the record has been saved while editing the record and this record is saved"
    # Note, use query on created_at to find records that were saved in between
    pending("Clarify this description lolz")
  end

end
