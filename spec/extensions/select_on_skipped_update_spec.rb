# frozen_string_literal: true
require_relative "spec_helper"

describe "select_on_skipped_update plugin" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:people]))
    @c.columns :id, :name, :num
    @c.plugin :select_on_skipped_update
    @c.require_modification = true
    @p = @c.load(:id=>1, :name=>'John', :num=>1)
    @db.sqls
  end

  it "should issue a SELECT query if calling save_changes on unmodified object" do
    @db.fetch = {:count=>1}
    @p.save_changes
    @db.sqls.must_equal ["SELECT count(*) AS count FROM people WHERE (id = 1) LIMIT 1"]
    @db.fetch = {:count=>0}
    proc{@p.save_changes}.must_raise Sequel::NoMatchingRow
    @db.sqls.must_equal ["SELECT count(*) AS count FROM people WHERE (id = 1) LIMIT 1"]
  end 

  it "should issue a SELECT query if calling save with no columns to update" do
    @db.fetch = {:count=>1}
    @p.save(columns: [])
    @db.sqls.must_equal ["SELECT count(*) AS count FROM people WHERE (id = 1) LIMIT 1"]
    @db.fetch = {:count=>0}
    proc{@p.save(columns: [])}.must_raise Sequel::NoMatchingRow
    @db.sqls.must_equal ["SELECT count(*) AS count FROM people WHERE (id = 1) LIMIT 1"]
  end 
  
  it "should skip SELECT query if object doesn't require modification" do
    @p.require_modification = false
    @p.save_changes
    @p.save(columns: [])
    @db.sqls.must_equal []
  end 
  
  it "should skip SELECT query if the UPDATE query was ran" do
    @db.numrows = 1
    @p.name = 'Bob'
    @p.save_changes
    @db.sqls.must_equal ["UPDATE people SET name = 'Bob' WHERE (id = 1)"]
    @p.name = 'Jim'
    @p.save(columns: [:name])
    @db.sqls.must_equal ["UPDATE people SET name = 'Jim' WHERE (id = 1)"]
  end 

  it "works with filters set by the instance_filters plugin" do
    @c.plugin :instance_filters
    @db.fetch = {:count=>1}
    @p.instance_filter{{name: 'a'}}
    @p.save_changes
    @db.sqls.must_equal ["SELECT count(*) AS count FROM people WHERE ((id = 1) AND (name = 'a')) LIMIT 1"]
    @db.fetch = {:count=>0}
    @p.instance_filter{{name: 'a'}}
    proc{@p.save_changes}.must_raise Sequel::NoMatchingRow
    @db.sqls.must_equal ["SELECT count(*) AS count FROM people WHERE ((id = 1) AND (name = 'a')) LIMIT 1"]
  end 
end
