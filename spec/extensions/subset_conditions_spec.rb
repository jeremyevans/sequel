require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "subset_conditions plugin" do
  before do
    @c = Class.new(Sequel::Model(:a))
    @c.plugin :subset_conditions
  end

  it "should provide *_conditions method return the arguments passed" do
    @c.subset(:published, :published => true)
    @c.where(@c.published_conditions).sql.must_equal @c.published.sql

    @c.subset(:active, :active)
    @c.where(@c.active_conditions).sql.must_equal @c.active.sql

    @c.subset(:active_published, :active, :published => true)
    @c.where(@c.active_published_conditions).sql.must_equal @c.active_published.sql
    @c.where(Sequel.&(@c.active_conditions, @c.published_conditions)).sql.must_equal @c.active_published.sql
    @c.where(Sequel.|(@c.active_conditions, @c.published_conditions)).sql.must_equal "SELECT * FROM a WHERE (active OR (published IS TRUE))"
    @c.where(Sequel.|(@c.active_published_conditions, :foo)).sql.must_equal "SELECT * FROM a WHERE ((active AND (published IS TRUE)) OR foo)"
  end

  it "should work with blocks" do
    p1 = proc{{:published=>true}}
    @c.subset(:published, &p1)
    @c.where(@c.published_conditions).sql.must_equal @c.published.sql

    p2 = proc{:active}
    @c.subset(:active, &p2)
    @c.where(@c.active_conditions).sql.must_equal @c.active.sql

    @c.subset(:active_published, p2, &p1)
    @c.where(@c.active_published_conditions).sql.must_equal @c.active_published.sql
    @c.where(Sequel.&(@c.active_conditions, @c.published_conditions)).sql.must_equal @c.active_published.sql
    @c.where(Sequel.|(@c.active_conditions, @c.published_conditions)).sql.must_equal "SELECT * FROM a WHERE (active OR (published IS TRUE))"
    @c.where(Sequel.|(@c.active_published_conditions, :foo)).sql.must_equal "SELECT * FROM a WHERE ((active AND (published IS TRUE)) OR foo)"
  end
end
