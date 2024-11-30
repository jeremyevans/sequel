require_relative "spec_helper"

describe "subset_conditions plugin" do
  before do
    @c = Class.new(Sequel::Model(:a))
    @c.plugin :subset_conditions
  end

  it "should provide *_conditions method return the arguments passed" do
    @c.dataset_module{subset(:published, :published => true)}
    @c.where(@c.published_conditions).sql.must_equal @c.published.sql

    @c.dataset_module{where(:active, :active)}
    @c.where(@c.active_conditions).sql.must_equal @c.active.sql

    @c.dataset_module{exclude(:not_bad, :bad)}
    @c.where(@c.not_bad_conditions).sql.must_equal @c.not_bad.sql

    @c.dataset_module{subset(:active_published, Sequel.&(:active, :published => true))}
    @c.where(@c.active_published_conditions).sql.must_equal @c.active_published.sql
    @c.where(Sequel.&(@c.active_conditions, @c.published_conditions)).sql.must_equal @c.active_published.sql
    @c.where(Sequel.|(@c.active_conditions, @c.published_conditions)).sql.must_equal "SELECT * FROM a WHERE (active OR (published IS TRUE))"
    @c.where(Sequel.|(@c.active_published_conditions, :foo)).sql.must_equal "SELECT * FROM a WHERE ((active AND (published IS TRUE)) OR foo)"

    @c.dataset_module{exclude(:not_x_or_y, :x){:y}}
    @c.where(@c.not_x_or_y_conditions).sql.must_equal @c.not_x_or_y.sql

  end

  it "should work with blocks" do
    p1 = proc{{:published=>true}}
    @c.dataset_module{subset(:published, &p1)}
    @c.where(@c.published_conditions).sql.must_equal @c.published.sql

    p2 = :active
    @c.dataset_module{subset(:active, p2)}
    @c.where(@c.active_conditions).sql.must_equal @c.active.sql

    @c.dataset_module{exclude(:inactive){p2}}
    @c.where(@c.inactive_conditions).sql.must_equal @c.inactive.sql

    @c.dataset_module{subset(:active_published, p2, &p1)}
    @c.where(@c.active_published_conditions).sql.must_equal @c.active_published.sql
    @c.where(Sequel.&(@c.active_conditions, @c.published_conditions)).sql.must_equal @c.active_published.sql
    @c.where(Sequel.|(@c.active_conditions, @c.published_conditions)).sql.must_equal "SELECT * FROM a WHERE (active OR (published IS TRUE))"
    @c.where(Sequel.|(@c.active_published_conditions, :foo)).sql.must_equal "SELECT * FROM a WHERE ((active AND (published IS TRUE)) OR foo)"
  end

  it "should support where_all and where_any for combining subset conditions" do
    @c.dataset_module do
      subset(:published, :published => true)
      where(:active, :active)
      exclude(:not_bad, :bad)

      where_all(:active_all1, :active)
      where_any(:active_any1, :active)
      where_all(:active_and_published, :active, :published)
      where_any(:active_or_published, :active, :published)
      where_all(:active_and_published_and_not_bad, :active, :published, :not_bad)
      where_any(:active_or_published_or_not_bad, :active, :published, :not_bad)
    end

    @c.active_all1.sql.must_equal 'SELECT * FROM a WHERE active'
    @c.active_any1.sql.must_equal 'SELECT * FROM a WHERE active'
    @c.active_and_published.sql.must_equal 'SELECT * FROM a WHERE (active AND (published IS TRUE))'
    @c.active_or_published.sql.must_equal 'SELECT * FROM a WHERE (active OR (published IS TRUE))'
    @c.active_and_published_and_not_bad.sql.must_equal 'SELECT * FROM a WHERE (active AND (published IS TRUE) AND NOT bad)'
    @c.active_or_published_or_not_bad.sql.must_equal 'SELECT * FROM a WHERE (active OR (published IS TRUE) OR NOT bad)'
  end
end
