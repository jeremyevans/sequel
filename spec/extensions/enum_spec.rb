require_relative "spec_helper"

describe "Sequel enum plugin" do
  before do
    @Album = Class.new(Sequel::Model(Sequel.mock[:albums]))
    @Album.columns :id, :status_id
    @Album.plugin :enum
    @Album.enum :status_id, :good=>3, :bad=>5
    @album = @Album.load(:status_id=>3)
  end

  it "should add enum_value! and enum_value? methods for setting/checking the enum values" do
    @album.good?.must_equal true
    @album.bad?.must_equal false

    @album.bad!.must_be_nil
    @album.good?.must_equal false
    @album.bad?.must_equal true

    @album.good!.must_be_nil
    @album.good?.must_equal true
    @album.bad?.must_equal false
  end

  it "should have column method convert to enum value if possible" do
    @album.status_id.must_equal :good
    @album.bad!
    @album.status_id.must_equal :bad
    @album[:status_id] = 3
    @album.status_id.must_equal :good
  end

  it "should have the column method pass non-enum values through" do
    @album[:status_id] = 4
    @album.status_id.must_equal 4
  end

  it "should have column= handle enum values" do
    @album.status_id = :bad
    @album[:status_id].must_equal 5
    @album.good?.must_equal false
    @album.bad?.must_equal true

    @album.status_id = :good
    @album[:status_id].must_equal 3
    @album.good?.must_equal true
    @album.bad?.must_equal false
  end

  it "should have column= handle non-enum values" do
    @album.status_id = 5
    @album[:status_id].must_equal 5
    @album.good?.must_equal false
    @album.bad?.must_equal true
  end

  it "should setup dataset methods for each value" do
    ds = @Album.where(:id=>1)
    ds.good.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id = 3))"
    ds.not_good.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id != 3))"
    ds.bad.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id = 5))"
    ds.not_bad.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id != 5))"
  end
end

describe "Sequel enum plugin" do
  before do
    @Album = Class.new(Sequel::Model(Sequel.mock[:albums]))
    @Album.columns :id, :status_id
    @Album.plugin :enum
    @album = @Album.load(:status_id=>3)
  end

  it "should handle value as an array" do
    @Album.enum :status_id, {:good=>[3, nil], :bad=>5}
    @album.good?.must_equal true
    @album.bad?.must_equal false

    @album.status_id = nil
    @album.good?.must_equal true
    @album.bad?.must_equal false

    @album.status_id = :good
    @album[:status_id].must_equal 3

    ds = @Album.where(:id=>1)
    ds.good.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id IN (3, NULL)))"
    ds.bad.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id = 5))"
  end

  it "should allow overriding methods in class and calling super" do
    @Album.enum :status_id, {:good=>3, :bad=>5}, :override_accessors=>false
    bad = nil
    @Album.class_eval do
      define_method(:bad?) do
        bad.nil? ? super() : bad
      end
    end

    @album.bad?.must_equal false
    bad = true
    @album.bad?.must_equal true
    bad = false
    @album.bad?.must_equal false
    bad = nil
    @album.bad!
    @album.bad?.must_equal true
  end

  it "should not override accessor methods for each value if :override_accessors option is false" do
    @Album.enum :status_id, {:good=>3, :bad=>5}, :override_accessors=>false
    @album.status_id.must_equal 3
    @album.status_id = :bad
    @album.status_id.must_equal :bad
    @album.bad!
    @album.status_id.must_equal 5
  end

  it "should not setup dataset methods for each value if :dataset_methods option is false" do
    @Album.enum :status_id, {:good=>3, :bad=>5}, :dataset_methods=>false
    ds = @Album.where(:id=>1)
    ds.wont_respond_to(:good)
    ds.wont_respond_to(:not_good)
    ds.wont_respond_to(:bad)
    ds.wont_respond_to(:not_bad)
  end

  it "should handle :prefix=>true option" do
    @Album.enum :status_id, {:good=>3, :bad=>5}, :prefix=>true

    @album.status_id_good?.must_equal true
    @album.status_id_bad!
    @album.status_id_bad?.must_equal true

    ds = @Album.where(:id=>1)
    ds.status_id_good.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id = 3))"
    ds.status_id_not_good.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id != 3))"
  end

  it "should handle :prefix=>string option" do
    @Album.enum :status_id, {:good=>3, :bad=>5}, :prefix=>'status'

    @album.status_good?.must_equal true
    @album.status_bad!
    @album.status_bad?.must_equal true

    ds = @Album.where(:id=>1)
    ds.status_good.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id = 3))"
    ds.status_not_good.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id != 3))"
  end

  it "should handle :suffix=>true option" do
    @Album.enum :status_id, {:good=>3, :bad=>5}, :suffix=>true

    @album.good_status_id?.must_equal true
    @album.bad_status_id!
    @album.bad_status_id?.must_equal true

    ds = @Album.where(:id=>1)
    ds.good_status_id.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id = 3))"
    ds.not_good_status_id.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id != 3))"
  end

  it "should handle :suffix=>true option" do
    @Album.enum :status_id, {:good=>3, :bad=>5}, :suffix=>'status'

    @album.good_status?.must_equal true
    @album.bad_status!
    @album.bad_status?.must_equal true

    ds = @Album.where(:id=>1)
    ds.good_status.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id = 3))"
    ds.not_good_status.sql.must_equal "SELECT * FROM albums WHERE ((id = 1) AND (status_id != 3))"
  end

  it "should support multiple emums per class" do 
    @Album.enum :id, {:odd=>1, :even=>2}
    @Album.enum :status_id, {:good=>3, :bad=>5}
    @album = @Album.load(:id=>1, :status_id=>3)
    @album.odd?.must_equal true
    @album.even?.must_equal false
    @album.good?.must_equal true
    @album.bad?.must_equal false
  end

  it "raises Error for column that isn't a symbol" do
    proc{@Album.enum 'status_id', :good=>3, :bad=>5}.must_raise Sequel::Error
  end

  it "raises Error for non-hash values" do
    proc{@Album.enum :status_id, [:good, :bad]}.must_raise Sequel::Error
  end

  it "raises Error for values hash with non-symbol keys" do
    proc{@Album.enum :status_id, 'good'=>3, :bad=>5}.must_raise Sequel::Error
  end
end
