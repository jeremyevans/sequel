require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_eager_any_typed_array plugin" do
  before do
    @db = Sequel.connect('mock://postgres')
    @c = Class.new(Sequel::Model(@db[:items]))
    def @c.name; "C" end
    @c.columns :id, :c_id
    @c.plugin :pg_eager_any_typed_array
    @c.plugin :many_through_many
    @c.plugin :pg_array_associations
    @c.dataset = @c.dataset.with_fetch([[{:id=>1, :c_id=>2}, {:id=>3, :c_id=>4}]])
    @c.db_schema[:id][:db_type] = "int4"
    @c.db_schema[:c_id][:db_type] = "int8"
    @c.one_to_many :cs, :class=>@c
    @c.many_to_one :c, :class=>@c
    @c.many_to_one :cc, :class=>@c, :key=>[:id, :c_id], :primary_key=>[:id, :c_id]
    @c.one_to_one :first_c, :class=>@c, :key=>:c_id
    @c.many_to_many :mtmcs, :class=>@c, :join_table=>:jt, :left_key=>:c_id, :right_key=>:id, :right_primary_key=>:c_id
    @c.one_through_one :otoc, :class=>@c, :join_table=>:jt, :left_key=>:c_id, :right_key=>:id, :right_primary_key=>:c_id
    @c.many_through_many :mthmcs, [[:jt1, :c_id, :id], [:jt2, :c_id, :id]], :class=>@c, :right_primary_key=>:c_id
    @c.one_through_many :othmc, [[:jt1, :c_id, :id], [:jt2, :c_id, :id]], :class=>@c, :right_primary_key=>:c_id
    @c.pg_array_to_many :pgacs, :class=>@c, :key=>:c_id

    @c.many_to_many :mtmdcs, :class=>@c, :join_table=>:jt, :left_primary_key=>:c_id, :left_key=>:id, :right_key=>:c_id, :join_table_db=>@db
    @c.one_through_one :otodc, :class=>@c, :join_table=>:jt, :left_primary_key=>:c_id, :left_key=>:id, :right_key=>:c_id, :join_table_db=>@db
    @c.many_through_many :mthmdcs, [{:table=>:jt1, :left=>:id, :right=>:c_id, :db=>@db}, [:jt2, :id, :c_id]], :class=>@c, :left_primary_key=>:c_id
    @c.one_through_many :othmdc, [{:table=>:jt1, :left=>:id, :right=>:c_id, :db=>@db}, [:jt2, :id, :c_id]], :class=>@c, :left_primary_key=>:c_id

    def @db.schema(t, *)
      t = t.first_source if t.is_a?(Sequel::Dataset)
      [[:id, {:db_type=>"#{t}_id"}], [:c_id, {:db_type=>"#{t}_c_id"}]]
    end
    @db.sqls
  end

  it "should automatically use column = ANY() for eager loads using scaler keys with known type" do
    @c.eager(:c, :cs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."id" = ANY(ARRAY[2,4]::int4[]))',
      'SELECT * FROM "items" WHERE ("items"."c_id" = ANY(ARRAY[1,3]::int8[]))']
  end

  it "should use column IN (value_list) if it cannot find the column type of from the schema" do
    @c.db_schema.clear
    @c.eager(:c, :cs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."id" IN (2, 4))',
      'SELECT * FROM "items" WHERE ("items"."c_id" IN (1, 3))']
  end

  it "should automatically use (column1, column2) IN (value_list) for eager loads using composite keys" do
    @c.eager(:cc).all
    @db.sqls.must_equal ['SELECT * FROM "items"', 'SELECT * FROM "items" WHERE (("items"."id", "items"."c_id") IN ((1, 2), (3, 4)))']
  end

  it "should automatically use column IN (value_list) for eager loads using scalar keys with unknown type" do
    @c.db_schema[:id].delete(:db_type)
    @c.db_schema[:c_id].delete(:db_type)
    @c.eager(:c, :cs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."id" IN (2, 4))',
      'SELECT * FROM "items" WHERE ("items"."c_id" IN (1, 3))']
  end

  it "should automatically use column IN (value_list) for eager loads when explicitly disabled" do
    @c.one_to_many :cs, :class=>@c, :eager_loading_predicate_transform=>nil
    @c.many_to_one :c, :class=>@c, :eager_loading_predicate_transform=>nil
    @c.eager(:c, :cs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."id" IN (2, 4))',
      'SELECT * FROM "items" WHERE ("items"."c_id" IN (1, 3))']
  end

  it "should use array with appropriate type for one_to_one association" do
    @c.eager(:first_c).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."c_id" = ANY(ARRAY[1,3]::int8[]))']
  end

  it "should use array with appropriate type for many_to_many association" do
    @c.eager(:mtmcs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "items".*, "jt"."c_id" AS "x_foreign_key_x" FROM "items" INNER JOIN "jt" ON ("jt"."id" = "items"."c_id") WHERE ("jt"."c_id" = ANY(ARRAY[1,3]::jt_c_id[]))']
  end

  it "should use array with appropriate type for one_through_one association" do
    @c.eager(:otoc).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "items".*, "jt"."c_id" AS "x_foreign_key_x" FROM "items" INNER JOIN "jt" ON ("jt"."id" = "items"."c_id") WHERE ("jt"."c_id" = ANY(ARRAY[1,3]::jt_c_id[]))']
  end

  it "should use array with appropriate type for many_through_many association" do
    @c.eager(:mthmcs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "items".*, "jt1"."c_id" AS "x_foreign_key_x" FROM "items" INNER JOIN "jt2" ON ("jt2"."id" = "items"."c_id") INNER JOIN "jt1" ON ("jt1"."id" = "jt2"."c_id") WHERE ("jt1"."c_id" = ANY(ARRAY[1,3]::jt1_c_id[]))']
  end

  it "should use array with appropriate type for one_through_many association" do
    @c.eager(:othmc).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "items".*, "jt1"."c_id" AS "x_foreign_key_x" FROM "items" INNER JOIN "jt2" ON ("jt2"."id" = "items"."c_id") INNER JOIN "jt1" ON ("jt1"."id" = "jt2"."c_id") WHERE ("jt1"."c_id" = ANY(ARRAY[1,3]::jt1_c_id[]))']
  end

  it "should use array with appropriate type for pg_array_to_many association" do
    @c.dataset = @c.dataset.with_fetch([[{:id=>1, :c_id=>[2]}, {:id=>3, :c_id=>[4]}]])
    @c.eager(:pgacs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."id" = ANY(ARRAY[2,4]::items_id[]))']
  end

  it "should use array for many_to_pg_array association" do
    @c.dataset = @c.dataset.with_fetch([[{:id=>1, :c_id=>[2]}, {:id=>3, :c_id=>[4]}]])
    @c.many_to_pg_array :mtpgas, :class=>@c, :key=>:c_id
    @c.eager(:mtpgas).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."c_id" && ARRAY[1,3]::items_c_id[])']
  end

  it "should not use typed array for many_to_many association with :join_table_db option" do
    @db.fetch = [[{:id=>2, :c_id=>5}, {:id=>4, :c_id=>6}]]
    @c.eager(:mtmdcs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "c_id", "id" FROM "jt" WHERE ("id" IN (2, 4))',
      'SELECT "items".* FROM "items" WHERE ("id" IN (5, 6))']
  end

  it "should not use type array for one_through_one association with :join_table_db option" do
    @db.fetch = [[{:id=>2, :c_id=>5}, {:id=>4, :c_id=>6}]]
    @c.eager(:otodc).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "c_id", "id" FROM "jt" WHERE ("id" IN (2, 4))',
      'SELECT "items".* FROM "items" WHERE ("id" IN (5, 6))']
  end

  it "should not use typed array for many_through_many association with edge :db option" do
    @db.fetch = [[{:id=>2, :c_id=>5}, {:id=>4, :c_id=>6}], [{:id=>5, :c_id=>7}, {:id=>6, :c_id=>8}]]
    @c.eager(:mthmdcs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "c_id", "id" FROM "jt1" WHERE ("id" IN (2, 4))',
      'SELECT "c_id", "id" FROM "jt2" WHERE ("id" IN (5, 6))',
      'SELECT "items".* FROM "items" WHERE ("id" IN (7, 8))']
  end

  it "should not use typed array for one_through_many association with edge :db option" do
    @db.fetch = [[{:id=>2, :c_id=>5}, {:id=>4, :c_id=>6}], [{:id=>5, :c_id=>7}, {:id=>6, :c_id=>8}]]
    @c.eager(:othmdc).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT "c_id", "id" FROM "jt1" WHERE ("id" IN (2, 4))',
      'SELECT "c_id", "id" FROM "jt2" WHERE ("id" IN (5, 6))',
      'SELECT "items".* FROM "items" WHERE ("id" IN (7, 8))']
  end
end
