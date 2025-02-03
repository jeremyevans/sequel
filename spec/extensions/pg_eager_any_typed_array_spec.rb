require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_eager_any_typed_array plugin" do
  before do
    @db = Sequel.connect('mock://postgres')
    @c = Class.new(Sequel::Model(@db[:items]))
    def @c.name; "C" end
    @c.columns :id, :c_id
    @c.plugin :pg_eager_any_typed_array
    @c.dataset = @c.dataset.with_fetch([[{:id=>1, :c_id=>2}, {:id=>3, :c_id=>4}]])
    @c.db_schema[:id][:db_type] = "int4"
    @c.db_schema[:c_id][:db_type] = "int8"
    @c.one_to_many :cs, :class=>@c
    @c.many_to_one :c, :class=>@c
    @c.many_to_one :cc, :class=>@c, :key=>[:id, :c_id], :primary_key=>[:id, :c_id]
    @db.sqls
  end

  it "should automatically use column = ANY() for eager loads using scaler keys with known type" do
    @c.eager(:c, :cs).all
    @db.sqls.must_equal ['SELECT * FROM "items"',
      'SELECT * FROM "items" WHERE ("items"."id" = ANY(ARRAY[2,4]::int4[]))',
      'SELECT * FROM "items" WHERE ("items"."c_id" = ANY(ARRAY[1,3]::int8[]))']
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
end
