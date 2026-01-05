require_relative "spec_helper"

describe "detect_unnecessary_association_options plugin" do
  before do
    @db = Sequel.mock(host: :postgres)
    @c = Class.new(Sequel::Model(@db[:test]))
    @c.columns :id, :duao_test_id
    @c.plugin :detect_unnecessary_association_options, action: :raise
    @db.sqls
    Object.const_set(:DuaoTest, @c)
    @error = Sequel::Plugins::DetectUnnecessaryAssociationOptions::UnnecessaryAssociationOption
  end
  after do
    Object.send(:remove_const, :DuaoTest)
  end

  it "does nothing if there are no associations" do
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "does nothing if no association have unnecessary options" do
    @c.one_to_many :duao_tests
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "handles case where default class that would be used for association does not exist" do
    @c.one_to_many :d_tests, class: :DuaoTest
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "handles case when specified class that would be used for association does not exist" do
    @c.one_to_many :duao_tests, class: :DTest
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary :class option with class is provided for singular association" do
    @c.one_to_many :duao_tests, class: @c
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes action if unnecessary :class option with string is provided for singular association" do
    @c.one_to_many :duao_tests, class: "DuaoTest"
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes action if unnecessary :class option with symbol is provided for singular association" do
    @c.one_to_many :duao_tests, class: :DuaoTest
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary :class option is provided for singular association" do
    @c.one_to_many :duao_tests, class: Class.new(@c)
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary :class option with class is provided for plural association" do
    @c.many_to_one :duao_test, class: @c
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes action if unnecessary :class option with string is provided for plural association" do
    @c.many_to_one :duao_test, class: "DuaoTest"
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes action if unnecessary :class option with symbol is provided for plural association" do
    @c.many_to_one :duao_test, class: :DuaoTest
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary :class option is provided for plural association" do
    @c.many_to_one :duao_test, class: Class.new(@c)
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_one :key option is provided" do
    @c.many_to_one :duao_test, key: :duao_test_id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_one :key option is provided" do
    @c.many_to_one :duao_test, key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_one :primary_key option is provided" do
    @c.many_to_one :duao_test, primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_one :primary_key option is provided" do
    @c.many_to_one :duao_test, primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_to_many :key option is provided" do
    @c.one_to_many :duao_tests, key: :duao_test_id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_to_many :key option is provided" do
    @c.one_to_many :duao_tests, key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_to_many :primary_key option is provided" do
    @c.one_to_many :duao_tests, primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_to_many :primary_key option is provided" do
    @c.one_to_many :duao_tests, primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_to_one :key option is provided" do
    @c.one_to_one :duao_test, key: :duao_test_id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_to_one :key option is provided" do
    @c.one_to_one :duao_test, key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_to_one :primary_key option is provided" do
    @c.one_to_one :duao_test, primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_to_one :primary_key option is provided" do
    @c.one_to_one :duao_test, primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_many :left_key option is provided" do
    @c.many_to_many :duao_tests, left_key: :duao_test_id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_many :left_key option is provided" do
    @c.many_to_many :duao_tests, left_key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_many :left_primary_key option is provided" do
    @c.many_to_many :duao_tests, left_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_many :left_primary_key option is provided" do
    @c.many_to_many :duao_tests, left_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_many :right_key option is provided" do
    @c.many_to_many :duao_tests, right_key: :duao_test_id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_many :right_key option is provided" do
    @c.many_to_many :duao_tests, right_key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_many :right_primary_key option is provided" do
    @c.many_to_many :duao_tests, right_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_many :right_primary_key option is provided" do
    @c.many_to_many :duao_tests, right_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_many :join_table option is provided" do
    @c.many_to_many :duao_tests, join_table: :duao_tests_duao_tests
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_many :join_table option is provided" do
    @c.many_to_many :duao_tests, join_table: :foo
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_through_one :left_key option is provided" do
    @c.one_through_one :duao_test, left_key: :duao_test_id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_through_one :left_key option is provided" do
    @c.one_through_one :duao_test, left_key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_through_one :left_primary_key option is provided" do
    @c.one_through_one :duao_test, left_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_through_one :left_primary_key option is provided" do
    @c.one_through_one :duao_test, left_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_through_one :right_key option is provided" do
    @c.one_through_one :duao_test, right_key: :duao_test_id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_through_one :right_key option is provided" do
    @c.one_through_one :duao_test, right_key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_through_one :right_primary_key option is provided" do
    @c.one_through_one :duao_test, right_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_through_one :right_primary_key option is provided" do
    @c.one_through_one :duao_test, right_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_through_one :join_table option is provided" do
    @c.one_through_one :duao_test, join_table: :duao_tests_duao_tests
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_through_one :join_table option is provided" do
    @c.one_through_one :duao_test, join_table: :foo
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_through_many :left_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.many_through_many :duao_tests, [[:x, :y, :z]], left_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_through_many :left_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.many_through_many :duao_tests, [[:x, :y, :z]], left_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_through_many :right_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.many_through_many :duao_tests, [[:x, :y, :z]], right_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_through_many :right_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.many_through_many :duao_tests, [[:x, :y, :z]], right_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_through_many :left_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.one_through_many :duao_test, [[:x, :y, :z]], left_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_through_many :left_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.one_through_many :duao_test, [[:x, :y, :z]], left_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary one_through_many :right_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.one_through_many :duao_test, [[:x, :y, :z]], right_primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary one_through_many :right_primary_key option is provided" do
    @c.plugin :many_through_many
    @c.one_through_many :duao_test, [[:x, :y, :z]], right_primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary pg_array_to_many :key option is provided" do
    @c.plugin :pg_array_associations
    @c.pg_array_to_many :duao_tests, key: :duao_test_ids
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary pg_array_to_many :key option is provided" do
    @c.plugin :pg_array_associations
    @c.pg_array_to_many :duao_tests, key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary pg_array_to_many :primary_key option is provided" do
    @c.plugin :pg_array_associations
    @c.pg_array_to_many :duao_tests, primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary pg_array_to_many :primary_key option is provided" do
    @c.plugin :pg_array_associations
    @c.pg_array_to_many :duao_tests, primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_pg_array :key option is provided" do
    @c.plugin :pg_array_associations
    @c.many_to_pg_array :duao_tests, key: :duao_test_ids
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_pg_array :key option is provided" do
    @c.plugin :pg_array_associations
    @c.many_to_pg_array :duao_tests, key: :id
    @c.detect_unnecessary_association_options.must_be_nil
  end

  it "takes action if unnecessary many_to_pg_array :primary_key option is provided" do
    @c.plugin :pg_array_associations
    @c.many_to_pg_array :duao_tests, primary_key: :id
    proc{@c.detect_unnecessary_association_options}.must_raise @error
  end

  it "takes no action if necessary many_to_pg_array :primary_key option is provided" do
    @c.plugin :pg_array_associations
    @c.many_to_pg_array :duao_tests, primary_key: :duao_test_id
    @c.detect_unnecessary_association_options.must_be_nil
  end
  
  it "ignores unsupported association types" do
    @c.many_to_one :duao_test
    @c.association_reflection(:duao_test)[:type] = :unsupported
    @c.detect_unnecessary_association_options.must_be_nil
  end
  
  it "warns if no plugin :action option is not :raise" do
    @c.plugin :detect_unnecessary_association_options, action: nil
    @c.one_to_many :duao_tests, class: @c
    message = nil
    @c.define_singleton_method(:warn){|msg| message = msg}
    @c.detect_unnecessary_association_options.must_be_nil
    message.must_include ":class option unnecessary"
  end

  it "detects unnecessary association options when finalizing associations" do
    @c.finalize_associations
    @c.one_to_many :duao_tests, class: @c
    proc{@c.finalize_associations}.must_raise @error
  end

  it "works correctly in subclasses" do
    sc = Class.new(@c)
    sc.one_to_many :duao_tests, class: @c
    proc{sc.detect_unnecessary_association_options}.must_raise @error
  end
end
