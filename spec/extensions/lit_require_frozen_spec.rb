# frozen_string_literal: true
require_relative "spec_helper"

describe "lit_require_frozen extension" do
  before do
    @db = Sequel.mock
    @db.extension(:lit_require_frozen)
  end

  it "allows literal string based on frozen string" do
    @db.literal(Sequel.lit("a".freeze)).must_equal "a"
  end

  it "allows placeholder literal string with frozen placeholder string" do
    @db.literal(Sequel.lit(":a".freeze, a: "a".dup)).must_equal "'a'"
  end

  it "allows placeholder literal string with array of frozen strings" do
    @db.literal(Sequel.lit(["|".freeze, "|".freeze], "a")).must_equal "|'a'|"
  end

  it "allows [] with frozen strings" do
    @db["a".freeze].sql.must_equal "a"
  end

  it "allows [] with frozen strings and placeholders" do
    @db[":a".freeze, a: "a"].sql.must_equal "'a'"
  end

  it "allows literal string based on frozen string in where" do
    ds = @db[:t].where(Sequel.lit("a".freeze))
    ds.sql.must_equal "SELECT * FROM t WHERE (a)"
  end

  it "allows .run with frozen strings" do
    @db.run("a".freeze)
    @db.sqls.must_equal ["a"]
  end

  it "disallows literal string based on unfrozen string" do
    proc{@db.literal(Sequel.lit("a".dup))}.must_raise Sequel::LitRequireFrozen::Error
  end

  it "disallows placeholder literal string with unfrozen placeholder string" do
    proc{@db.literal(Sequel.lit(":a".dup, a: "a"))}.must_raise Sequel::LitRequireFrozen::Error
  end

  it "disallows placeholder literal string with array containing unfrozen string" do
    proc{@db.literal(Sequel.lit(["|".freeze, "|".dup], a: "a"))}.must_raise Sequel::LitRequireFrozen::Error
  end

  it "disallows [] with unfrozen strings" do
    proc{@db["a".dup]}.must_raise Sequel::LitRequireFrozen::Error
  end

  it "disallows [] with unfrozen strings and placeholders" do
    proc{@db[":a".dup, a: "a"]}.must_raise Sequel::LitRequireFrozen::Error
  end

  it "disallows .run with unfrozen strings" do
    proc{@db.run("a".dup)}.must_raise Sequel::LitRequireFrozen::Error
    @db.sqls.must_equal []
  end

  it "disallows literal string based on runfrozen string in where" do
    ds = @db[:t].where(Sequel.lit("a".dup))
    proc{ds.sql}.must_raise Sequel::LitRequireFrozen::Error
  end
end
