require_relative "spec_helper"

describe "is_distinct_from extension" do
  dbf = lambda do |db_type|
    db = Sequel.connect("mock://#{db_type}")
    db.extension :is_distinct_from
    db
  end

  it "should support Sequel.is_distinct_from" do
    dbf[:postgres].literal(Sequel.is_distinct_from(:a, :b)).must_equal '("a" IS DISTINCT FROM "b")'
  end

  it "should support is_distinct_from on Sequel expressions" do
    dbf[:postgres].literal(Sequel[:a].is_distinct_from(:b)).must_equal '("a" IS DISTINCT FROM "b")'
  end

  it "should support is_distinct_from on literal strings" do
    dbf[:postgres].literal(Sequel.lit('a').is_distinct_from(:b)).must_equal '(a IS DISTINCT FROM "b")'
  end

  it "should use IS DISTINCT FROM syntax on PostgreSQL and H2" do
    dbf[:postgres].literal(Sequel.is_distinct_from(:a, :b)).must_equal '("a" IS DISTINCT FROM "b")'
    db = dbf[:h2]
    def db.database_type; :h2; end
    db.literal(Sequel.is_distinct_from(:a, :b)).must_equal '(a IS DISTINCT FROM b)'
  end

  it "should handle given nil values on derby" do
    db = dbf[:derby]
    def db.database_type; :derby; end
    db.literal(Sequel.is_distinct_from(:a, :b)).must_equal "((CASE WHEN ((a = b) OR ((a IS NULL) AND (b IS NULL))) THEN 0 ELSE 1 END) = 1)"
    db.literal(Sequel.is_distinct_from(nil, :b)).must_equal "(b IS NOT NULL)"
    db.literal(Sequel.is_distinct_from(:a, nil)).must_equal "(a IS NOT NULL)"
    db.literal(Sequel.is_distinct_from(nil, nil)).must_equal "'f'" # FALSE or (1 = 0) when using jdbc/derby adapter
  end

  it "should emulate IS DISTINCT FROM behavior on other databases" do
    dbf[:foo].literal(Sequel.is_distinct_from(:a, :b)).must_equal "((CASE WHEN ((a = b) OR ((a IS NULL) AND (b IS NULL))) THEN 0 ELSE 1 END) = 1)"
  end

  it "should respect existing supports_is_distinct_from? dataset method" do
    db = Sequel.mock
    db.extend_datasets do
      private
      def supports_is_distinct_from?; true; end
    end
    db.extension :is_distinct_from
    db.literal(Sequel.is_distinct_from(:a, :b)).must_equal "(a IS DISTINCT FROM b)"
  end
end
