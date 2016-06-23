require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")


describe "string_agg extension" do
  dbf = lambda do |db_type|
    db = Sequel.connect("mock://#{db_type}")
    db.extension :string_agg
    db
  end

  before(:all) do
    Sequel.extension :string_agg
  end
  before do
    @sa1 = Sequel.string_agg(:c)
    @sa2 = Sequel.string_agg(:c, '-')
    @sa3 = Sequel.string_agg(:c, '-').order(:o)
    @sa4 = Sequel.string_agg(:c).order(:o).distinct
  end

  it "should use existing method" do
    db = Sequel.mock
    db.extend_datasets do
      def string_agg_sql_append(sql, sa)
        sql << "sa(#{sa.expr})"
      end
    end
    db.extension :string_agg
    db.literal(Sequel.string_agg(:c)).must_equal "sa(c)"
  end

  it "should correctly literalize on Postgres" do
    db = dbf.call(:postgres)
    db.literal(@sa1).must_equal "string_agg(c, ',')"
    db.literal(@sa2).must_equal "string_agg(c, '-')"
    db.literal(@sa3).must_equal "string_agg(c, '-' ORDER BY o)"
    db.literal(@sa4).must_equal "string_agg(DISTINCT c, ',' ORDER BY o)"
  end

  it "should correctly literalize on SQLAnywhere" do
    db = dbf.call(:sqlanywhere)
    db.literal(@sa1).must_equal "list(c, ',')"
    db.literal(@sa2).must_equal "list(c, '-')"
    db.literal(@sa3).must_equal "list(c, '-' ORDER BY o)"
    db.literal(@sa4).must_equal "list(DISTINCT c, ',' ORDER BY o)"
  end

  it "should correctly literalize on MySQL, H2, HSQLDB, CUBRID" do
    [:mysql, :h2, :hsqldb, :cubrid].each do |type|
      db = dbf.call(type)
      db.meta_def(:database_type){type}
      db.literal(@sa1).must_equal "GROUP_CONCAT(c SEPARATOR ',')"
      db.literal(@sa2).must_equal "GROUP_CONCAT(c SEPARATOR '-')"
      db.literal(@sa3).must_equal "GROUP_CONCAT(c ORDER BY o SEPARATOR '-')"
      db.literal(@sa4).must_equal "GROUP_CONCAT(DISTINCT c ORDER BY o SEPARATOR ',')"
    end
  end

  it "should correctly literalize on Oracle and DB2" do
    [:oracle, :db2].each do |type|
      db = dbf.call(type)
      db.literal(@sa1).must_equal "listagg(c, ',') WITHIN GROUP (ORDER BY 1)"
      db.literal(@sa2).must_equal "listagg(c, '-') WITHIN GROUP (ORDER BY 1)"
      db.literal(@sa3).must_equal "listagg(c, '-') WITHIN GROUP (ORDER BY o)"
      proc{db.literal(@sa4)}.must_raise Sequel::Error
    end
  end

  it "should handle order without arguments" do
    db = dbf.call(:postgres)
    db.literal(@sa1.order).must_equal "string_agg(c, ',')"
  end

  it "should handle operations on object" do
    db = dbf.call(:postgres)
    db.literal(@sa1 + 'b').must_equal "(string_agg(c, ',') || 'b')"
    db.literal(@sa1.like('b')).must_equal "(string_agg(c, ',') LIKE 'b' ESCAPE '\\')"
    db.literal(@sa1 < 'b').must_equal "(string_agg(c, ',') < 'b')"
    db.literal(@sa1.as(:b)).must_equal "string_agg(c, ',') AS b"
    db.literal(@sa1.cast(:b)).must_equal "CAST(string_agg(c, ',') AS b)"
    db.literal(@sa1.desc).must_equal "string_agg(c, ',') DESC"
    db.literal(@sa1 =~ /a/).must_equal "(string_agg(c, ',') ~ 'a')"
    db.literal(@sa1.sql_subscript(1)).must_equal "string_agg(c, ',')[1]"
  end
end
