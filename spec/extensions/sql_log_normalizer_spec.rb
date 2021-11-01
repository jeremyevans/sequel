require_relative "spec_helper"

describe "sql_log_normalizer extension" do
  def db(opts={})
    return @db if @db
    @sql = String.new
    def @sql.info(sql)
      replace(sql.sub(/\A\(.*?\) /, ''))
    end
    if opts[:logger] == false
      loggers = []
    else
      loggers = @sql
    end
    @db = Sequel.mock(:loggers=>loggers)
    case opts[:type]
    when :backslash
      @db.extend_datasets do
        private
        def literal_string_append(sql, v)
          sql << "'" << v.gsub(/(\\|')/){"\\#{$1}"} << "'"
        end
      end
    when :n_standard
      @db.extend_datasets do
        private
        def literal_string_append(sql, v)
          sql << "N'" << v.gsub("'", "''") << "'"
        end
      end
    when :bad
      @db.extend_datasets do
        private
        def literal_string_append(sql, v)
          sql << "X'" << v.gsub("'", "''") << "'"
        end
      end
    end
    @db.extension(:sql_log_normalizer)
    @db
  end

  it "should normalize literal strings and numbers for standard escaping" do
    db[:ts].first(:a=>1, :b=>2.3, :c=>'d', :d=>"e\\f\\'g'")
    db.sqls.last.must_equal "SELECT * FROM ts WHERE ((a = 1) AND (b = 2.3) AND (c = 'd') AND (d = 'e\\f\\''g''')) LIMIT 1"
    @sql.must_equal "SELECT * FROM ts WHERE ((a = ?) AND (b = ?) AND (c = ?) AND (d = ?)) LIMIT ?"
  end

  it "should normalize literal strings and numbers for backslash escaping" do
    db(:type=>:backslash)[:ts].first(:a=>1, :b=>2.3, :c=>'d', :d=>"e\\f\\'g'")
    db.sqls.last.must_equal "SELECT * FROM ts WHERE ((a = 1) AND (b = 2.3) AND (c = 'd') AND (d = 'e\\\\f\\\\\\'g\\'')) LIMIT 1"
    @sql.must_equal "SELECT * FROM ts WHERE ((a = ?) AND (b = ?) AND (c = ?) AND (d = ?)) LIMIT ?"
  end

  it "should normalize literal strings and numbers for N' escaping" do
    db(:type=>:n_standard)[:ts].first(:a=>1, :b=>2.3, :c=>'d', :d=>"e\\f\\'g'")
    db.sqls.last.must_equal "SELECT * FROM ts WHERE ((a = 1) AND (b = 2.3) AND (c = N'd') AND (d = N'e\\f\\''g''')) LIMIT 1"
    @sql.must_equal "SELECT * FROM ts WHERE ((a = ?) AND (b = ?) AND (c = ?) AND (d = ?)) LIMIT ?"
  end

  it "should normalize literal strings and numbers for N' escaping when using non N' string" do
    db(:type=>:n_standard)[:ts].first(:a=>1, :b=>2.3, :c=>Sequel.lit("'d'"), :d=>"e\\f\\'g'")
    db.sqls.last.must_equal "SELECT * FROM ts WHERE ((a = 1) AND (b = 2.3) AND (c = 'd') AND (d = N'e\\f\\''g''')) LIMIT 1"
    @sql.must_equal "SELECT * FROM ts WHERE ((a = ?) AND (b = ?) AND (c = ?) AND (d = ?)) LIMIT ?"
  end

  it "should raise an error if you attempt to load it into a database that doesn't literalize strings in an expected way" do
    proc{db(:type=>:bad)}.must_raise Sequel::Error
  end

  it "should not affect cases where no logger is used" do
    db(:logger=>false)[:ts].first(:a=>1, :b=>2.3, :c=>'d', :d=>"e\\f\\'g'")
    db.sqls.last.must_equal "SELECT * FROM ts WHERE ((a = 1) AND (b = 2.3) AND (c = 'd') AND (d = 'e\\f\\''g''')) LIMIT 1"
  end

  it "should handle case where identifier contains apostrophe (will not remove all strings in this case)" do
    db[:"'ts"].first(:a=>1, :b=>2.3, :c=>'d', :d=>"e\\f\\'g'")
    db.sqls.last.must_equal "SELECT * FROM 'ts WHERE ((a = 1) AND (b = 2.3) AND (c = 'd') AND (d = 'e\\f\\''g''')) LIMIT 1"
    @sql.must_equal "SELECT * FROM ?d?e\\f\\?g''')) LIMIT ?"
  end

  it "should not include bound variables when logging" do
    db.log_connection_yield("X", nil, :a=>1, :b=>2.3, :c=>'d', :d=>"e\\f\\'g'"){}
    @sql.must_equal "X"
  end
end
