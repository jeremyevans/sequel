require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "query_literals extension" do
  before do
    @ds = Sequel.mock.dataset.from(:t).extension(:query_literals)
  end

  it "should not use special support if given a block" do
    @ds.select('a, b, c'){d}.sql.must_equal 'SELECT \'a, b, c\', d FROM t'
  end

  it "should have #select use literal string if given a single string" do
    @ds.select('a, b, c').sql.must_equal 'SELECT a, b, c FROM t'
  end

  it "should have #select use placeholder literal string if given a string and additional arguments" do
    @ds.select('a, b, ?', 1).sql.must_equal 'SELECT a, b, 1 FROM t'
  end

  it "should have #select work the standard way if initial string is a literal string already" do
    @ds.select(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT a, b, ?, 1 FROM t'
  end

  it "should have #select work regularly if not given a string as the first argument" do
    @ds.select(:a, 1).sql.must_equal 'SELECT a, 1 FROM t'
  end

  describe 'with existing selection' do
    before do
      @ds = @ds.select(:d)
    end

    it "should have #select_more use literal string if given a single string" do
      @ds.select_more('a, b, c').sql.must_equal 'SELECT d, a, b, c FROM t'
    end

    it "should have #select_more use placeholder literal string if given a string and additional arguments" do
      @ds.select_more('a, b, ?', 1).sql.must_equal 'SELECT d, a, b, 1 FROM t'
    end

    it "should have #select_more work the standard way if initial string is a literal string already" do
      @ds.select_more(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT d, a, b, ?, 1 FROM t'
    end

    it "should have #select_more work regularly if not given a string as the first argument" do
      @ds.select_more(:a, 1).sql.must_equal 'SELECT d, a, 1 FROM t'
    end
  end

  it "should have #select_append use literal string if given a single string" do
    @ds.select_append('a, b, c').sql.must_equal 'SELECT *, a, b, c FROM t'
  end

  it "should have #select_append use placeholder literal string if given a string and additional arguments" do
    @ds.select_append('a, b, ?', 1).sql.must_equal 'SELECT *, a, b, 1 FROM t'
  end

  it "should have #select_append work the standard way if initial string is a literal string already" do
    @ds.select_append(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT *, a, b, ?, 1 FROM t'
  end

  it "should have #select_append work regularly if not given a string as the first argument" do
    @ds.select_append(:a, 1).sql.must_equal 'SELECT *, a, 1 FROM t'
  end

  it "should have #select_group use literal string if given a single string" do
    @ds.select_group('a, b, c').sql.must_equal 'SELECT a, b, c FROM t GROUP BY a, b, c'
  end

  it "should have #select_group use placeholder literal string if given a string and additional arguments" do
    @ds.select_group('a, b, ?', 1).sql.must_equal 'SELECT a, b, 1 FROM t GROUP BY a, b, 1'
  end

  it "should have #select_group work the standard way if initial string is a literal string already" do
    @ds.select_group(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT a, b, ?, 1 FROM t GROUP BY a, b, ?, 1'
  end

  it "should have #select_group work regularly if not given a string as the first argument" do
    @ds.select_group(:a, 1).sql.must_equal 'SELECT a, 1 FROM t GROUP BY a, 1'
  end

  it "should have #group use literal string if given a single string" do
    @ds.group('a, b, c').sql.must_equal 'SELECT * FROM t GROUP BY a, b, c'
  end

  it "should have #group use placeholder literal string if given a string and additional arguments" do
    @ds.group('a, b, ?', 1).sql.must_equal 'SELECT * FROM t GROUP BY a, b, 1'
  end

  it "should have #group work the standard way if initial string is a literal string already" do
    @ds.group(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT * FROM t GROUP BY a, b, ?, 1'
  end

  it "should have #group work regularly if not given a string as the first argument" do
    @ds.group(:a, 1).sql.must_equal 'SELECT * FROM t GROUP BY a, 1'
  end

  it "should have #group_and_count use literal string if given a single string" do
    @ds.group_and_count('a, b, c').sql.must_equal 'SELECT a, b, c, count(*) AS count FROM t GROUP BY a, b, c'
  end

  it "should have #group_and_count use placeholder literal string if given a string and additional arguments" do
    @ds.group_and_count('a, b, ?', 1).sql.must_equal 'SELECT a, b, 1, count(*) AS count FROM t GROUP BY a, b, 1'
  end

  it "should have #group_and_count work the standard way if initial string is a literal string already" do
    @ds.group_and_count(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT a, b, ?, 1, count(*) AS count FROM t GROUP BY a, b, ?, 1'
  end

  it "should have #group_and_count work regularly if not given a string as the first argument" do
    @ds.group_and_count(:a, 1).sql.must_equal 'SELECT a, 1, count(*) AS count FROM t GROUP BY a, 1'
  end

  it "should have #group_append use literal string if given a single string" do
    @ds.group(:d).group_append('a, b, c').sql.must_equal 'SELECT * FROM t GROUP BY d, a, b, c'
  end

  it "should have #group_append use placeholder literal string if given a string and additional arguments" do
    @ds.group(:d).group_append('a, b, ?', 1).sql.must_equal 'SELECT * FROM t GROUP BY d, a, b, 1'
  end

  it "should have #group_append work the standard way if initial string is a literal string already" do
    @ds.group(:d).group_append(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT * FROM t GROUP BY d, a, b, ?, 1'
  end

  it "should have #group_append work regularly if not given a string as the first argument" do
    @ds.group(:d).group_append(:a, 1).sql.must_equal 'SELECT * FROM t GROUP BY d, a, 1'
  end

  it "should have #order use literal string if given a single string" do
    @ds.order('a, b, c').sql.must_equal 'SELECT * FROM t ORDER BY a, b, c'
  end

  it "should have #order use placeholder literal string if given a string and additional arguments" do
    @ds.order('a, b, ?', 1).sql.must_equal 'SELECT * FROM t ORDER BY a, b, 1'
  end

  it "should have #order work the standard way if initial string is a literal string already" do
    @ds.order(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT * FROM t ORDER BY a, b, ?, 1'
  end

  it "should have #order work regularly if not given a string as the first argument" do
    @ds.order(:a, 1).sql.must_equal 'SELECT * FROM t ORDER BY a, 1'
  end

  describe 'with existing order' do
    before do
      @ds = @ds.order(:d)
    end

    it "should have #order_more use literal string if given a single string" do
      @ds.order_more('a, b, c').sql.must_equal 'SELECT * FROM t ORDER BY d, a, b, c'
    end

    it "should have #order_more use placeholder literal string if given a string and additional arguments" do
      @ds.order_more('a, b, ?', 1).sql.must_equal 'SELECT * FROM t ORDER BY d, a, b, 1'
    end

    it "should have #order_more work the standard way if initial string is a literal string already" do
      @ds.order_more(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT * FROM t ORDER BY d, a, b, ?, 1'
    end

    it "should have #order_more work regularly if not given a string as the first argument" do
      @ds.order_more(:a, 1).sql.must_equal 'SELECT * FROM t ORDER BY d, a, 1'
    end

    it "should have #order_prepend use literal string if given a single string" do
      @ds.order_prepend('a, b, c').sql.must_equal 'SELECT * FROM t ORDER BY a, b, c, d'
    end

    it "should have #order_append use placeholder literal string if given a string and additional arguments" do
      @ds.order_prepend('a, b, ?', 1).sql.must_equal 'SELECT * FROM t ORDER BY a, b, 1, d'
    end

    it "should have #order_append work the standard way if initial string is a literal string already" do
      @ds.order_prepend(Sequel.lit('a, b, ?'), 1).sql.must_equal 'SELECT * FROM t ORDER BY a, b, ?, 1, d'
    end

    it "should have #order_append work regularly if not given a string as the first argument" do
      @ds.order_prepend(:a, 1).sql.must_equal 'SELECT * FROM t ORDER BY a, 1, d'
    end
  end
end
