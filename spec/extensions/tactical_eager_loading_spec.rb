require_relative "spec_helper"

describe "Sequel::Plugins::TacticalEagerLoading" do
  def sql_match(*args)
    sqls = DB.sqls
    sqls.length.must_equal args.length
    sqls.zip(args).each do |is, should|
      if should.is_a?(Regexp)
        is.must_match should
      else
        is.must_equal should
      end
    end
  end
  
  attr_reader :ts

  before do
    class ::TacticalEagerLoadingModel < Sequel::Model(:t)
      plugin :tactical_eager_loading
      columns :id, :parent_id
      many_to_one :parent, :class=>self
      one_to_many :children, :class=>self, :key=>:parent_id
      set_dataset dataset.with_fetch(proc do |sql|
        if sql !~ /WHERE/
          [{:id=>1, :parent_id=>101}, {:id=>2, :parent_id=>102}, {:id=>101, :parent_id=>nil}, {:id=>102, :parent_id=>nil}]
        elsif sql =~ /WHERE.*\bid = (\d+)/
          [{:id=>$1.to_i, :parent_id=>nil}]
        elsif sql =~ /WHERE.*\bid IN \(([\d, ]*)\)/
          $1.split(', ').map{|x| {:id=>x.to_i, :parent_id=>nil}}
        elsif sql =~ /WHERE.*\bparent_id IN \(([\d, ]*)\)/
          $1.split(', ').map{|x| {:id=>x.to_i - 100, :parent_id=>x.to_i} if x.to_i > 100}.compact
        end
      end)
    end
    @c = ::TacticalEagerLoadingModel
    @ds = TacticalEagerLoadingModel.dataset
    DB.reset
    @ts = @c.all
    sql_match('SELECT * FROM t')
  end
  after do
    Object.send(:remove_const, :TacticalEagerLoadingModel)
    sql_match
  end

  it "Dataset#all should set the retrieved_by and retrieved_with attributes" do
    ts.map{|x| [x.retrieved_by, x.retrieved_with]}.must_equal [[@ds,ts], [@ds,ts], [@ds,ts], [@ds,ts]]
  end

  it "Dataset#all shouldn't raise an error if a Sequel::Model instance is not returned" do
    @c.naked.all
    sql_match('SELECT * FROM t')
  end

  it "association getter methods should eagerly load the association if the association isn't cached" do
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    sql_match(/\ASELECT \* FROM t WHERE \(t\.id IN \(10[12], 10[12]\)\)\z/)
    ts.map{|x| x.children}.must_equal [[], [], [ts[0]], [ts[1]]]
    sql_match(/\ASELECT \* FROM t WHERE \(t\.parent_id IN/)
  end

  it "association getter methods should not eagerly load the association if the association is cached" do
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    sql_match(/\ASELECT \* FROM t WHERE \(t\.id IN \(10[12], 10[12]\)\)\z/)
    @c.dataset = @c.dataset.with_extend{def eager_load(*) raise end}
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
  end

  it "association getter methods should not eagerly load the association if a block is given" do
    ts.map{|x| x.parent{|ds| ds}}.must_equal [ts[2], ts[3], nil, nil]
    sql_match('SELECT * FROM t WHERE (t.id = 101) LIMIT 1', 'SELECT * FROM t WHERE (t.id = 102) LIMIT 1')
  end

  it "association getter methods should not eagerly load the association if a callback proc is given" do
    ts.map{|x| x.parent(:callback=>proc{|ds| ds})}.must_equal [ts[2], ts[3], nil, nil]
    sql_match('SELECT * FROM t WHERE (t.id = 101) LIMIT 1', 'SELECT * FROM t WHERE (t.id = 102) LIMIT 1')
  end

  it "association getter methods should not eagerly load the association if :reload=>true is passed" do
    ts.map{|x| x.parent(:reload=>true)}.must_equal [ts[2], ts[3], nil, nil]
    sql_match('SELECT * FROM t WHERE id = 101', 'SELECT * FROM t WHERE id = 102')
  end

  it "association getter methods should eagerly reload the association if :eager_reload=>true is passed" do
    ts.first.parent(:reload=>true)
    sql_match('SELECT * FROM t WHERE id = 101')
    ts.map{|x| x.associations.fetch(:parent, 1)}.must_equal [ts[2], 1, 1, 1]
    ts.first.parent(:eager_reload=>true)
    sql_match(/\ASELECT \* FROM t WHERE \(t\.id IN \(10[12], 10[12]\)\)\z/)
    ts.map{|x| x.associations.fetch(:parent, 1)}.must_equal [ts[2], ts[3], nil, nil]
  end

  it "association getter methods should support eagerly loading dependent associations via :eager" do
    parents = ts.map{|x| x.parent(:eager=>:children)}
    sql_match(/\ASELECT \* FROM t WHERE \(t\.id IN \(10[12], 10[12]\)\)\z/, /\ASELECT \* FROM t WHERE \(t\.parent_id IN/)
    parents.must_equal [ts[2], ts[3], nil, nil]
    parents[0..1].map{|x| x.children}.must_equal [[ts[0]], [ts[1]]]
  end

  it "association getter methods should support eager callbacks via :eager" do
    parents = ts.map{|x| x.parent(:eager=>proc{|ds| ds.where{name > 'M'}.eager(:children)})}
    sql_match(/\ASELECT \* FROM t WHERE \(\(t\.id IN \(10[12], 10[12]\)\) AND \(name > 'M'\)\)\z/, /\ASELECT \* FROM t WHERE \(t\.parent_id IN/)
    parents.must_equal [ts[2], ts[3], nil, nil]
    parents[0..1].map{|x| x.children}.must_equal [[ts[0]], [ts[1]]]
  end

  it "should handle case where an association is valid on an instance, but not on all instances" do
    c = Class.new(@c)
    c.many_to_one :parent2, :class=>@c, :key=>:parent_id
    @c.dataset.with_row_proc(proc{|r| (r[:parent_id] == 101 ? c : @c).call(r)}).all{|x| x.parent2 if x.is_a?(c)}
    sql_match('SELECT * FROM t', 'SELECT * FROM t WHERE id = 101')
  end

  it "association getter methods should not eagerly load the association if an instance is frozen" do
    ts.first.freeze
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    sql_match('SELECT * FROM t WHERE id = 101', 'SELECT * FROM t WHERE (t.id IN (102))')
    ts.map{|x| x.children}.must_equal [[], [], [ts[0]], [ts[1]]]
    sql_match('SELECT * FROM t WHERE (t.parent_id = 1)', /\ASELECT \* FROM t WHERE \(t\.parent_id IN/)
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    sql_match('SELECT * FROM t WHERE id = 101')
    ts.map{|x| x.children}.must_equal [[], [], [ts[0]], [ts[1]]]
    sql_match('SELECT * FROM t WHERE (t.parent_id = 1)')
  end

  it "#marshallable should make marshalling not fail" do
    Marshal.dump(ts.map{|x| x.marshallable!})
  end
end
