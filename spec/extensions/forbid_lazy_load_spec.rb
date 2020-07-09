require_relative "spec_helper"

describe "forbid_lazy_load plugin" do
  before do
    @c = Class.new(Sequel::Model)
    @c.set_dataset Sequel::Model.db[:ts].with_fetch({:id=>2, :t_id=>3})
    @c.plugin :forbid_lazy_load
    @c.columns :id, :t_id
    @c.many_to_one :t, :class=>@c, :key=>:t_id
    @c.one_to_many :ts, :class=>@c, :key=>:t_id
    @c.many_to_many :mtm_ts, :class=>@c, :join_table=>:ts, :left_key=>:id, :right_key=>:t_id
    @c.one_to_one :otoo_t, :class=>@c, :key=>:t_id
    @c.one_through_one :oto_t, :class=>@c, :join_table=>:ts, :left_key=>:id, :right_key=>:t_id
    @o1 = @c.load(:id=>1, :t_id=>2)
    @o2 = @c.load(:id=>2, :t_id=>3)
  end

  it "should not forbid lazy load if not set at instance level" do
    @o1.t.must_equal @o2
    @o1.ts.must_equal [@o2]
    @o1.mtm_ts.must_equal [@o2]
    @o1.otoo_t.must_equal @o2
    @o1.oto_t.must_equal @o2
  end

  it "should forbid lazy load when using :forbid_lazy_load true association method option" do
    proc{@o1.t(:forbid_lazy_load=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.ts(:forbid_lazy_load=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.mtm_ts(:forbid_lazy_load=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.otoo_t(:forbid_lazy_load=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.oto_t(:forbid_lazy_load=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
  end

  it "should forbid lazy load if set at instance level" do
    @o1.forbid_lazy_load.must_be_same_as @o1
    proc{@o1.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
  end

  it "should allow lazy load for instance if set at instance level" do
    o = @c.all.first
    o.allow_lazy_load.must_be_same_as o
    o.t.must_equal @o2
    o.ts.must_equal [@o2]
    o.mtm_ts.must_equal [@o2]
    o.otoo_t.must_equal @o2
    o.oto_t.must_equal @o2
  end

  it "should forbid lazy load if retrieved by dataset via Dataset#all" do
    o = @c.all.first
    proc{o.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
  end

  it "should forbid lazy load if retrieved by dataset via Dataset#each" do
    o = @c.each{|x| break x}
    proc{o.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{o.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
  end

  it "should forbid lazy load if retrieved by dataset via Dataset#where_each" do
    5.times do
      o = @c.where_each(:id=>1){|x| break x}
      proc{o.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    end
  end

  it "should forbid lazy load if retrieved by dataset via Dataset#first with integer argument" do
    5.times do
      o = @c.first(2)[0]
      proc{o.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error

      o = @c.first(2){id > 0}[0]
      proc{o.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    end
  end

  it "should not forbid lazy load if retrieved by dataset via Dataset#first with no arguments" do
    5.times do
      o = @c.first
      o.t.must_equal @o2
      o.ts.must_equal [@o2]
      o.mtm_ts.must_equal [@o2]
      o.otoo_t.must_equal @o2
      o.oto_t.must_equal @o2
    end
  end

  it "should not forbid lazy load if retrieved by dataset via Dataset#first with hash argument" do
    5.times do
      o = @c.first(id: 2)
      o.t.must_equal @o2
      o.ts.must_equal [@o2]
      o.mtm_ts.must_equal [@o2]
      o.otoo_t.must_equal @o2
      o.oto_t.must_equal @o2
    end
  end

  it "should not forbid lazy load if retrieved by dataset via Dataset#first with block" do
    5.times do
      o = @c.first{id > 1}
      o.t.must_equal @o2
      o.ts.must_equal [@o2]
      o.mtm_ts.must_equal [@o2]
      o.otoo_t.must_equal @o2
      o.oto_t.must_equal @o2
    end
  end

  it "should not forbid lazy load if retrieved by dataset via Dataset#with_pk" do
    5.times do
      o = @c.dataset.with_pk(1)
      o.t.must_equal @o2
      o.ts.must_equal [@o2]
      o.mtm_ts.must_equal [@o2]
      o.otoo_t.must_equal @o2
      o.oto_t.must_equal @o2
    end
  end

  it "should not forbid lazy load for associated objects returned by singular associations" do
    [@o1.t, @o1.otoo_t, @o1.oto_t].each do |o|
      o.associations.clear
      o.t.must_equal @o2
      o.ts.must_equal [@o2]
      o.mtm_ts.must_equal [@o2]
      o.otoo_t.must_equal @o2
      o.oto_t.must_equal @o2
    end
  end

  it "should forbid lazy load for associated objects returned by plural associations" do
    [@o1.ts, @o1.mtm_ts].each do |os|
      o = os.first
      o.associations.clear
      proc{o.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
      proc{o.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    end
  end

  it "should allow association access if cached even if forbidding lazy loading" do
    @o1.t.must_equal @o2
    @o1.ts.must_equal [@o2]
    @o1.mtm_ts.must_equal [@o2]
    @o1.otoo_t.must_equal @o2
    @o1.oto_t.must_equal @o2

    @o1.forbid_lazy_load

    @o1.t.must_equal @o2
    @o1.ts.must_equal [@o2]
    @o1.mtm_ts.must_equal [@o2]
    @o1.otoo_t.must_equal @o2
    @o1.oto_t.must_equal @o2
  end

  it "should forbid lazy load for associations if forbid_lazy_load true association option is used" do
    @c.many_to_one :t, :class=>@c, :key=>:t_id, :forbid_lazy_load=>true
    @c.one_to_many :ts, :class=>@c, :key=>:t_id, :forbid_lazy_load=>true
    @c.many_to_many :mtm_ts, :class=>@c, :join_table=>:ts, :left_key=>:id, :right_key=>:t_id, :forbid_lazy_load=>true
    @c.one_to_one :otoo_t, :class=>@c, :key=>:t_id, :forbid_lazy_load=>true
    @c.one_through_one :oto_t, :class=>@c, :join_table=>:ts, :left_key=>:id, :right_key=>:t_id, :forbid_lazy_load=>true

    proc{@o1.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
  end

  it "should allow lazy load for associations even if instances have it forbidden if forbid_lazy_load false association option is used" do
    @c.many_to_one :t, :class=>@c, :key=>:t_id, :forbid_lazy_load=>false
    @c.one_to_many :ts, :class=>@c, :key=>:t_id, :forbid_lazy_load=>false
    @c.many_to_many :mtm_ts, :class=>@c, :join_table=>:ts, :left_key=>:id, :right_key=>:t_id, :forbid_lazy_load=>false
    @c.one_to_one :otoo_t, :class=>@c, :key=>:t_id, :forbid_lazy_load=>false
    @c.one_through_one :oto_t, :class=>@c, :join_table=>:ts, :left_key=>:id, :right_key=>:t_id, :forbid_lazy_load=>false

    o = @c.all.first
    o.t.must_equal @o2
    o.ts.must_equal [@o2]
    o.mtm_ts.must_equal [@o2]
    o.otoo_t.must_equal @o2
    o.oto_t.must_equal @o2
  end

  it "should forbid lazy load after finalizing associations if not using static_cache in associated class" do
    @c.finalize_associations
    @o1.forbid_lazy_load

    proc{@o1.t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.mtm_ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.otoo_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.oto_t}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
  end

  it "should set forbid_lazy_load false association option if using static_cache in associated class and using allow_lazy_load_for_static_cache_associations" do
    @c.plugin :static_cache
    @c.allow_lazy_load_for_static_cache_associations
    @o1.forbid_lazy_load

    @o1.t.must_equal @o2
    @o1.ts.must_equal [@o2]
    @o1.mtm_ts.must_equal [@o2]
    @o1.otoo_t.must_equal @o2
    @o1.oto_t.must_equal @o2
  end

  it "should automatically set forbid_lazy_load false association option when finalizing associations if using static_cache in associated class" do
    @c.plugin :static_cache
    @c.finalize_associations
    @o1.forbid_lazy_load

    @o1.t.must_equal @o2
    @o1.ts.must_equal [@o2]
    @o1.mtm_ts.must_equal [@o2]
    @o1.otoo_t.must_equal @o2
    @o1.oto_t.must_equal @o2
  end

  it "should allow lazy load when forbidden when using :reload association method option" do
    @o1.forbid_lazy_load
    @o1.t(:reload=>true).must_equal @o2
    @o1.ts(:reload=>true).must_equal [@o2]
    @o1.mtm_ts(:reload=>true).must_equal [@o2]
    @o1.otoo_t(:reload=>true).must_equal @o2
    @o1.oto_t(:reload=>true).must_equal @o2
  end

  it "should work correctly if loading an associated object for a class that does not use the forbid_lazy_load plugin" do
    c = Class.new(Sequel::Model)
    c.set_dataset Sequel::Model.db[:ts].with_fetch({:id=>2, :t_id=>3})
    @c.one_to_one :otoo_t, :class=>c, :key=>:t_id
    @o1.otoo_t.must_equal c.load(@o2.values)
  end

  it "should not allow lazy load for associations to static cache models not using forbid_lazy_load plugin" do
    c = Class.new(Sequel::Model)
    c.set_dataset Sequel::Model.db[:ts].with_fetch({:id=>2, :t_id=>3})
    @c.one_to_many :ts, :class=>c, :key=>:t_id
    @c.plugin :static_cache
    @c.finalize_associations
    @o1.forbid_lazy_load

    @o1.t.must_equal @o2
    proc{@o1.ts}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    @o1.mtm_ts.must_equal [@o2]
    @o1.otoo_t.must_equal @o2
    @o1.oto_t.must_equal @o2
  end

  it "should allow lazy load when forbidden when using :forbid_lazy_load false association option" do
    @o1.forbid_lazy_load
    @o1.t(:forbid_lazy_load=>false).must_equal @o2
    @o1.ts(:forbid_lazy_load=>false).must_equal [@o2]
    @o1.mtm_ts(:forbid_lazy_load=>false).must_equal [@o2]
    @o1.otoo_t(:forbid_lazy_load=>false).must_equal @o2
    @o1.oto_t(:forbid_lazy_load=>false).must_equal @o2
  end

  it "should forbid lazy load when using :forbid_lazy_load true association method option even when using :reload association method option" do
    proc{@o1.t(:forbid_lazy_load=>true, :reload=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.ts(:forbid_lazy_load=>true, :reload=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.mtm_ts(:forbid_lazy_load=>true, :reload=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.otoo_t(:forbid_lazy_load=>true, :reload=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
    proc{@o1.oto_t(:forbid_lazy_load=>true, :reload=>true)}.must_raise Sequel::Plugins::ForbidLazyLoad::Error
  end

  it "should not effect naked datasets" do
    @c.naked.all.must_equal [{:id=>2, :t_id=>3}]
    @c.naked.where_each(:id=>1){|x| break x}.must_equal(:id=>2, :t_id=>3)
  end

  it "should handle datasets without row_procs" do
    ds = @c.naked
    ds.all.first.must_equal(:id=>2, :t_id=>3)
    ds.each{|x| break x}.must_equal(:id=>2, :t_id=>3)
    ds.where_each(:id=>1){|x| break x}.must_equal(:id=>2, :t_id=>3)
    ds.first(2)[0].must_equal(:id=>2, :t_id=>3)
    ds.first(2){id > 0}[0].must_equal(:id=>2, :t_id=>3)
    ds.first.must_equal(:id=>2, :t_id=>3)
    ds.first{id > 1}.must_equal(:id=>2, :t_id=>3)
    ds.first(:id=>2).must_equal(:id=>2, :t_id=>3)
    ds.with_pk(1).must_equal(:id=>2, :t_id=>3)
  end

  it "should handle datasets with row_procs different from the model" do
    ds = @c.dataset.with_row_proc(proc{|x| x})
    ds.all.first.must_equal(:id=>2, :t_id=>3)
    ds.each{|x| break x}.must_equal(:id=>2, :t_id=>3)
    ds.where_each(:id=>1){|x| break x}.must_equal(:id=>2, :t_id=>3)
    ds.first(2)[0].must_equal(:id=>2, :t_id=>3)
    ds.first(2){id > 0}[0].must_equal(:id=>2, :t_id=>3)
    ds.first.must_equal(:id=>2, :t_id=>3)
    ds.first{id > 1}.must_equal(:id=>2, :t_id=>3)
    ds.first(:id=>2).must_equal(:id=>2, :t_id=>3)
    ds.with_pk(1).must_equal(:id=>2, :t_id=>3)
  end
end
