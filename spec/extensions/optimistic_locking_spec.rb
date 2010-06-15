require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "optimistic_locking plugin" do
  before do
    @c = Class.new(Sequel::Model(:people)) do
    end
    h = {1=>{:id=>1, :name=>'John', :lock_version=>2}}
    lv = @lv = "lock_version"
    @c.dataset.quote_identifiers = false
    @c.dataset.meta_def(:h){h}
    @c.dataset.meta_def(:lv){lv}
    @c.dataset.meta_def(:update) do |opts|
      case update_sql(opts)
      when /UPDATE people SET (name|#{lv}) = ('Jim'|'Bob'|\d+), (?:name|#{lv}) = ('Jim'|'Bob'|\d+) WHERE \(\(id = (\d+)\) AND \(#{lv} = (\d+)\)\)/
        name, nlv = $1 == 'name' ? [$2, $3] : [$3, $2]
        m = h[$4.to_i]
        if m && m[:lock_version] == $5.to_i
          m.merge!(:name=>name.gsub("'", ''), :lock_version=>nlv.to_i)
          1
        else
          0
        end
      else
        puts update_sql(opts)
      end
    end
    @c.dataset.instance_eval do
      def fetch_rows(sql)
        m = h[1].dup
        v = m.delete(:lock_version)
        m[lv.to_sym] = v
        yield(m)
      end
    end
    @c.dataset.meta_def(:delete) do
      case delete_sql
      when /DELETE FROM people WHERE \(\(id = (\d+)\) AND \(#{lv} = (\d+)\)\)/
        m = h[$1.to_i]
        if m && m[lv.to_sym] == $2.to_i
          h.delete[$1.to_i]
          1
        else
          0
        end
      else
        puts delete_sql
      end
    end
    @c.columns :id, :name, :lock_version
    @c.plugin :optimistic_locking
  end

  specify "should raise an error when updating a stale record" do
    p1 = @c[1]
    p2 = @c[1]
    p1.update(:name=>'Jim')
    proc{p2.update(:name=>'Bob')}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end 

  specify "should raise an error when destroying a stale record" do
    p1 = @c[1]
    p2 = @c[1]
    p1.update(:name=>'Jim')
    proc{p2.destroy}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end 

  specify "should not raise an error when updating the same record twice" do
    p1 = @c[1]
    p1.update(:name=>'Jim')
    proc{p1.update(:name=>'Bob')}.should_not raise_error
  end

  specify "should allow changing the lock column via model.lock_column=" do
    @lv.replace('lv')
    @c.columns :id, :name, :lv
    @c.lock_column = :lv
    p1 = @c[1]
    p2 = @c[1]
    p1.update(:name=>'Jim')
    proc{p2.update(:name=>'Bob')}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end

  specify "should allow changing the lock column via plugin option" do
    @lv.replace('lv')
    @c.columns :id, :name, :lv
    @c.plugin :optimistic_locking, :lock_column=>:lv
    p1 = @c[1]
    p2 = @c[1]
    p1.update(:name=>'Jim')
    proc{p2.destroy}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end

  specify "should work when subclassing" do
    c = Class.new(@c)
    p1 = c[1]
    p2 = c[1]
    p1.update(:name=>'Jim')
    proc{p2.update(:name=>'Bob')}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end 
end
