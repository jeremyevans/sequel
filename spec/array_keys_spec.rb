require File.join(File.dirname(__FILE__), 'spec_helper')

context "An array with symbol keys" do
  setup do
    @a = [1, 2, 3]
	  @a.keys = [:a, :b, :c]
  end
  
  specify "should provide subscript access" do
    @a[0].should == 1
	  @a[0..1].should == [1, 2]
	
	  @a[1] = 4
	  @a.should == [1, 4, 3]
  end
  
  specify "should provide key access using symbols" do
    @a[:a].should == 1
    @a[:b].should == 2
    @a[:B].should == nil
 
    @a[:a] = 11
    @a.should == [11, 2, 3]
    @a[:a].should == 11
 
    @a[:d] = 4
    @a.should == [11, 2, 3, 4]
    @a.keys.should == [:a, :b, :c, :d]
  end
  
  specify "should provide key acess using strings" do
    @a['a'].should == 1
    @a['A'].should be_nil
    
    @a['d'] = 4
    @a.should == [1, 2, 3, 4]
    @a.keys.should == [:a, :b, :c, :d]
  end

  specify "should provide #store functionality" do
    @a.store(:a, 11)
    @a.should == [11, 2, 3]
    
    @a.store(:d, 4)
    @a.should == [11, 2, 3, 4]

    @a.store('d', 44)
    @a.should == [11, 2, 3, 44]
  end

  specify "should provide #to_hash/#to_h functionality" do
    @a.to_hash.should == {:a => 1, :b => 2, :c => 3}
    @a.to_h.should == {:a => 1, :b => 2, :c => 3}
  end
  
  specify "should provide #fields as alias to #keys" do
    @a.fields.should == [:a, :b, :c]
    @a.fields = [:x, :y, :z]
    
    @a[:x].should == 1
  end
  
  specify "should provide #slice functionality with keys" do
    s = @a.slice(0, 2)
    s.should == [1, 2]
    s.keys.should == [:a, :b]
    
    s = @a.slice(1..2)
    s.should == [2, 3]
    s.keys.should == [:b, :c]
  end
  
  specify "should provide #each_pair iterator" do
    pairs = []
    @a.each_pair {|k, v| pairs << [k, v]}
    pairs.should == [[:a, 1], [:b, 2], [:c, 3]]
  end
  
  specify "should provide stock #delete functionality for arrays without keys" do
    a = [1, 2, 3]
    a.delete(2)
    a.should == [1, 3]
  end
  
  specify "should provide key-based #delete functionality" do
    @a.delete(:b)
    @a.should == [1, 3]
    @a.keys.should == [:a, :c]
    @a[:a].should == 1
    @a[:c].should == 3
  end
  
  specify "should separate array keys after #delete/#delete_at" do
    b = @a.dup

    b.delete(:b)

    @a.keys.should == [:a, :b, :c]
    b.keys.should == [:a, :c]
    @a.should == [1, 2, 3]
    b.should == [1, 3]
    @a[:b].should == 2
    b[:b].should == nil
  end
  
  specify "should provide #each_key functionality" do
    keys = []
    @a.each_key {|k| keys << k}
    keys.should == [:a, :b, :c]
  end
  
  specify "should provide #each_value functionality" do
    values = []
    @a.each_value {|v| values << v}
    values.should == [1, 2, 3]
  end
  
  specify "should provide stock #include? functionality for arrays without keys" do
    [1, 2, 3].include?(2).should be_true
    [1, 2, 3].include?(4).should be_false
  end

  specify "should provide #has_key?/#member?/#key?/#include? functionality" do
    @a.has_key?(:a).should be_true
    @a.has_key?(:b).should be_true
    @a.has_key?(:c).should be_true
    @a.has_key?(:B).should be_false
    @a.has_key?(:d).should be_false

    @a.has_key?('a').should be_true
    @a.has_key?('b').should be_true
    @a.has_key?('c').should be_true
    @a.has_key?('A').should be_false
    @a.has_key?('d').should be_false

    @a.key?(:a).should be_true
    @a.key?(:b).should be_true
    @a.key?(:c).should be_true
    @a.key?(:B).should be_false
    @a.key?(:d).should be_false

    @a.key?('a').should be_true
    @a.key?('b').should be_true
    @a.key?('c').should be_true
    @a.key?('A').should be_false
    @a.key?('d').should be_false

    @a.member?(:a).should be_true
    @a.member?(:b).should be_true
    @a.member?(:c).should be_true
    @a.member?(:B).should be_false
    @a.member?(:d).should be_false

    @a.member?('a').should be_true
    @a.member?('b').should be_true
    @a.member?('c').should be_true
    @a.member?('A').should be_false
    @a.member?('d').should be_false

    @a.include?(:a).should be_true
    @a.include?(:b).should be_true
    @a.include?(:c).should be_true
    @a.include?(:B).should be_false
    @a.include?(:d).should be_false

    @a.include?('a').should be_true
    @a.include?('b').should be_true
    @a.include?('c').should be_true
    @a.include?('A').should be_false
    @a.include?('d').should be_false
  end
  
  specify "should provide original #include? functionality for arrays without keys" do
    [1, 2, 3].include?(:a).should be_false
    [1, 2, 3].include?(1).should be_true
  end
  
  specify "should provide #has_value?/#value? functionality" do
    @a.has_value?(1).should be_true
    @a.has_value?(2).should be_true
    @a.has_value?(3).should be_true
    @a.has_value?(4).should be_false

    @a.value?(1).should be_true
    @a.value?(2).should be_true
    @a.value?(3).should be_true
    @a.value?(4).should be_false
  end
  
  specify "should provide #fetch functionality" do
    @a.fetch(:a).should == 1
    @a.fetch(:b).should == 2
    @a.fetch(:c).should == 3
    proc {@a.fetch(:d)}.should raise_error(IndexError)
    @a.fetch(:d, 4).should == 4
    @a.fetch(:d, nil).should == nil
    
    @a.fetch(:a) {|v| v.to_s}.should == '1'
    @a.fetch(:d, 4) {|v| v.to_s}.should == '4'
  end
  
  specify "should provide #values functionality" do
    @a.values.should == [1, 2, 3]
  end
  
  specify "should provide #dup functionality" do
    b = @a.dup
    b.should == [1, 2, 3]
    b.keys.should == @a.keys
    
    b[:a].should == 1
    b[:b].should == 2
    b[:c].should == 3
    b[:d].should be_nil
    
    @a.keys << :e
    @a.keys.should == [:a, :b, :c, :e]
    b.keys.should == @a.keys
  end
  
  specify "should provide #clone functionality" do
    b = @a.clone
    b.should == [1, 2, 3]
    b.keys.should == @a.keys

    b[:a].should == 1
    b[:b].should == 2
    b[:c].should == 3
    b[:d].should be_nil

    @a.keys << :e
    @a.keys.should == [:a, :b, :c, :e]
    b.keys.should_not == @a.keys
  end

  specify "should provide #merge functionality" do
    @a.merge(@a).to_hash.should == {:a => 1, :b => 2, :c => 3}
    
    @a.merge({:b => 22, :d => 4}).to_hash.should == {:a => 1, :b => 22, :c => 3, :d => 4}
    
    b = [1, 2, 3]
    b.keys = [:b, :c, :d]
    @a.merge(b).to_hash.should == {:a => 1, :b => 1, :c => 2, :d => 3}

    # call with a block. The block returns the old value passed to it
    @a.merge(b) {|k, o, n| o}.to_hash.should == {:a => 1, :b => 2, :c => 3, :d => 3}
  end
  
  specify "should provide #merge!/#update!/#update functionality" do
    @a.merge!(@a)
    @a.to_hash.should == {:a => 1, :b => 2, :c => 3}
    
    @a.update(:b => 22)
    @a.to_hash.should == {:a => 1, :b => 22, :c => 3}
    
    b = [1, 2, 3]
    b.keys = [:b, :c, :d]
    @a.update!(b)
    @a.to_hash.should == {:a => 1, :b => 1, :c => 2, :d => 3}
  end
end

context "An array with string keys" do
  setup do
    @a = [1, 2, 3]
      @a.keys = ['a', 'b', 'c']
  end

  specify "should provide key access using symbols" do
    @a[:a].should == 1
    @a[:b].should == 2
    @a[:B].should == nil

    @a[:a] = 11
    @a.should == [11, 2, 3]
    @a[:a].should == 11

    @a[:d] = 4
    @a.should == [11, 2, 3, 4]
    @a.keys.should == ['a', 'b', 'c', :d]
  end

  specify "should provide key acess using strings" do
    @a['a'].should == 1
    @a['A'].should be_nil

    @a['d'] = 4
    @a.should == [1, 2, 3, 4]
    @a.keys.should == ['a', 'b', 'c', :d]
  end

  specify "should provide #store functionality" do
    @a.store(:a, 11)
    @a.should == [11, 2, 3]

    @a.store(:d, 4)
    @a.should == [11, 2, 3, 4]

    @a.store('d', 44)
    @a.should == [11, 2, 3, 44]
  end

  specify "should provide #to_hash/#to_h functionality" do
    @a.to_hash.should == {:a => 1, :b => 2, :c => 3}
    @a.to_h.should == {:a => 1, :b => 2, :c => 3}
  end

  specify "should provide #fields as alias to #keys" do
    @a.fields.should == ['a', 'b', 'c']
    @a.fields = [:x, :y, :z]

    @a[:x].should == 1
  end

  specify "should provide #slice functionality with keys" do
    s = @a.slice(0, 2)
    s.should == [1, 2]
    s.keys.should == ['a', 'b']

    s = @a.slice(1..2)
    s.should == [2, 3]
    s.keys.should == ['b', 'c']
  end

  specify "should provide #each_pair iterator" do
    pairs = []
    @a.each_pair {|k, v| pairs << [k, v]}
    pairs.should == [['a', 1], ['b', 2], ['c', 3]]
  end

  specify "should provide key-based #delete functionality" do
    @a.delete(:b)
    @a.should == [1, 3]
    @a.keys.should == ['a', 'c']
    @a[:a].should == 1
    @a[:c].should == 3
  end

  specify "should provide #each_key functionality" do
    keys = []
    @a.each_key {|k| keys << k}
    keys.should == ['a', 'b', 'c']
  end

  specify "should provide #each_value functionality" do
    values = []
    @a.each_value {|v| values << v}
    values.should == [1, 2, 3]
  end

  specify "should provide #has_key?/#member?/#key?/#include? functionality" do
    @a.has_key?(:a).should be_true
    @a.has_key?(:b).should be_true
    @a.has_key?(:c).should be_true
    @a.has_key?(:B).should be_false
    @a.has_key?(:d).should be_false

    @a.has_key?('a').should be_true
    @a.has_key?('b').should be_true
    @a.has_key?('c').should be_true
    @a.has_key?('A').should be_false
    @a.has_key?('d').should be_false

    @a.key?(:a).should be_true
    @a.key?(:b).should be_true
    @a.key?(:c).should be_true
    @a.key?(:B).should be_false
    @a.key?(:d).should be_false

    @a.key?('a').should be_true
    @a.key?('b').should be_true
    @a.key?('c').should be_true
    @a.key?('A').should be_false
    @a.key?('d').should be_false

    @a.member?(:a).should be_true
    @a.member?(:b).should be_true
    @a.member?(:c).should be_true
    @a.member?(:B).should be_false
    @a.member?(:d).should be_false

    @a.member?('a').should be_true
    @a.member?('b').should be_true
    @a.member?('c').should be_true
    @a.member?('A').should be_false
    @a.member?('d').should be_false

    @a.include?(:a).should be_true
    @a.include?(:b).should be_true
    @a.include?(:c).should be_true
    @a.include?(:B).should be_false
    @a.include?(:d).should be_false

    @a.include?('a').should be_true
    @a.include?('b').should be_true
    @a.include?('c').should be_true
    @a.include?('A').should be_false
    @a.include?('d').should be_false
  end

  specify "should provide original #include? functionality for arrays without keys" do
    [1, 2, 3].include?(:a).should be_false
    [1, 2, 3].include?(1).should be_true
  end

  specify "should provide #has_value?/#value? functionality" do
    @a.has_value?(1).should be_true
    @a.has_value?(2).should be_true
    @a.has_value?(3).should be_true
    @a.has_value?(4).should be_false

    @a.value?(1).should be_true
    @a.value?(2).should be_true
    @a.value?(3).should be_true
    @a.value?(4).should be_false
  end

  specify "should provide #fetch functionality" do
    @a.fetch(:a).should == 1
    @a.fetch(:b).should == 2
    @a.fetch(:c).should == 3
    proc {@a.fetch(:d)}.should raise_error(IndexError)
    @a.fetch(:d, 4).should == 4
    @a.fetch(:d, nil).should == nil

    @a.fetch(:a) {|v| v.to_s}.should == '1'
    @a.fetch(:d, 4) {|v| v.to_s}.should == '4'
  end

  specify "should provide #values functionality" do
    @a.values.should == [1, 2, 3]
  end

  specify "should provide #dup functionality" do
    b = @a.dup
    b.should == [1, 2, 3]
    b.keys.should == @a.keys

    b[:a].should == 1
    b[:b].should == 2
    b[:c].should == 3
    b[:d].should be_nil

    @a.keys << :e
    @a.keys.should == ['a', 'b', 'c', :e]
    b.keys.should == @a.keys
  end

  specify "should provide #clone functionality" do
    b = @a.clone
    b.should == [1, 2, 3]
    b.keys.should == @a.keys

    b[:a].should == 1
    b[:b].should == 2
    b[:c].should == 3
    b[:d].should be_nil

    @a.keys << :e
    @a.keys.should == ['a', 'b', 'c', :e]
    b.keys.should_not == @a.keys
  end

  specify "should provide #merge functionality" do
    @a.merge(@a).to_hash.should == {:a => 1, :b => 2, :c => 3}

    @a.merge({:b => 22, :d => 4}).to_hash.should == {:a => 1, :b => 22, :c => 3, :d => 4}

    b = [1, 2, 3]
    b.keys = [:b, :c, :d]
    @a.merge(b).to_hash.should == {:a => 1, :b => 1, :c => 2, :d => 3}

    # call with a block. The block returns the old value passed to it
    @a.merge(b) {|k, o, n| o}.to_hash.should == {:a => 1, :b => 2, :c => 3, :d => 3}
  end

  specify "should provide #merge!/#update!/#update functionality" do
    @a.merge!(@a)
    @a.to_hash.should == {:a => 1, :b => 2, :c => 3}

    @a.update(:b => 22)
    @a.to_hash.should == {:a => 1, :b => 22, :c => 3}

    b = [1, 2, 3]
    b.keys = [:b, :c, :d]
    @a.update!(b)
    @a.to_hash.should == {:a => 1, :b => 1, :c => 2, :d => 3}
  end
end

context "Array.from_hash" do
  specify "should construct an array with keys from a hash" do
    h = {:x => 1, :y => 2, :z => 3}
    a = Array.from_hash(h)
    a.to_hash.should == h
  end
end

context "Sequel.use_array_tuples" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        block[{:a => 1, :b => 2, :c => 3}]
      end
    end
    
    @ds = @c.new(nil).from(:items)
  end
  
  teardown do
    Sequel.use_hash_tuples
  end
  
  specify "should cause the dataset to return array tuples instead of hashes" do
    @ds.first.should == {:a => 1, :b => 2, :c => 3}
    Sequel.use_array_tuples
    a = @ds.first
    a.class.should == Array
    a.values.sort.should == [1, 2, 3]
    a.keys.map {|k| k.to_s}.sort.should == ['a', 'b', 'c']
    a[:a].should == 1
    a[:b].should == 2
    a[:c].should == 3
    a[:d].should == nil
  end
  
  specify "should be reversible using Sequel.use_hash_tuples" do
    Sequel.use_array_tuples
    @ds.first.class.should == Array
    
    Sequel.use_hash_tuples
    @ds.first.should == {:a => 1, :b => 2, :c => 3}
  end
end