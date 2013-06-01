require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

Sequel.extension :pg_array, :pg_hstore, :pg_hstore_ops

describe "Sequel::Postgres::HStoreOp" do
  before do
    @ds = Sequel.connect('mock://postgres', :quote_identifiers=>false).dataset
    @h = Sequel.hstore_op(:h)
  end

  it "#- should use the - operator" do
    @ds.literal(@h - :a).should == "(h - a)"
  end

  it "#- should cast String argument to text when using - operator" do
    @ds.literal(@h - 'a').should == "(h - CAST('a' AS text))"
  end

  it "#- should not cast LiteralString argument to text when using - operator" do
    @ds.literal(@h - Sequel.lit('a')).should == "(h - a)"
  end

  it "#- should handle arrays" do
    @ds.literal(@h - %w'a').should == "(h - ARRAY['a'])"
  end

  it "#- should handle hashes" do
    @ds.literal(@h - {'a'=>'b'}).should == "(h - '\"a\"=>\"b\"'::hstore)"
  end

  it "#- should return an HStoreOp" do
    @ds.literal((@h - :a)['a']).should == "((h - a) -> 'a')"
  end

  it "#[] should use the -> operator" do
    @ds.literal(@h['a']).should == "(h -> 'a')"
  end

  it "#[] should handle arrays" do
    @ds.literal(@h[%w'a']).should == "(h -> ARRAY['a'])"
  end

  it "#[] should return a PGArrayOp if given an array" do
    @ds.literal(@h[%w'a'][0]).should == "(h -> ARRAY['a'])[0]"
  end

  it "#[] should return a PGArrayOp if given a PGArray" do
    @ds.literal(@h[Sequel.pg_array(%w'a')][0]).should == "(h -> ARRAY['a'])[0]"
  end

  it "#[] should return a PGArrayOp if given a PGArrayOp" do
    @ds.literal(@h[Sequel.pg_array_op(:a)][0]).should == "(h -> a)[0]"
  end

  it "#[] should return a string expression" do
    @ds.literal(@h['a'] + 'b').should == "((h -> 'a') || 'b')"
  end

  it "#concat and #merge should use the || operator" do
    @ds.literal(@h.concat(:h1)).should == "(h || h1)"
    @ds.literal(@h.merge(:h1)).should == "(h || h1)"
  end

  it "#concat and #merge should handle hashes" do
    @ds.literal(@h.concat('a'=>'b')).should == "(h || '\"a\"=>\"b\"'::hstore)"
    @ds.literal(@h.merge('a'=>'b')).should == "(h || '\"a\"=>\"b\"'::hstore)"
  end

  it "#concat should return an HStoreOp" do
    @ds.literal(@h.concat(:h1)['a']).should == "((h || h1) -> 'a')"
  end

  it "#contain_all should use the ?& operator" do
    @ds.literal(@h.contain_all(:h1)).should == "(h ?& h1)"
  end

  it "#contain_all handle arrays" do
    @ds.literal(@h.contain_all(%w'h1')).should == "(h ?& ARRAY['h1'])"
  end

  it "#contain_any should use the ?| operator" do
    @ds.literal(@h.contain_any(:h1)).should == "(h ?| h1)"
  end

  it "#contain_any should handle arrays" do
    @ds.literal(@h.contain_any(%w'h1')).should == "(h ?| ARRAY['h1'])"
  end

  it "#contains should use the @> operator" do
    @ds.literal(@h.contains(:h1)).should == "(h @> h1)"
  end

  it "#contains should handle hashes" do
    @ds.literal(@h.contains('a'=>'b')).should == "(h @> '\"a\"=>\"b\"'::hstore)"
  end

  it "#contained_by should use the <@ operator" do
    @ds.literal(@h.contained_by(:h1)).should == "(h <@ h1)"
  end

  it "#contained_by should handle hashes" do
    @ds.literal(@h.contained_by('a'=>'b')).should == "(h <@ '\"a\"=>\"b\"'::hstore)"
  end

  it "#defined should use the defined function" do
    @ds.literal(@h.defined('a')).should == "defined(h, 'a')"
  end

  it "#delete should use the delete function" do
    @ds.literal(@h.delete('a')).should == "delete(h, 'a')"
  end

  it "#delete should handle arrays" do
    @ds.literal(@h.delete(%w'a')).should == "delete(h, ARRAY['a'])"
  end

  it "#delete should handle hashes" do
    @ds.literal(@h.delete('a'=>'b')).should == "delete(h, '\"a\"=>\"b\"'::hstore)"
  end

  it "#delete should return an HStoreOp" do
    @ds.literal(@h.delete('a')['a']).should == "(delete(h, 'a') -> 'a')"
  end

  it "#each should use the each function" do
    @ds.literal(@h.each).should == "each(h)"
  end

  it "#has_key? and aliases should use the ? operator" do
    @ds.literal(@h.has_key?('a')).should == "(h ? 'a')"
    @ds.literal(@h.key?('a')).should == "(h ? 'a')"
    @ds.literal(@h.member?('a')).should == "(h ? 'a')"
    @ds.literal(@h.include?('a')).should == "(h ? 'a')"
    @ds.literal(@h.exist?('a')).should == "(h ? 'a')"
  end

  it "#hstore should return the receiver" do
    @h.hstore.should equal(@h)
  end

  it "#keys and #akeys should use the akeys function" do
    @ds.literal(@h.keys).should == "akeys(h)"
    @ds.literal(@h.akeys).should == "akeys(h)"
  end

  it "#keys and #akeys should return PGArrayOps" do
    @ds.literal(@h.keys[0]).should == "akeys(h)[0]"
    @ds.literal(@h.akeys[0]).should == "akeys(h)[0]"
  end

  it "#populate should use the populate_record function" do
    @ds.literal(@h.populate(:a)).should == "populate_record(a, h)"
  end

  it "#record_set should use the #= operator" do
    @ds.literal(@h.record_set(:a)).should == "(a #= h)"
  end

  it "#skeys should use the skeys function" do
    @ds.literal(@h.skeys).should == "skeys(h)"
  end

  it "#slice should should use the slice function" do
    @ds.literal(@h.slice(:a)).should == "slice(h, a)"
  end

  it "#slice should handle arrays" do
    @ds.literal(@h.slice(%w'a')).should == "slice(h, ARRAY['a'])"
  end

  it "#slice should return an HStoreOp" do
    @ds.literal(@h.slice(:a)['a']).should == "(slice(h, a) -> 'a')"
  end

  it "#svals should use the svals function" do
    @ds.literal(@h.svals).should == "svals(h)"
  end

  it "#to_array should use the hstore_to_array function" do
    @ds.literal(@h.to_array).should == "hstore_to_array(h)"
  end

  it "#to_array should return a PGArrayOp" do
    @ds.literal(@h.to_array[0]).should == "hstore_to_array(h)[0]"
  end

  it "#to_matrix should use the hstore_to_matrix function" do
    @ds.literal(@h.to_matrix).should == "hstore_to_matrix(h)"
  end

  it "#to_matrix should return a PGArrayOp" do
    @ds.literal(@h.to_matrix[0]).should == "hstore_to_matrix(h)[0]"
  end

  it "#values and #avals should use the avals function" do
    @ds.literal(@h.values).should == "avals(h)"
    @ds.literal(@h.avals).should == "avals(h)"
  end

  it "#values and #avals should return PGArrayOps" do
    @ds.literal(@h.values[0]).should == "avals(h)[0]"
    @ds.literal(@h.avals[0]).should == "avals(h)[0]"
  end

  it "should have Sequel.hstore_op return HStoreOp instances as-is" do
    Sequel.hstore_op(@h).should equal(@h)
  end

  it "should have Sequel.hstore return HStoreOp instances" do
    Sequel.hstore(:h).should == @h
  end

  it "should be able to turn expressions into hstore ops using hstore" do
    @ds.literal(Sequel.qualify(:b, :a).hstore['a']).should == "(b.a -> 'a')"
    @ds.literal(Sequel.function(:a, :b).hstore['a']).should == "(a(b) -> 'a')"
  end

  it "should be able to turn literal strings into hstore ops using hstore" do
    @ds.literal(Sequel.lit('a').hstore['a']).should == "(a -> 'a')"
  end

  it "should allow transforming HStore instances into HStoreOp instances" do
    @ds.literal(Sequel.hstore('a'=>'b').op['a']).should == "('\"a\"=>\"b\"'::hstore -> 'a')"
  end
end
