require File.join(File.dirname(__FILE__), "spec_helper")

# Note, this is just a few ideas. The real specs for this plugin will need to be created per search engine.

describe Sequel::Plugins::Searchable do

  it "should define a search engine among ferret, solr, lucene, sphinx"
  
  it "should define a 'full_text_search' method"

  it "should allow specification of the fields that get searched, ex: :fields => [:body, :title]"
  
end

describe Sequel::Plugins::Searchable, "#full_text_search" do

  it "should perform the search based on the engine"
  
end
