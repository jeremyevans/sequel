$:.unshift(File.expand_path('../../../lib', File.dirname(__FILE__)))
require 'json'
require 'coverage'

Coverage.start(methods: true)

require 'sequel'
DB = Sequel.mock(:columns=>[:id, :t_id], :fetch=>{:id=>1, :t_id=>2}, :numrows=>1)

opts = ENV['PLUGIN_OPTS'] ? Sequel.parse_json(ENV['PLUGIN_OPTS']).transform_keys(&:to_sym) : {}
Sequel::Model.plugin :unused_associations, opts

require_relative 'tua'

eval($stdin.read)

begin
  cov_data = if ENV['NO_COVERAGE_RESULT']
    Sequel::Model.update_associations_coverage
  else
    Sequel::Model.update_associations_coverage(coverage_result: Coverage.result)
  end

  data = if ENV['NO_COVERAGE_DATA']
    Sequel::Model.update_unused_associations_data
  elsif ENV['KEEP_COVERAGE']
    Sequel::Model.update_unused_associations_data(:keep_coverage=>true)
  else
    Sequel::Model.update_unused_associations_data(coverage_data: cov_data)
  end

  result = if ENV['NO_DATA']
    [Sequel::Model.unused_associations.sort,
     Sequel::Model.unused_association_options.sort]
  else
    [Sequel::Model.unused_associations(unused_associations_data: data).sort,
     Sequel::Model.unused_association_options(unused_associations_data: data).sort]
  end
rescue => e
  result = "#{e.class}: #{e.message}"
end

print Sequel.object_to_json(result)
