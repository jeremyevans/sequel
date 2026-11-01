# frozen-string-literal: true
#
# The virtual_row_core_refinements extension applies the core
# refinements when evaluating virtual row blocks:
#
#   Sequel.extension :virtual_row_core_refinements
#
# This requires Proc#refined, so it is only available in Ruby 4.1.

raise "the virtual_row_core_refinements extension requires Ruby 4.1+" unless RUBY_VERSION >= "4.1" # simplecov:disable

require_relative 'core_refinements'

#
module Sequel::VirtualRowCoreRefinements
  def virtual_row(&block)
    super(&block.refined(Sequel::CoreRefinements))
  end

  Sequel.extend(self)
end
