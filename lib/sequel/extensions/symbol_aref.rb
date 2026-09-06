# frozen-string-literal: true
#
# The symbol_aref extension makes Symbol#[] support Symbol,
# Sequel::SQL::Identifier, and Sequel::SQL::QualifiedIdentifier instances,
# returning appropriate Sequel::SQL::QualifiedIdentifier instances.  It's
# designed as a shortcut so that instead of:
#
#   Sequel[:table][:column] # table.column
#
# you can just write:
#
#   :table[:column] # table.column
#
# To load the extension:
#
#   Sequel.extension :symbol_aref
#
# There is a refinement version of this in the symbol_aref_refinement extension.
#
# Related module: Sequel::SymbolAref

module Sequel::SymbolAref
  def [](v)
    case v
    when Symbol, Sequel::SQL::Identifier, Sequel::SQL::QualifiedIdentifier
      Sequel::SQL::QualifiedIdentifier.new(self, v)
    else
      super
    end
  end
end

class Symbol
  prepend Sequel::SymbolAref
end
