# frozen-string-literal: true
#
# The symbol_aref extension makes Symbol#[] support Symbol,
# Sequel::SQL::Indentifier, and Sequel::SQL::QualifiedIdentifier instances,
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
# If you are using Ruby 2+, and you would like to use refinements, there
# is a refinement version of this in the symbol_aref_refinement extension.
#
# If you are using the ruby18_symbol_extensions, and would like symbol_aref
# to take affect, load the symbol_aref extension after the
# ruby18_symbol_extensions.
#
# Related module: Sequel::SymbolAref

if RUBY_VERSION >= '2.0'
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
else
  class Symbol
    if method_defined?(:[])
      alias_method :aref_before_sequel, :[] 
    end

    if RUBY_VERSION >= '1.9'
      # 
      def [](v)
        case v
        when Symbol, Sequel::SQL::Identifier, Sequel::SQL::QualifiedIdentifier
          Sequel::SQL::QualifiedIdentifier.new(self, v)
        else
          aref_before_sequel(v)
        end
      end
    else
      def [](*v)
        arg = v.first if v.length == 1

        case arg
        when Symbol, Sequel::SQL::Identifier, Sequel::SQL::QualifiedIdentifier
          Sequel::SQL::QualifiedIdentifier.new(self, arg)
        else
          respond_to?(:aref_before_sequel) ? aref_before_sequel(*v) : super(*v)
        end
      end
    end
  end
end
