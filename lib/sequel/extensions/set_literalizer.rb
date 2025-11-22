# frozen-string-literal: true
#
# The set_literalizer extension should no longer be used, as Sequel
# now supports Set values by default. For backwards compatibility
# the set_literalizer extension will treat a set that contains only
# 2 element arrays as a condition specifier (matching the behavior
# for arrays where all elements are 2 element arrays). This is not
# compatible with Sequel's current default behavior. If you are
# relying on this behavior, it is recommended you convert the set
# to an array.
#
# Related module: Sequel::Dataset::SetLiteralizer

module Sequel
  # SEQUEL6: Remove
  Sequel::Deprecation.deprecate("The set_literalizer extension", "Sequel now supports set literalization by default")

  class Dataset
    module SetLiteralizer
      private

      # Allow using sets as condition specifiers.
      def filter_expr(expr = nil, &block)
        if expr.is_a?(Set)
          expr
        else
          super
        end
      end

      # Literalize Set instances by converting the set to array.
      def literal_set_append(sql, v)
        literal_append(sql, v.to_a) 
      end
    end

    register_extension(:set_literalizer, SetLiteralizer)
  end
end
