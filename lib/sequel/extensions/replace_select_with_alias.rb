# The replace_select_with_alias extension adds Sequel::Dataset#replace_select_with_alias
# for replacing existing selected columns from a dataset with aliases for the
# same column names. It preservers the order in which columns are selected.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:replace_select_with_alias)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:replace_select_with_alias)

module Sequel
  module ReplaceSelectWithAlias
    # Returns a copy of the dataset with the select statements
    # for the given aliased columns replacing the original selects.
    # If no aliases are given, it will return the existing selection.
    # If no columns are currently selected, it will select *.
    #
    #   DB[:items].select(:a, :b).replace_select_with_alias(Sequel.as("1", :a)) # SELECT '1' AS a, b FROM items
    #   DB[:items].replace_select_with_alias(Sequel.as("1", :a)) # SELECT * FROM items
    #   DB[:items].select(:a).replace_select_with_alias(Sequel.as("1", :b)) # SELECT a FROM items
    #   DB[:items].select(:a, :b).replace_select_with_alias { |o| Sequel.as("1", o.a) }.sql.should == "SELECT '1' AS a, b FROM items"
    def replace_select_with_alias(*columns, &block)
      virtual_row_columns(columns, block)
      aliased_columns = _aliased_columns(columns)
      return self if !@opts[:select] || (@opts[:select] & aliased_columns.keys).empty?

      select(*_replace_aliases(@opts[:select], aliased_columns))
    end

    def _aliased_columns(columns)
      columns.reduce({}) do |aliased_columns, column|
        case column.alias
        when nil
        when Sequel::SQL::Identifier
          aliased_columns[column.alias.value.to_sym] = column
        else
          aliased_columns[column.alias] = column
        end

        aliased_columns
      end
    end

    def _replace_aliases(selects, aliased_columns)
      selects.map { |select| aliased_columns[select] || select }
    end
  end

  Dataset.register_extension(:replace_select_with_alias, ReplaceSelectWithAlias)
end
