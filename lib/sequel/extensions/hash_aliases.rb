# The hash_aliases extension allows Dataset#select and Dataset#from
# to treat a hash argument as an alias specification, with keys
# being the expressions and values being the aliases, 
# which was the historical behavior before Sequel 4.
# It is only recommended to use this for backwards compatibility.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:hash_aliases)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:hash_aliases)

#
module Sequel
  module HashAliases
    def from(*source)
      super(*convert_hash_aliases(source))
    end

    def select(*columns, &block)
      virtual_row_columns(columns, block)
      super(*convert_hash_aliases(columns), &nil)
    end

    private

    def convert_hash_aliases(columns)
      m = []
      columns.each do |i|
        if i.is_a?(Hash)
          m.concat(i.map{|k, v| SQL::AliasedExpression.new(k,v)})
        else
          m << i
        end
      end
      m
    end
  end

  Dataset.register_extension(:hash_aliases, HashAliases)
end
