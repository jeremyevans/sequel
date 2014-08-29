# This adds the following dataset methods:
#
# []= :: filter with the first argument, update with the second
# insert_multiple :: insert multiple rows at once
# set :: alias for update
# to_csv :: return string in csv format for the dataset
# db= :: change the dataset's database
# opts= :: change the dataset's opts
#
# It is only recommended to use this for backwards compatibility.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:sequel_3_dataset_methods)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:sequel_3_dataset_methods)

#
module Sequel
  module Sequel3DatasetMethods
    COMMA = Dataset::COMMA
    # The database related to this dataset.  This is the Database instance that
    # will execute all of this dataset's queries.
    attr_writer :db

    # The hash of options for this dataset, keys are symbols.
    attr_writer :opts

    # Update all records matching the conditions with the values specified.
    # Returns the number of rows affected.
    #
    #   DB[:table][:id=>1] = {:id=>2} # UPDATE table SET id = 2 WHERE id = 1
    #   # => 1 # number of rows affected
    def []=(conditions, values)
      filter(conditions).update(values)
    end

    # Inserts multiple values. If a block is given it is invoked for each
    # item in the given array before inserting it.  See +multi_insert+ as
    # a possibly faster version that may be able to insert multiple
    # records in one SQL statement (if supported by the database).
    # Returns an array of primary keys of inserted rows.
    #
    #   DB[:table].insert_multiple([{:x=>1}, {:x=>2}])
    #   # => [4, 5]
    #   # INSERT INTO table (x) VALUES (1)
    #   # INSERT INTO table (x) VALUES (2)
    #
    #   DB[:table].insert_multiple([{:x=>1}, {:x=>2}]){|row| row[:y] = row[:x] * 2; row }
    #   # => [6, 7]
    #   # INSERT INTO table (x, y) VALUES (1, 2)
    #   # INSERT INTO table (x, y) VALUES (2, 4)
    def insert_multiple(array, &block)
      if block
        array.map{|i| insert(block.call(i))}
      else
        array.map{|i| insert(i)}
      end
    end
    
    # Return a copy of the dataset with unqualified identifiers in the
    # SELECT, WHERE, GROUP, HAVING, and ORDER clauses qualified by the
    # given table. If no columns are currently selected, select all
    # columns of the given table.
    #
    #   DB[:items].filter(:id=>1).qualify_to(:i)
    #   # SELECT i.* FROM items WHERE (i.id = 1)
    def qualify_to(table)
      qualify(table)
    end
    
    # Qualify the dataset to its current first source.  This is useful
    # if you have unqualified identifiers in the query that all refer to
    # the first source, and you want to join to another table which
    # has columns with the same name as columns in the current dataset.
    # See +qualify_to+.
    #
    #   DB[:items].filter(:id=>1).qualify_to_first_source
    #   # SELECT items.* FROM items WHERE (items.id = 1)
    def qualify_to_first_source
      qualify
    end
    
    # Alias for update, but not aliased directly so subclasses
    # don't have to override both methods.
    def set(*args)
      update(*args)
    end
    
    # Returns a string in CSV format containing the dataset records. By 
    # default the CSV representation includes the column titles in the
    # first line. You can turn that off by passing false as the 
    # include_column_titles argument.
    #
    # This does not use a CSV library or handle quoting of values in
    # any way.  If any values in any of the rows could include commas or line
    # endings, you shouldn't use this.
    #
    #   puts DB[:table].to_csv # SELECT * FROM table
    #   # id,name
    #   # 1,Jim
    #   # 2,Bob
    def to_csv(include_column_titles = true)
      n = naked
      cols = n.columns
      csv = ''
      csv << "#{cols.join(COMMA)}\r\n" if include_column_titles
      n.each{|r| csv << "#{cols.collect{|c| r[c]}.join(COMMA)}\r\n"}
      csv
    end
  end

  Dataset.register_extension(:sequel_3_dataset_methods, Sequel3DatasetMethods)
end
