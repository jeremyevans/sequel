module Sequel
  class Dataset
    # Allows you to join multiple datasets/tables and have the result set
    # split into component tables.
    #
    # This differs from the usual usage of join, which returns the result set
    # as a single hash.  For example:
    #
    #   # CREATE TABLE artists (id INTEGER, name TEXT);
    #   # CREATE TABLE albums (id INTEGER, name TEXT, artist_id INTEGER);
    #   DB[:artists].left_outer_join(:albums, :artist_id=>:id).first
    #   => {:id=>(albums.id||artists.id), :name=>(albums.name||artist.names), :artist_id=>albums.artist_id}
    #   DB[:artists].graph(:albums, :artist_id=>:id).first
    #   => {:artists=>{:id=>artists.id, :name=>artists.name}, :albums=>{:id=>albums.id, :name=>albums.name, :artist_id=>albums.artist_id}}
    #
    # Using a join such as left_outer_join, the attribute names that are shared between
    # the tables are combined in the single return hash.  You can get around that by
    # using .select with correct aliases for all of the columns, but it is simpler to
    # use graph and have the result set split for you.  In addition, graph respects
    # any row_proc or transform attributes of the current dataset and the datasets
    # you use with graph.
    #
    # If you are graphing a table and all columns for that table are nil, this
    # indicates that no matching rows existed in the table, so graph will return nil
    # instead of a hash with all nil values:
    #
    #   # If the artist doesn't have any albums
    #   DB[:artists].graph(:albums, :artist_id=>:id).first
    #   => {:artists=>{:id=>artists.id, :name=>artists.name}, :albums=>nil}
    #
    # Arguments:
    # * dataset -  Can be a symbol (specifying a table), another dataset,
    #   or an object that responds to .dataset and yields a symbol or a dataset
    # * join_conditions - Any condition(s) allowed by join_table.
    # * options -  A hash of graph options.  The following options are currently used:
    #   * :table_alias - The alias to use for the table.  If not specified, doesn't
    #     alias the table.  You will get an error if the the alias (or table) name is
    #     used more than once.
    #   * :join_type - The type of join to use (passed to join_table).  Defaults to
    #     :left_outer.
    #   * :select - An array of columns to select.  When not used, selects
    #     all columns in the given dataset.  When set to false, selects no
    #     columns and is like simply joining the tables, though graph keeps
    #     some metadata about join that makes it important to use graph instead
    #     of join.
    # * block - A block that is passed to join_table.
    def graph(dataset, join_conditions = nil, options = {}, &block)
      # Allow the use of a model, dataset, or symbol as the first argument
      # Find the table name/dataset based on the argument
      dataset = dataset.dataset if dataset.respond_to?(:dataset)
      case dataset
      when Symbol
        table = dataset
        dataset = @db[dataset]
      when ::Sequel::Dataset
        table = dataset.first_source
      else
        raise Error, "The dataset argument should be a symbol, dataset, or model"
      end

      # Raise Sequel::Error with explanation that the table alias has been used
      raise_alias_error = lambda do
        raise(Error, "this #{options[:table_alias] ? 'alias' : 'table'} has already been been used, please specify " \
          "#{options[:table_alias] ? 'a different alias' : 'an alias via the :table_alias option'}") 
      end

      # Only allow table aliases that haven't been used
      table_alias = options[:table_alias] || table
      raise_alias_error.call if @opts[:graph] && @opts[:graph][:table_aliases] && @opts[:graph][:table_aliases].include?(table_alias)

      # Join the table early in order to avoid cloning the dataset twice
      ds = join_table(options[:join_type] || :left_outer, table, join_conditions, table_alias, &block)
      opts = ds.opts

      # Whether to include the table in the result set
      add_table = options[:select] == false ? false : true
      # Whether to add the columns to the list of column aliases
      add_columns = !ds.opts.include?(:graph_aliases)

      # Setup the initial graph data structure if it doesn't exist
      unless graph = opts[:graph]
        master = ds.first_source
        raise_alias_error.call if master == table_alias
        # Master hash storing all .graph related information
        graph = opts[:graph] = {}
        # Associates column aliases back to tables and columns
        column_aliases = graph[:column_aliases] = {}
        # Associates table alias (the master is never aliased)
        table_aliases = graph[:table_aliases] = {master=>self}
        # Keep track of the alias numbers used
        ca_num = graph[:column_alias_num] = {}
        # All columns in the master table are never
        # aliased, but are not included if set_graph_aliases
        # has been used.
        if add_columns
          select = (opts[:select] ||= [])
          columns.each do |column|
            column_aliases[column] = [master, column]
            select.push(:"#{master}__#{column}")
          end
        end
      end

      # Add the table alias to the list of aliases
      # Even if it isn't been used in the result set,
      # we add a key for it with a nil value so we can check if it
      # is used more than once
      table_aliases = graph[:table_aliases]
      table_aliases[table_alias] = add_table ? dataset : nil

      # Add the columns to the selection unless we are ignoring them
      if add_table && add_columns
        select = opts[:select]
        column_aliases = graph[:column_aliases]
        ca_num = graph[:column_alias_num]
        # Which columns to add to the result set
        cols = options[:select] || dataset.columns
        # If the column hasn't been used yet, don't alias it.
        # If it has been used, try table_column.
        # If that has been used, try table_column_N 
        # using the next value of N that we know hasn't been
        # used
        cols.each do |column|
          col_alias, c = if column_aliases[column]
            tc = :"#{table_alias}_#{column}"
            if column_aliases[tc]
              if can = ca_num[tc]
                ca_num[tc] += 1
                tc = :"#{tc}_#{can}"
              else
                ca_num[tc] = 1
                tc = :"#{tc}_0"
             end
            end
            [tc, :"#{table_alias}__#{column}___#{tc}"]
          else
            [column, :"#{table_alias}__#{column}"]
          end
          column_aliases[col_alias] = [table_alias, column]
          select.push(c)
        end
      end
      ds
    end

    # This allows you to manually specify the graph aliases to use
    # when using graph.  You can use it to only select certain
    # columns, and have those columns mapped to specific aliases
    # in the result set.  This is the equivalent of .select for a
    # graphed dataset, and must be used instead of .select whenever
    # graphing is used. Example:
    #
    #   DB[:artists].graph(:albums, :artist_id=>:id).set_graph_aliases(:artist_name=>[:artists, :name], :album_name=>[:albums, :name]).first
    #   => {:artists=>{:name=>artists.name}, :albums=>{:name=>albums.name}}
    #
    # Arguments:
    # * graph_aliases - Should be a hash with keys being symbols of
    #   column aliases, and values being arrays with two symbol elements.
    #   The first element of the array should be the table alias,
    #   and the second should be the actual column name.
    def set_graph_aliases(graph_aliases)
      ds = select(*graph_aliases.collect{|col_alias, tc| :"#{tc[0]}__#{tc[1]}#{"___#{col_alias}" unless tc[1] == col_alias}"})
      ds.opts[:graph_aliases]=graph_aliases
      ds
    end

    private

    # Fetch the rows, split them into component table parts,
    # tranform and run the row_proc on each part (if applicable),
    # and yield a hash of the parts.
    def graph_each(opts, &block)
      # Reject tables with nil datasets, as they are excluded from
      # the result set
      datasets = @opts[:graph][:table_aliases].to_a.reject{|ta,ds| ds.nil?}
      # Get just the list of table aliases into a local variable, for speed
      table_aliases = datasets.collect{|ta,ds| ta}
      # Get an array of arrays, one for each dataset, with
      # the necessary information about each dataset, for speed
      datasets = datasets.collect do |ta, ds|
        [ta, ds, ds.instance_variable_get(:@transform), ds.row_proc]
      end
      # Use the manually set graph aliases, if any, otherwise
      # use the ones automatically created by .graph
      column_aliases = @opts[:graph_aliases] || @opts[:graph][:column_aliases]
      fetch_rows(select_sql(opts)) do |r|
        graph = {}
        # Create the sub hashes, one per table
        table_aliases.each{|ta| graph[ta]={}}
        # Split the result set based on the column aliases
        # If there are columns in the result set that are
        # not in column_aliases, they are ignored
        column_aliases.each do |col_alias, tc|
          ta, column = tc
          graph[ta][column] = r[col_alias]
        end
        # For each dataset, transform and run the row
        # row_proc if applicable
        datasets.each do |ta,ds,tr,rp|
          g = graph[ta]
          graph[ta] = if g.values.any?
            g = ds.transform_load(g) if tr
            g = rp[g] if rp
            g
          else
            nil
          end
        end

        yield graph
      end
      self
    end
  end
end
