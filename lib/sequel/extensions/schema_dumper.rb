module Sequel
  class Database
    # Dump indexes for all tables as a migration.  This complements
    # the :indexes=>false option to dump_schema_migration.
    def dump_indexes_migration
      ts = tables
      <<END_MIG
Class.new(Sequel::Migration) do
  def up
#{ts.map{|t| dump_table_indexes(t, :add_index)}.reject{|x| x == ''}.join("\n\n").gsub(/^/o, '    ')}
  end
  
  def down
#{ts.map{|t| dump_table_indexes(t, :drop_index)}.reject{|x| x == ''}.join("\n\n").gsub(/^/o, '    ')}
  end
end
END_MIG
    end

    # Return a string that contains a Sequel::Migration subclass that when
    # run would recreate the database structure. Options:
    # * :same_db - Don't attempt to translate database types to ruby types.
    #   If this isn't set to true, all database types will be translated to
    #   ruby types, but there is no guarantee that the migration generated
    #   will yield the same type.  Without this set, types that aren't
    #   recognized will be translated to a string-like type.
    # * :indexes - If set to false, don't dump indexes (they can be added
    #   later via dump_index_migration).
    def dump_schema_migration(options={})
      ts = tables
      <<END_MIG
Class.new(Sequel::Migration) do
  def up
#{ts.map{|t| dump_table_schema(t, options)}.join("\n\n").gsub(/^/o, '    ')}
  end
  
  def down
    drop_table(#{ts.inspect[1...-1]})
  end
end
END_MIG
    end

    # Return a string with a create table block that will recreate the given
    # table's schema.  Takes the same options as dump_schema_migration.
    def dump_table_schema(table, options={})
      s = schema(table).dup
      pks = s.find_all{|x| x.last[:primary_key] == true}.map{|x| x.first}
      options = options.merge(:single_pk=>true) if pks.length == 1
      m = method(:column_schema_to_generator_opts)
      im = method(:index_to_generator_opts)
      indexes = indexes(table) if options[:indexes] != false and respond_to?(:indexes)
      gen = Schema::Generator.new(self) do
        s.each{|name, info| send(*m.call(name, info, options))}
        primary_key(pks) if !@primary_key && pks.length > 0
        indexes.each{|iname, iopts| send(:index, iopts[:columns], im.call(table, iname, iopts))} if indexes
      end
      commands = [gen.dump_columns, gen.dump_constraints, gen.dump_indexes].reject{|x| x == ''}.join("\n\n")
      "create_table(#{table.inspect}) do\n#{commands.gsub(/^/o, '  ')}\nend"
    end

    private

    # Convert the given default, which should be a database specific string, into
    # a ruby object.  If it can't be converted, return the string with the inspect
    # method modified so that .lit is always appended after it.
    def column_schema_to_ruby_default(default, type)
      case default 
      when /false/
        false
      when 'true'
        true
      when /\A\d+\z/
        default.to_i
      else
        def default.inspect
          "#{super}.lit"
        end
        default
      end
    end

    # Convert the given name and parsed database schema into an array with a method
    # name and arguments to it to pass to a Schema::Generator to recreate the column.
    def column_schema_to_generator_opts(name, schema, options)
      if options[:single_pk] && schema_autoincrementing_primary_key?(schema)
        [:primary_key, name]
      else
        col_opts = options[:same_db] ? {:type=>schema[:db_type]} : column_schema_to_ruby_type(schema)
        type = col_opts.delete(:type)
        col_opts.delete(:size) if col_opts[:size].nil?
        col_opts[:default] = column_schema_to_ruby_default(schema[:default], type) if schema[:default]
        col_opts[:null] = false if schema[:allow_null] == false
        [:column, name, type, col_opts]
      end
    end

    # Convert the column schema information to a hash of column options, one of which must
    # be :type.  The other options added should modify that type (e.g. :size).  If a
    # database type is not recognized, return it as a String type.
    def column_schema_to_ruby_type(schema)
      case t = schema[:db_type].downcase
      when /\A(?:medium|small)?int(?:eger)?(?:\((?:\d+)\))?\z/o
        {:type=>Integer}
      when /\Atinyint(?:\((?:\d+)\))?\z/o
        {:type=>(Sequel.convert_tinyint_to_bool ? TrueClass : Integer)}
      when /\Abigint(?:\((?:\d+)\))?\z/o
        {:type=>Bignum}
      when /\A(?:real|float|double(?: precision)?)\z/o
        {:type=>Float}
      when 'boolean'
        {:type=>TrueClass}
      when /\A(?:(?:tiny|medium|long)?text|clob)\z/o
        {:type=>String, :text=>true}
      when 'date'
        {:type=>Date}
      when 'datetime'
        {:type=>DateTime}
      when /\Atimestamp(?: with(?:out)? time zone)?\z/o
        {:type=>DateTime}
      when /\Atime(?: with(?:out)? time zone)?\z/o
        {:type=>Time, :only_time=>true}
      when /\Achar(?:acter)?(?:\((\d+)\))?\z/o
        {:type=>String, :size=>($1.to_i if $1), :fixed=>true}
      when /\A(?:varchar|character varying|bpchar|string)(?:\((\d+)\))?\z/o
        s = ($1.to_i if $1)
        {:type=>String, :size=>(s == 255 ? nil : s)}
      when 'money'
        {:type=>BigDecimal, :size=>[19,2]}
      when /\A(?:decimal|numeric|number)(?:\((\d+)(?:,\s*(\d+))?\))?\z/o
        s = [($1.to_i if $1), ($2.to_i if $2)].compact
        {:type=>BigDecimal, :size=>(s.empty? ? nil : s)}
      when /\A(?:bytea|(?:tiny|medium|long)?blob|(?:var)?binary)(?:\((\d+)\))?\z/o
        {:type=>File, :size=>($1.to_i if $1)}
      when 'year'
        {:type=>Integer}
      else
        {:type=>String}
      end
    end

    # Return a string that containing add_index/drop_index method calls for
    # creating the index migration.
    def dump_table_indexes(table, meth)
      return '' unless respond_to?(:indexes)
      im = method(:index_to_generator_opts)
      indexes = indexes(table) 
      gen = Schema::Generator.new(self) do
        indexes.each{|iname, iopts| send(:index, iopts[:columns], im.call(table, iname, iopts))}
      end
      gen.dump_indexes(meth=>table)
    end

    # Convert the parsed index information into options to the Generators index method. 
    def index_to_generator_opts(table, name, index_opts)
      h = {}
      h[:name] = name unless default_index_name(table, index_opts[:columns]) == name.to_s
      h[:unique] = true if index_opts[:unique]
      h
    end
  end

  module Schema
    class Generator
      # Dump this generator's columns to a string that could be evaled inside
      # another instance to represent the same columns
      def dump_columns
        strings = []
        cols = columns.dup
        if pkn = primary_key_name
          cols.delete_if{|x| x[:name] == pkn}
          pk = @primary_key.dup
          pkname = pk.delete(:name)
          @db.serial_primary_key_options.each{|k,v| pk.delete(k) if v == pk[k]}
          strings << "primary_key #{pkname.inspect}#{opts_inspect(pk)}"
        end
        cols.each do |c|
          c = c.dup
          name = c.delete(:name)
          type = c.delete(:type)
          opts = opts_inspect(c)
          strings << if type.is_a?(Class)
            "#{type.name} #{name.inspect}#{opts}"
          else
            "column #{name.inspect}, #{type.inspect}#{opts}"
          end
        end
        strings.join("\n")
      end

      # Dump this generator's constraints to a string that could be evaled inside
      # another instance to represent the same constraints
      def dump_constraints
        constraints.map do |c|
          c = c.dup
          type = c.delete(:type)
          case type
          when :check
            raise(Error, "can't dump check/constraint specified with Proc") if c[:check].is_a?(Proc)
            name = c.delete(:name)
            if !name and c[:check].length == 1 and c[:check].first.is_a?(Hash)
              "check #{c[:check].first.inspect[1...-1]}"
            else
              "#{name ? "constraint #{name.inspect}," : 'check'} #{c[:check].map{|x| x.inspect}.join(', ')}"
            end
          else
            cols = c.delete(:columns)
            "#{type} #{cols.inspect}#{opts_inspect(c)}"
          end
        end.join("\n")
      end

      # Dump this generator's indexes to a string that could be evaled inside
      # another instance to represent the same indexes. Options:
      # * :add_index - Use add_index instead of index, so the methods
      #   can be called outside of a generator but inside a migration.
      #   The value of this option should be the table name to use.
      # * :drop_index - Same as add_index, but create drop_index statements.
      def dump_indexes(options={})
        indexes.map do |c|
          c = c.dup
          cols = c.delete(:columns)
          if table = options[:add_index] || options[:drop_index]
            "#{options[:drop_index] ? 'drop' : 'add'}_index #{table.inspect}, #{cols.inspect}#{opts_inspect(c)}"
          else
            "index #{cols.inspect}#{opts_inspect(c)}"
          end
        end.join("\n")
      end

      private

      def opts_inspect(opts)
        ", #{opts.inspect[1...-1]}" if opts.length > 0
      end
    end
  end
end
