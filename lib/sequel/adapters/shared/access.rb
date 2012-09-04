module Sequel
  module Access
    module DatabaseMethods
    
      DECIMAL_TYPE_RE = /decimal/io
          
      # Access uses type :access as the database_type
      def database_type
        :access
      end

      # Doesn't work, due to security restrictions on MSysObjects
      def tables
        from(:MSysObjects).filter(:Type=>1, :Flags=>0).select_map(:Name).map{|x| x.to_sym}
      end

      # Access uses type Counter for an autoincrementing keys
      def serial_primary_key_options
        {:primary_key => true, :type=>:Counter}
      end

      def schema_column_type(db_type)
        case db_type
        when /\A(bit)\z/io
          :boolean
        when /\A(byte)\z/io
          :integer
        when /\A(guid)\z/io
          :integer
        when /\A(image)\z/io
          :blob
        else
          super
        end
      end
      
      def schema_parse_table(table_name, opts)
        m = output_identifier_meth(opts[:dataset])
        idxs = ado_schema_indexes(table_name)
        ado_schema_columns(table_name).map {|row|
          specs = { 
            :allow_null => row.allow_null,
            :db_type => row.db_type,
            :default => row.default,
            :primary_key => !!idxs.find {|idx| 
                              idx["COLUMN_NAME"] == row["COLUMN_NAME"] &&
                              idx["PRIMARY_KEY"]
                            },
            :type =>  if row.db_type =~ DECIMAL_TYPE_RE && row.scale == 0
                        :integer
                      else
                        schema_column_type(row.db_type)
                      end,
            :ado_type => row["DATA_TYPE"]
          }
          specs[:default] = nil if blank_object?(specs[:default])
          specs[:allow_null] = specs[:allow_null] && !specs[:primary_key]
          [ m.call(row["COLUMN_NAME"]), specs ]
        }
      end
      
      def indexes(table_name,opts={})
        m = output_identifier_meth
        idxs = ado_schema_indexes(table_name).inject({}) do |memo, idx|
          unless idx["PRIMARY_KEY"]
            index = memo[m.call(idx["INDEX_NAME"])] ||= {
              :columns=>[], :unique=>idx["UNIQUE"]
            }
            index[:columns] << m.call(idx["COLUMN_NAME"])
          end
          memo
        end
        idxs
      end
            
      private

      def identifier_input_method_default
        nil
      end
      
      def identifier_output_method_default
        nil
      end
      
      def ado_schema_indexes(table_name)
        rows=[]
        fetch_ado_schema('indexes', [nil,nil,nil,nil,table_name.to_s]) do |row|
          rows << row
        end
        rows
      end
      
      def ado_schema_columns(table_name)
        rows=[]
        fetch_ado_schema('columns', [nil,nil,table_name.to_s,nil]) do |row| 
          rows << AdoSchema::Column.new(row)
        end
        rows.sort!{|a,b| a["ORDINAL_POSITION"] <=> b["ORDINAL_POSITION"]}
      end
            
      def fetch_ado_schema(type, criteria=[])
        execute_open_ado_schema(type, criteria) do |s|
          cols = s.Fields.extend(Enumerable).map {|c| c.Name}
          s.getRows.transpose.each do |r|
            row = {}
            cols.each{|c| row[c] = r.shift}
            yield row
          end unless s.eof
        end
      end
           
      # This is like execute() in that it yields an ADO RecordSet, except
      # instead of an SQL interface there's this OpenSchema call
      # cf. http://msdn.microsoft.com/en-us/library/ee275721(v=bts.10)
      #
      def execute_open_ado_schema(type, criteria=[])
        ado_schema = AdoSchema.new(type, criteria)
        synchronize(opts[:server]) do |conn|
          begin
            r = log_yield("OpenSchema #{type.inspect}, #{criteria.inspect}") { 
              if ado_schema.criteria.empty?
                conn.OpenSchema(ado_schema.type) 
              else
                conn.OpenSchema(ado_schema.type, ado_schema.criteria) 
              end
            }
            yield(r) if block_given?
          rescue ::WIN32OLERuntimeError => e
            raise_error(e)
          end
        end
        nil
      end
      
      class AdoSchema
        
        QUERY_TYPE = {
          'columns' => 4,
          'indexes' => 12,
          'tables'  => 20
        }
        
        attr_reader :type, :criteria
        def initialize(type, crit)
          @type     = lookup_type(type)
          @criteria = ole_criteria(crit)
        end
        
        def lookup_type(type)
          return Integer(type)
        rescue
          QUERY_TYPE[type]
        end
        
        def ole_criteria(crit)
          Array(crit)
        end
        
        class Column
        
          DATA_TYPE = {
            2   => "SMALLINT",
            3   => "INTEGER",
            4   => "REAL",
            5   => "DOUBLE",
            6   => "MONEY",
            7   => "DATETIME",
            11  => "BIT",
            14  => "DECIMAL",
            16  => "TINYINT",
            17  => "BYTE",
            72  => "GUID",
            128 => "BINARY",
            130 => "TEXT",
            131 => "DECIMAL",
            201 => "TEXT",
            205 => "IMAGE"
          }
          
          def initialize(row)
            @row = row
          end
          
          def [](col)
            @row[col]
          end
          
          def allow_null
            self["IS_NULLABLE"]
          end
          
          def default
            self["COLUMN_DEFAULT"]
          end
          
          def db_type
            t = DATA_TYPE[self["DATA_TYPE"]]
            if t == "DECIMAL" && precision
              t + "(#{precision.to_i},#{(scale || 0).to_i})"
            elsif t == "TEXT" && maximum_length && maximum_length > 0
              t + "(#{maximum_length.to_i})"
            else
              t
            end
          end
          
          def precision
            self["NUMERIC_PRECISION"]
          end
          
          def scale
            self["NUMERIC_SCALE"]
          end
          
          def maximum_length
            self["CHARACTER_MAXIMUM_LENGTH"]
          end
                    
        end
        
      end      
    end
  
    module DatasetMethods
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'select limit distinct columns from join where group order having compounds')
      DATE_FORMAT = '#%Y-%m-%d#'.freeze
      TIMESTAMP_FORMAT = '#%Y-%m-%d %H:%M:%S#'.freeze
      TOP = " TOP ".freeze
      BRACKET_CLOSE = Dataset::BRACKET_CLOSE
      BRACKET_OPEN = Dataset::BRACKET_OPEN

      # Access doesn't support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      # Access uses # to quote dates
      def literal_date(d)
        d.strftime(DATE_FORMAT)
      end

      # Access uses # to quote datetimes
      def literal_datetime(t)
        t.strftime(TIMESTAMP_FORMAT)
      end
      alias literal_time literal_datetime

      # Access uses TOP for limits
      def select_limit_sql(sql)
        if l = @opts[:limit]
          sql << TOP
          literal_append(sql, l)
        end
      end

      # Access uses [] for quoting identifiers
      def quoted_identifier_append(sql, v)
        sql << BRACKET_OPEN << v.to_s << BRACKET_CLOSE
      end

      # Access requires the limit clause come before other clauses
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
