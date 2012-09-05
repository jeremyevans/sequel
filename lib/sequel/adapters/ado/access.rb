Sequel.require 'adapters/shared/access'

module Sequel
  module ADO
    # Database and Dataset instance methods for MSSQL specific
    # support via ADO.
    module Access
      class AdoSchema
        QUERY_TYPE = {
          'columns' => 4,
          'indexes' => 12,
          'tables'  => 20
        }
        
        attr_reader :type, :criteria

        def initialize(type, crit)
          @type     = lookup_type(type)
          @criteria = Array(crit)
        end
        
        def lookup_type(type)
          return Integer(type)
        rescue
          QUERY_TYPE[type]
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

      module DatabaseMethods
        include Sequel::Access::DatabaseMethods
    
        DECIMAL_TYPE_RE = /decimal/io

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
          
        def schema_column_type(db_type)
          case db_type.downcase
          when 'bit'
            :boolean
          when 'byte', 'guid'
            :integer
          when 'image'
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
      end
      
      class Dataset < ADO::Dataset
        include Sequel::Access::DatasetMethods
      end
    end
  end
end
