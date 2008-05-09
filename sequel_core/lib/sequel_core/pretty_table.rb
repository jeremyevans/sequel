module Sequel
  module PrettyTable
    # Prints nice-looking plain-text tables
    # 
    #   +--+-------+
    #   |id|name   |
    #   |--+-------|
    #   |1 |fasdfas|
    #   |2 |test   |
    #   +--+-------+
    def self.print(records, columns = nil) # records is an array of hashes
      columns ||= records_columns(records)
      sizes = column_sizes(records, columns)
      
      puts separator_line(columns, sizes)
      puts header_line(columns, sizes)
      puts separator_line(columns, sizes)
      records.each {|r| puts data_line(columns, sizes, r)}
      puts separator_line(columns, sizes)
    end
  end
  class << PrettyTable
    private
    def records_columns(records)
      columns = []
      records.each do |r|
        if Array === r && (k = r.keys)
          return k
        elsif Hash === r
          r.keys.each {|k| columns << k unless columns.include?(k)}
        end
      end
      columns
    end
    
    def column_sizes(records, columns)
      sizes = Hash.new {0}
      columns.each do |c|
        s = c.to_s.size
        sizes[c.to_sym] = s if s > sizes[c.to_sym]
      end
      records.each do |r|
        columns.each do |c|
          s = r[c].to_s.size
          sizes[c.to_sym] = s if s > sizes[c.to_sym]
        end
      end
      sizes
    end
    
    def separator_line(columns, sizes)
      l = ''
      '+' + columns.map {|c| '-' * sizes[c]}.join('+') + '+'
    end
    
    def format_cell(size, v)
      case v
      when Bignum, Fixnum
        "%#{size}d" % v
      when Float
        "%#{size}g" % v
      else
        "%-#{size}s" % v.to_s
      end
    end
    
    def data_line(columns, sizes, record)
      '|' + columns.map {|c| format_cell(sizes[c], record[c])}.join('|') + '|'
    end
    
    def header_line(columns, sizes)
      '|' + columns.map {|c| "%-#{sizes[c]}s" % c.to_s}.join('|') + '|'
    end
  end
end

