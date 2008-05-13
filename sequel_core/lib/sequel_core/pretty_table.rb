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
      columns ||= records.first.keys.sort_by{|x|x.to_s}
      sizes = column_sizes(records, columns)
      
      puts separator_line(columns, sizes)
      puts header_line(columns, sizes)
      puts separator_line(columns, sizes)
      records.each {|r| puts data_line(columns, sizes, r)}
      puts separator_line(columns, sizes)
    end

    ### Private Module Methods ###

    def self.column_sizes(records, columns) # :nodoc:
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
    
    def self.data_line(columns, sizes, record) # :nodoc:
      '|' << columns.map {|c| format_cell(sizes[c], record[c])}.join('|') << '|'
    end
    
    def self.format_cell(size, v) # :nodoc:
      case v
      when Bignum, Fixnum
        "%#{size}d" % v
      when Float
        "%#{size}g" % v
      else
        "%-#{size}s" % v.to_s
      end
    end
    
    def self.header_line(columns, sizes) # :nodoc:
      '|' << columns.map {|c| "%-#{sizes[c]}s" % c.to_s}.join('|') << '|'
    end

    def self.separator_line(columns, sizes) # :nodoc:
      '+' << columns.map {|c| '-' * sizes[c]}.join('+') << '+'
    end
    metaprivate :column_sizes, :data_line, :format_cell, :header_line, :separator_line
  end
end

