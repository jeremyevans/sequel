# Print nice-looking plain-text tables
# +--+-------+
# |id|name   |
# |--+-------|
# |1 |fasdfas|
# |2 |test   |
# +--+-------+

module Sequel
  class PrettyTable
    def self.hash_columns(records)
      columns = []
      records.each do |r|
        r.keys.each {|k| columns << k unless columns.include?(k)}
      end
      columns
    end
    
    def self.column_sizes(records, columns)
      sizes = Hash.new {0}
      columns.each do |c|
        s = c.to_s.size
        sizes[c.to_sym] = s if s > sizes[c.to_sym]
      end
      records.each do |r|
        r.each do |k, v|
          s = v.to_s.size
          sizes[k.to_sym] = s if s > sizes[k.to_sym]
        end
      end
      sizes
    end
    
    def self.separator_line(columns, sizes)
      l = ''
      '+' + columns.map {|c| '-' * sizes[c]}.join('+') + '+'
    end
    
    def self.data_line(columns, sizes, record)
      '|' + columns.map {|c| "%-#{sizes[c]}s" % record[c].to_s}.join('|') + '|'
    end
    
    def self.header_line(columns, sizes)
      '|' + columns.map {|c| "%-#{sizes[c]}s" % c.to_s}.join('|') + '|'
    end
    
    def self.print(records, columns = nil) # records is an array of hashes
      columns ||= hash_columns(records)
      sizes = column_sizes(records, columns)
      
      puts separator_line(columns, sizes)
      puts header_line(columns, sizes)
      puts separator_line(columns, sizes)
      records.each {|r| puts data_line(columns, sizes, r)}
      puts separator_line(columns, sizes)
    end
  end
end

