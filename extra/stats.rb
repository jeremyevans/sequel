# Unashamedly appropriated from Rails

class CodeStatistics
  def initialize(*pairs)
    @pairs      = pairs
    @statistics = calculate_statistics
    @total      = calculate_total if pairs.length > 1
  end

  def to_s
    print_header
    @statistics.each{ |k, v| print_line(k, v) }
    print_splitter
  
    if @total
      print_line('Total', @total)
      print_splitter
      print_code_to_test
    end
  end

  private
  def calculate_statistics
    @pairs.inject({}) do |stats, pair|
      stats[pair.first] = calculate_directory_statistics(pair.last); stats
    end
  end

  def get_file_statistics(fn, stats)
    f = File.open(fn)
    while line = f.gets
      stats[:lines]     += 1
      stats[:classes]   += 1 if line =~ /class [A-Z]/
      stats[:methods]   += 1 if line =~ /def [a-z]/
      stats[:codelines] += 1 unless line =~ /^\s*$/ || line =~ /^\s*#/
    end
  end
  
  def get_directory_statistics(dir, stats)
    Dir.foreach(dir) do |fn|
      next if fn =~ /^\./
      fn = File.join(dir, fn)
      if File.directory?(fn)
        get_directory_statistics fn, stats
      else
        next unless fn =~ /.*rb/
        get_file_statistics fn, stats
      end
    end
    stats
  end

  def calculate_directory_statistics(directory, pattern = /.*rb/)
    stats = { :lines => 0, :codelines => 0, :classes => 0, :methods => 0 }
    get_directory_statistics directory, stats
    stats
  end

  def calculate_total
    total = { :lines => 0, :codelines => 0, :classes => 0, :methods => 0 }
    @statistics.each_value { |pair| pair.each { |k, v| total[k] += v } }
    total
  end

  def print_header
    print_splitter
    puts '| Name          | Lines |   LOC | Classes | Methods | M/C | LOC/M |'
    print_splitter
  end

  def print_splitter
    puts '+---------------+-------+-------+---------+---------+-----+-------+'
  end

  def print_line(name, statistics)
    m_over_c   = (statistics[:methods] / statistics[:classes])   rescue m_over_c = 0
    loc_over_m = (statistics[:codelines] / statistics[:methods]) - 2 rescue loc_over_m = 0

    puts "| #{name.ljust(13)} " +
         "| #{statistics[:lines].to_s.rjust(5)} " +
         "| #{statistics[:codelines].to_s.rjust(5)} " +
         "| #{statistics[:classes].to_s.rjust(7)} " +
         "| #{statistics[:methods].to_s.rjust(7)} " +
         "| #{m_over_c.to_s.rjust(3)} " +
         "| #{loc_over_m.to_s.rjust(5)} |"
  end

  def print_code_to_test
    c_loc = 0
    t_loc = 0
    @statistics.each do |n, s|
      if n =~ /spec/i
        t_loc += s[:codelines]
      else
        c_loc += s[:codelines]
      end
    end
    ratio = (((t_loc.to_f / c_loc)*10).round.to_f/10).to_s[0,4]
    puts "  Code LOC: #{c_loc}     Spec LOC: #{t_loc}     Code to Spec Ratio: 1:#{ratio}"
  end
end
