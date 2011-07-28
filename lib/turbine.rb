require 'popen4'

module Turbine
  VERSION = "0.1"
  
  class MySQL
    HEADER_KEYS = [ :server_id, :end_log_pos, :thread_id, :execution_time, :error_code ]
    HEADER_REGEX = /^.*?server id (\d+)\s+end_log_pos (\d+)\s+Query\s+thread_id=(\d+)\s+exec_time=([\d\.]+)\s+error_code=(\d+)\n/
    attr :mysqlbinlog
    attr :statements
    attr :options
    
    def initialize(options)
      default_options = { :file => '', :database => '', :offset => 0 }
      @options = default_options.merge(options)
      
      @options[:mysqlbinlog] = `which mysqlbinlog`.strip
      @statements = Array.new
    end
    
    def reload!
      command = "#{@options[:mysqlbinlog]} #{@options[:file]} -o #{@options[:offset]}"
      unless @options[:database].empty?
        command << " --database #{@options[:database]}"
      end
      
      status = POpen4.popen4(command) do |stdout, stderr, stdin, pid|
        @error = stderr.read.strip
        @contents = stdout.read.strip
      end
      
      unless status.exitstatus == 0
        raise @error
      end
    end
    
    def parse!
      self.reload!
      array = @contents.split("\n# at ")

      array.each do |each|
        # Get the '# at 420' key (first line/comment of statement)
        key = each.lines.to_a[0].strip
        
        # Get everything except the first line
        query = each.lines.to_a[1..-1].join
        
        # Get the header from our first new line.  It looks like this:
        # #YYMMDD HH:MM:SS server id N end_log_pos N Query thread_id=N exec_time=N error_code=0
        statement = self.parse_header(query)
        
        # If the first line was a header like we expected, we can now exclude it from the rest
        # because we have everything we need out of it
        unless statement.empty?
          query = each.lines.to_a[2..-1].join
        end
        
        statement[:start_log_pos] = key.to_i
        statement[:query] = query
        @statements << statement
      end
    end
    
    def current_offset
      # Subtract 2 for the 2 statements that get passed by every mysqlbinlog result set
      @statements.count - 2 + @options[:offset]
    end
    
    def parse_header(section)
      header = section.scan(HEADER_REGEX)[0]
      if header
        Hash[HEADER_KEYS.zip(header)]
      else 
        Hash.new
      end
    end
    
  end
  
end
