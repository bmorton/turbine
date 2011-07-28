require 'turbine'

puts "MySQL Watcher -- An example app for Turbine"
puts "This example will watch your MySQL binlog file and report any changes made after the script has started"
puts "-------------------------------------------------------------------------------------------------------\n\n" 

t = Turbine::MySQL.new  :file => '/usr/local/mysql/data/mysql-bin.0*',
                        :database => 'mockingbird',
                        :offset => 0

puts "Getting initial offset..."
t.parse!
current_offset = t.current_offset + 1

loop do
  puts "Reloading with offset #{current_offset} and parsing..."
  t.offset = current_offset
  t.parse!
  
  if t.current_offset > current_offset
    current_offset = t.current_offset + 1
    t.statements.each do |statement|
      puts statement[:query]
      puts "\n"
    end
    puts "----------------------------------------------------\n\n\n"
  else
    puts "-- No changes!"
  end
  puts "Sleeping..."
  sleep 5
end
