require 'connection'
module SqlServerReplicator
  class Replicator
   
    INVALID_TEXT_TYPES = ['tinytext','mediumtext','longtext'] #will be converted to text
    INVALID_NUMERIC_TYPES = ['mediumint'] #will be converted to int
    INVALID_BINARY_TYPES = ['blob','mediumblob','longblob'] #will be converted to binary
    
    #Given a valid sql dump, import the schema and data into a table with same name in sql server
    #Use import_data = false to import only the schema
    def self.replicate(connect_opts, dump, import_data = true)
      begin
        new(connect_opts, dump, import_data).process
      rescue Exception => e
        puts e.message
      end
    end
    
    #--------------------------------End of class methods------------------------------------------------#
  
    def process
      create, insert, table = extract_sql #Extract mysql create/insert statements from the dump file
      raise "Couldn't extract create syntax from MySql Dump File" if create.nil?
      create = escape_create_string(create)
      begin
        @connection.execute("DROP TABLE #{table}") rescue ''#Drop existing table first
        @connection.execute(create) #Recreate the table 
        if insert && @import_data
          values = row_values(insert) 
          values.each do |val|
            sql = "INSERT INTO #{table}  VALUES #{val}"
            begin
              @connection.execute(sql) #Insert rows
            rescue Exception => e
              puts e.message
              puts sql
              puts "table #{table}"
            end
          end
        else
          puts "There's no records to be added" if @import_data && !insert
        end
      rescue Exception => e
        puts e.message
        puts "table #{table}"
      end
    end
    
    private
    
    def initialize(connect_opts = {}, dump = nil, import_data = true)
      @mysql_dump = dump
      @connection = Connection.new(connect_opts)
      @import_data = import_data
    end
    
    def extract_sql
      @mysql_dump.gsub!(/`\w*`/){|s| "[#{s[1, s.size-2]}]"} #Replace `special_word` with [replace_word]
      create_syntax = "CREATE " + @mysql_dump.scan(/CREATE(.*?)ENGINE/m).first.first rescue nil
      insert_syntax = "INSERT " + @mysql_dump.scan(/INSERT(.*?);/).first.first rescue nil
      table = @mysql_dump.scan(/CREATE TABLE (\[*\w*\]*) \(/).first.first rescue nil
      return create_syntax, insert_syntax, table
    end
    
    #Given mysql insert dump
    #Outputs [[attr1,attr2],*]
    def row_values(string) 
      string.gsub!(/INSERT  INTO (.*?) VALUES/, '')
      splitted = string.split("),(")
      splitted.each do |s| 
        s.gsub!("(", '') if splitted.first == s
        s.gsub!(")", '') if splitted.last == s
        s.gsub!(s, "(#{s})")
      end
      splitted
    end
    
    #clean this up
    def escape_create_string(string)
      val = ""
      string.split("\n").each do |s|
        s.downcase!
        s.gsub!(/(unsigned|auto_increment)/, '')
        s.gsub!(/comment '(\s*)'/, '')
        s.gsub!(/character set (\w*)/, '') #Remove character set 
        s.gsub!(/collate (\w*)/, '') #Remove collate
        s.gsub!(/enum\(.*?\)/, 'varchar(100)') #Change enum to varchar(100)
        s.gsub!(/bigint\([\d*]+\)/, "bigint") #Remove limit in bigint
        s.gsub!(/int\([\d*]+\)/, "int")
        s.gsub!(/tinyint\([\d*]+\)/, "tinyint") 
        s.gsub!(/default null/, 'null') #DEFAULT NULL for some weird reason forces column to be NOT NULL when using dbi
        INVALID_TEXT_TYPES.each{|t| s.gsub!(t, "text")}
        INVALID_BINARY_TYPES.each{|t| s.gsub!(t, "binary")}
        INVALID_NUMERIC_TYPES.each{|t| s.gsub!(t, "int")}
        s.gsub!(s, '') if s.include?('constraint')
        s.gsub!(s, '') if s.include?('key') && !s.include?('primary')
        s.gsub!(/,$/, " null,") unless s.include?('null') || s.include?('primary')
        val << s << "\n" unless s.empty?
      end
      val.gsub(",\n)","\n)")
    end
  end
end
