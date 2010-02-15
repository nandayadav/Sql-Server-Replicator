require 'dbi'
module SqlServerReplicator
  class Connection
    attr_accessor :dbh
    
    def self.execute_sql(sql)
      c = new
      begin
        c.execute(sql)
      rescue Exception => e
        c.disconnect
      end
      c.disconnect
    end
    
    private 
    
    def initialize(dsn, user, password)
      @user = user
      @dsn = dsn
      @password = password
      @dbh = nil
      connect
    end
    
    def connect
      begin
        @dbh = DBI.connect(@dsn, @user, @password)
      rescue Exception => e
        puts e.message
      end
    end
    
    def disconnect
      @dbh.disconnect if @dbh
    end
    
    def execute(sql)
      begin
        connect if @dbh.nil?
        @dbh.do(sql)
      rescue Exception => e
        puts e.message
        puts "For Sql:" + sql
        raise e
      end
    end
    
  end
end
