module EMJack
  class Job
    
    attr_reader :id, :conn
    attr_accessor :body, :ttr, :priority, :delay
    
    def initialize(conn, id, body)
      @conn = conn
      @id = id && Integer(id)
      @body = body
      @priority = conn && conn.default_priority
      @delay = conn && conn.default_delay
      @ttr = conn && conn.default_ttr
    end
    
    alias_method :jobid, :id
    
    def delete(&block)
      conn.delete(self, &block)
    end
    
    def stats(&block)
      conn.stats(:job, self, &block)
    end

    def to_s
      "#{id} -- #{body.inspect}"
    end
  end
end