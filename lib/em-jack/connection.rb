require 'eventmachine'
require 'yaml'

module EMJack
  class Connection

    attr_accessor :host, :port
    attr_reader :default_priority, :default_delay, :default_ttr
    
    def initialize(opts = nil)
      @host = opts && opts[:host] || 'localhost'
      @port = opts && opts[:port] || 11300
      @tube = opts && opts[:tube]
      @retry_count = opts && opts[:retry_count] || 5
      @default_priority = opts && opts[:default_priority] || 65536
      @default_delay = opts && opts[:default_delay] || 0
      @default_ttr = opts && opts[:default_ttr] || 300
      
      @used_tube = 'default'
      @watched_tubes = ['default']
      
      @data = ""
      @retries = 0
      @in_reserve = false
      @deferrables = []
      
      @conn = EM::connect(host, port, EMJack::BeanstalkConnection) do |conn|
        conn.client = self
      end
      
      unless @tube.nil?
        use(@tube)
        watch(@tube)
      end
    end
    
    def drain!
      stats do |stats|
        p stats['available-jobs']
      end
    end
    
    def use(tube, &block)
      return if @used_tube == tube
      @used_tube = tube
      @conn.send(:use, tube)
      add_deferrable(&block)
      
    end
    
    def watch(tube, &block)
      return if @watched_tubes.include?(tube)
      @watched_tubes.push(tube)
      @conn.send(:watch, tube)
      add_deferrable(&block)
    end
    
    def ignore(tube, &block)
      return if not @watched_tubes.include?(tube)
      @watched_tubes.delete(tube)
      @conn.send(:ignore, tube)
      add_deferrable(&block)
    end

    def reserve(&block)
      @conn.send(:reserve)
      add_deferrable(&block)
    end

    def each_job(&block)
      work = Proc.new do
        r = reserve
        r.callback do |job|
          block.call(job)
          EM.next_tick { work.call }
        end
      end
      work.call
    end

    def stats(type = nil, val = nil, &block)
      case(type)
      when nil then @conn.send(:stats)
      when :tube then @conn.send(:'stats-tube', val)
      when :job then @conn.send(:'stats-job', job_id(val))
      else raise EMJack::InvalidCommand.new
      end
      add_deferrable(&block)
    end

    def job_id(val)
      case val
      when Job
        val.id
      else
        val
      end
    end
    
    def list(type = nil, &block)
      case(type)
      when nil then @conn.send(:'list-tubes')
      when :use, :used then @conn.send(:'list-tube-used')
      when :watched then @conn.send(:'list-tubes-watched')
      else raise EMJack::InvalidCommand.new
      end
      add_deferrable(&block)
    end

    def delete(val, &block)
      return unless val
      @conn.send(:delete, job_id(val))
      add_deferrable(&block)
    end
    
    def put(msg, opts = nil, &block)
      case msg
      when Job
        priority = opts && opts[:priority] || msg.priority
        delay = opts && opts[:delay] || msg.delay
        ttr = opts && opts[:ttr] || msg.ttr
        body = msg.body
      else
        priority = opts && opts[:priority] || default_priority
        delay = opts && opts[:delay] || default_delay
        ttr = opts && opts[:ttr] || default_ttr
        body = msg.to_s
      end
      
      priority = default_priority if priority < 0
      priority = 2 ** 32 if priority > (2 ** 32)
      delay = default_delay if delay < 0
      ttr = default_ttr if ttr < 0
      
      @conn.send_with_data(:put, body, priority, delay, ttr, body.size)
      add_deferrable(&block)
    end
  
    def connected
      @retries = 0
    end

    def disconnected
      # XXX I think I need to run out the deferrables as failed here
      # since the connection was dropped

      raise EMJack::Disconnected if @retries >= @retry_count
      @retries += 1
      EM.add_timer(1) { @conn.reconnect(host, port) }
    end

    def add_deferrable(&block)
      df = EM::DefaultDeferrable.new
      df.errback { |err| puts "ERROR: #{err}" }
      
      @deferrables.push(df)
      df.callback(&block) if block
      df
    end
  
    def received(data)
      @data << data

      until @data.empty?
        idx = @data.index(/(.*?\r\n)/)
        break if idx.nil?

        first = $1
        
        handled = false
        %w(OUT_OF_MEMORY INTERNAL_ERROR DRAINING BAD_FORMAT
           UNKNOWN_COMMAND EXPECTED_CRLF JOB_TOO_BIG DEADLINE_SOON
           TIMED_OUT NOT_FOUND).each do |cmd|
          next unless first =~ /^#{cmd}\r\n/i
          df = @deferrables.shift
          df.fail(cmd.downcase.to_sym)

          @data = @data[(cmd.length + 2)..-1]
          handled = true
          break
        end
        next if handled

        case (first)
        when /^DELETED\r\n/ then
          df = @deferrables.shift
          df.succeed

        when /^INSERTED\s+(\d+)\r\n/ then
          df = @deferrables.shift
          df.succeed($1.to_i)

        when /^BURIED\s+(\d+)\r\n/ then
          df = @deferrables.shift
          df.fail(:buried, $1.to_i)

        when /^USING\s+(.*)\r\n/ then
          df = @deferrables.shift
          df.succeed($1)

        when /^WATCHING\s+(\d+)\r\n/ then
          df = @deferrables.shift
          df.succeed($1.to_i)

        when /^OK\s+(\d+)\r\n/ then
          bytes = $1.to_i

          body, @data = extract_body(bytes, @data)
          break if body.nil?

          df = @deferrables.shift
          df.succeed(YAML.load(body))
          next

        when /^RESERVED\s+(\d+)\s+(\d+)\r\n/ then
          id = $1.to_i
          bytes = $2.to_i

          body, @data = extract_body(bytes, @data)
          break if body.nil?

          df = @deferrables.shift
          job = EMJack::Job.new(self, id, body)
          df.succeed(job)
          next
        else
          break
        end
        @data.slice!(0, first.size)
      end
    end
    
    def extract_body(bytes, data)
      rem = data[(data.index(/\r\n/) + 2)..-1]
      return [nil, data] if rem.length < bytes
      body = rem[0..(bytes - 1)]
      data = rem[(bytes + 2)..-1]
      data = "" if data.nil?
      [body, data]
    end
  end  
end
