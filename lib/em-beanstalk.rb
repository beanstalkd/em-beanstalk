require 'eventmachine'
require 'yaml'

$LOAD_PATH << File.expand_path(File.dirname(__FILE__))

require 'em-beanstalk/job'
require 'em-beanstalk/defer'
require 'em-beanstalk/connection'

module EM
  class Beanstalk

    Disconnected   = Class.new(RuntimeError)
    InvalidCommand = Class.new(RuntimeError)

    Body = Struct.new(:body, :data)
    
    module VERSION
      STRING = File.read(File.join(File.dirname(__FILE__), '..', 'VERSION'))
    end

    attr_accessor :host, :port
    attr_reader :default_priority, :default_delay, :default_ttr, :default_error_callback
  
    def initialize(opts = nil)
      @host                   = opts && opts[:host] || 'localhost'
      @port                   = opts && opts[:port] || 11300
      @tube                   = opts && opts[:tube] || 'default'
      @retry_count            = opts && opts[:retry_count] || 5
      @default_priority       = opts && opts[:default_priority] || 65536
      @default_delay          = opts && opts[:default_delay] || 0
      @default_ttr            = opts && opts[:default_ttr] || 300
      @default_timeout        = opts && opts[:timeout] || 5
      @default_error_callback = opts && opts[:default_error_callback] || Proc.new{ |error| puts "ERROR: #{error.inspect}" }
      @raise_on_disconnect    = opts && opts.key?(:raise_on_disconnect) ? opts[:raise_on_disconnect] : true
      
      @watched_tubes          = []
    
      @data = ""
      @retries = 0
      @in_reserve = false
      @deferrables = []
    
      @conn = EM::connect(host, port, EM::Beanstalk::Connection) do |conn|
        conn.client = self
        conn.comm_inactivity_timeout = 0
        conn.pending_connect_timeout = @default_timeout
      end
    
      if @tube
        use(@tube)
        watch(@tube)
      end
    end
  
    def close
      @disconnect_manually = true
      @conn.close_connection
    end
  
    def drain!(&block)
      stats do |stats|
        stats['current-jobs-ready'].zero? ?
          EM.next_tick(&block) :
          reserve{|job| job.delete{ drain!(&block) }}
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

    def reserve(timeout = nil, &block)
      if timeout
        @conn.send(:'reserve-with-timeout', timeout)
      else
        @conn.send(:reserve)
      end
      add_deferrable(&block)
    end

    def each_job(timeout = nil, &block)
      work = Proc.new do
        r = reserve(timeout)
        r.callback do |job|
          block.call(job)
          EM.next_tick { work.call }
        end
        r
      end
      work.call
    end

    def stats(type = nil, val = nil, &block)
      case(type)
      when nil        then @conn.send(:stats)
      when :tube      then @conn.send(:'stats-tube', val)
      when :job       then @conn.send(:'stats-job', job_id(val))
      else                 raise EM::Beanstalk::InvalidCommand.new
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
      when :tube, :tubes, nil then  @conn.send(:'list-tubes')
      when :use, :used        then  @conn.send(:'list-tube-used')
      when :watch, :watched   then  @conn.send(:'list-tubes-watched')
      else                          raise EM::Beanstalk::InvalidCommand.new
      end
      add_deferrable(&block)
    end

    def delete(val, &block)
      return unless val
      @conn.send(:delete, job_id(val))
      add_deferrable(&block)
    end
    
    def peek(id, &block)
      case id
      when :ready   then  @conn.send(:'peek-ready')
      when :delayed then  @conn.send(:'peek-delayed')
      when :buried  then  @conn.send(:'peek-buried')
      else                @conn.send(:'peek', id)
      end
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

    def release(val, opts = nil, &block)
      return if val.nil?
      delay = opts && opts[:delay] || default_delay
      priority = opts && opts[:priority] || default_priority
      @conn.send(:release, job_id(val), priority.to_i, delay.to_i)
      add_deferrable(&block)
    end

    def connected
      @retries = 0
    end

    def disconnected
      @deferrables.each {|d| d.fail(:disconnected) }
      unless @disconnect_manually
        raise EM::Beanstalk::Disconnected if @retries >= @retry_count && @raise_on_disconnect
        @retries += 1
        EM.add_timer(1) { reconnect }
      end
    end

    def reconnect
      @disconnect_manually = false
      @conn.reconnect(host, port)
    end

    def add_deferrable(&block)
      df = Defer.new(default_error_callback, &block)
      @deferrables.push(df)
      df
    end

    def on_error(&block)
      @error_callback = block
    end

    def received(data)
      @data << data
      
      until @data.empty?
        idx = @data.index(/(.*?)\r\n/)
        break if idx.nil?

        first = $1
      
        case (first)
        when /^DELETED/
          df = @deferrables.shift
          df.succeed
        when /^INSERTED\s+(\d+)/
          df = @deferrables.shift
          df.succeed($1.to_i)
        when /^RELEASED/
          df = @deferrables.shift
          df.succeed
        when /^BURIED\s+(\d+)/
          df = @deferrables.shift
          df.fail(:buried, $1.to_i)
        when /^USING\s+(.*)/
          df = @deferrables.shift
          df.succeed($1)
        when /^WATCHING\s+(\d+)/
          df = @deferrables.shift
          df.succeed($1.to_i)
        when /^OK\s+(\d+)/
          bytes = $1.to_i
          if body = extract_body(bytes, @data)
            @data = body.data
            df = @deferrables.shift
            df.succeed(YAML.load(body.body))
            next
          else
            break
          end
        when /^(RESERVED|FOUND)\s+(\d+)\s+(\d+)/
          id = $2.to_i
          bytes = $3.to_i
          if body = extract_body(bytes, @data)
            @data = body.data
            df = @deferrables.shift
            job = EM::Beanstalk::Job.new(self, id, body.body)
            df.succeed(job)
            next
          else
            break
          end
        # error state
        when /^(OUT_OF_MEMORY|INTERNAL_ERROR|DRAINING|BAD_FORMAT|UNKNOWN_COMMAND|EXPECTED_CRLF|JOB_TOO_BIG|DEADLINE_SOON|TIMED_OUT|NOT_FOUND)/
          puts "... got error, calling df."
          df = @deferrables.shift
          df.fail($1.downcase.to_sym)
          @data = @data[($1.length + 2)..-1]
        else
          break
        end
        @data.slice!(0, first.size + 2)
      end
    end
  
    def extract_body(bytes, data)
      rem = data[(data.index(/\r\n/) + 2)..-1]
      if rem.length < bytes
        nil
      else
        body = rem[0..(bytes - 1)]
        data = rem[(bytes + 2)..-1]
        data = "" if data.nil?
        Body.new(body, data)
      end
    end
  end  
end
