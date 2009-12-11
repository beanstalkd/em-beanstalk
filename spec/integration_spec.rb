require 'spec/spec_helper'

describe EM::Beanstalk, "integration" do
  include EM::Spec

  it 'should use a default host of "localhost" and port of 11300' do
    conn = EM::Beanstalk.new
    conn.host.should == 'localhost'
    conn.port.should == 11300    
    done
  end

  it 'should watch and use a provided tube on connect' do
    conn = EM::Beanstalk.new
    conn.watch('my-lovely-tube') do 
      conn.list(:watched) do |watched|
        watched.should include("my-lovely-tube")
        done
      end
    end
  end

  it 'should use errback' do
    conn = EM::Beanstalk.new
    conn.delete(123123) {
      fail
    }.on_error { |err|
      puts "err! #{err.inspect}"
      done
    } 
  end

  it 'should send the "use" command' do
    conn = EM::Beanstalk.new
    conn.use('my-lovely-tube') do 
      conn.list(:used) do |used|
        used.should include("my-lovely-tube")
        done
      end
    end
  end
  
  it "should put a job" do
    conn = EM::Beanstalk.new
    conn.put('myjob') do
      conn.reserve do |job|
        job.body.should == 'myjob'
        job.delete { done }
      end
    end
  end

  it "should peek a job" do
    conn = EM::Beanstalk.new
    conn.put('myjob') do |id|
      conn.peek(id) do |job|
        job.body.should == 'myjob'
        job.delete { done }
      end
    end
  end

  it "should drain the queue" do
    conn = EM::Beanstalk.new
    conn.put('myjob')
    conn.put('myjob')
    conn.put('myjob')
    conn.put('myjob')
    conn.put('myjob')
    conn.drain!{
      conn.stats do |stats|
        stats['current-jobs-ready'].should == 0
        done
      end
    }
  end

  it 'should default the delay, priority and ttr settings' do
    conn = EM::Beanstalk.new
    conn.put('myjob') do
      conn.reserve do |job|
        job.delay.should == conn.default_delay
        job.ttr.should == conn.default_ttr
        job.priority.should == conn.default_priority
        job.delete { done }
      end
    end
  end
  
  it 'should default the delay, priority and ttr settings' do
    conn = EM::Beanstalk.new
    conn.put('myjob') do
      conn.reserve do |job|
        job.delay.should == conn.default_delay
        job.ttr.should == conn.default_ttr
        job.priority.should == conn.default_priority
        job.delete { done }
      end
    end
  end
  
  it 'should accept a delay setting' do
    conn = EM::Beanstalk.new
    start = Time.new.to_f
    conn.put('mydelayedjob', :delay => 3) do |id|
      conn.reserve do |job|
        (Time.new.to_f - start).should be_close(3, 0.1)
        job.id.should == id
        job.delete { done }
      end
    end
  end
  
  it 'should accept a ttr setting' do
    conn = EM::Beanstalk.new
    conn.put('mydelayedjob', :ttr => 1) do |id|
      
      conn.reserve do |job|
        job.id.should == id
        EM.add_timer(3) do
          conn2 = EM::Beanstalk.new
          conn2.reserve do |job2|
            job2.id.should == id
            job2.delete { done }
          end
        end
      end
    end
  end
  
  it 'should accept a priority setting' do
    conn = EM::Beanstalk.new
    conn.put('myhighpriorityjob', :priority => 800) do |low_id|
      conn.put('myhighpriorityjob', :priority => 300) do |high_id|
        conn.reserve do |job|
          job.id.should == high_id
          job.delete { conn.delete(low_id){done}}
        end
      end
    end
  end
  
  it 'should handle a non-string provided as the put message' do
    conn = EM::Beanstalk.new
    conn.put(22) do |id|
      conn.reserve do |job|
        job.body.should == '22'
        job.delete {done}
      end
    end
  end
  #
  #it 'should send the "delete" command' do
  #  @connection_mock.should_receive(:send).once.with(:delete, 1)
  #  job = EM::Beanstalk::Job.new(nil, 1, "body")
  #  conn = EM::Beanstalk.new
  #  conn.delete(job)
  #end
  #
  #it 'should handle a nil job sent to the "delete" command' do
  #  @connection_mock.should_not_receive(:send).with(:delete, nil)
  #  conn = EM::Beanstalk.new
  #  conn.delete(nil)
  #end
  #
  #it 'should send the "reserve" command' do
  #  @connection_mock.should_receive(:send).with(:reserve)
  #  conn = EM::Beanstalk.new
  #  conn.reserve
  #end
  #
  #it 'should raise exception if reconnect fails more then RETRY_COUNT times' do
  #  EM.should_receive(:add_timer).exactly(5).times
  #
  #  conn = EM::Beanstalk.new
  #  5.times { conn.disconnected }
  #  lambda { conn.disconnected }.should raise_error(EM::Beanstalk::Disconnected)
  #end
  #
  #it 'should reset the retry count on connection' do
  #  EM.should_receive(:add_timer).at_least(1).times
  #
  #  conn = EM::Beanstalk.new
  #  5.times { conn.disconnected }
  #  conn.connected
  #  lambda { conn.disconnected }.should_not raise_error(EM::Beanstalk::Disconnected)
  #end
  #
  #%w(OUT_OF_MEMORY INTERNAL_ERROR DRAINING BAD_FORMAT
  #   UNKNOWN_COMMAND EXPECTED_CRLF JOB_TOO_BIG DEADLINE_SOON
  #   TIMED_OUT NOT_FOUND).each do |cmd|
  #  it "should handle #{cmd} messages" do
  #     conn = EM::Beanstalk.new
  #
  #     df = conn.add_deferrable
  #     df.should_receive(:fail).with(cmd.downcase.to_sym)
  #
  #     conn.received("#{cmd}\r\n")
  #   end
  #end
  #
  #it 'should handle deleted messages' do
  #  conn = EM::Beanstalk.new
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed)
  #
  #  conn.received("DELETED\r\n")
  #end
  #
  #it 'should handle inserted messages' do
  #  conn = EM::Beanstalk.new
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with(40)
  #
  #  conn.received("INSERTED 40\r\n")
  #end
  #
  #it 'should handle buried messages' do
  #  conn = EM::Beanstalk.new
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:fail).with(:buried, 40)
  #
  #  conn.received("BURIED 40\r\n")
  #end
  #
  #it 'should handle using messages' do
  #  conn = EM::Beanstalk.new
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with("mytube")
  #
  #  conn.received("USING mytube\r\n")
  #end
  #
  #it 'should handle watching messages' do
  #  conn = EM::Beanstalk.new
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with(24)
  #
  #  conn.received("WATCHING 24\r\n")
  #end
  #
  #it 'should handle reserved messages' do
  #  conn = EM::Beanstalk.new
  #
  #  msg = "This is my message"
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with do |job|
  #    job.class.should == EM::Beanstalk::Job
  #    job.id.should == 42
  #    job.body.should == msg
  #  end
  #
  #  conn.received("RESERVED 42 #{msg.length}\r\n#{msg}\r\n")
  #end
  #
  #it 'should handle receiving multiple replies in one packet' do
  #  conn = EM::Beanstalk.new
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with(24)
  #
  #  df2 = conn.add_deferrable
  #  df2.should_receive(:succeed).with("mytube")
  #
  #  conn.received("WATCHING 24\r\nUSING mytube\r\n")
  #end
  #
  #it 'should handle receiving data in chunks' do
  #  conn = EM::Beanstalk.new
  #  
  #  msg1 = "First half of the message\r\n"
  #  msg2 = "Last half of the message"
  #  
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with do |job|
  #    job.body.should == "#{msg1}#{msg2}"
  #  end
  #
  #  conn.received("RESERVED 9 #{(msg1 + msg2).length}\r\n#{msg1}")
  #  conn.received("#{msg2}\r\n")
  #end
  #
  #it 'should send the stat command' do
  #  @connection_mock.should_receive(:send).once.with(:stats)
  #  conn = EM::Beanstalk.new
  #  conn.stats
  #end
  #
  #it 'should handle receiving the OK command' do
  #  conn = EM::Beanstalk.new
  #
  #  msg =<<-HERE
#---
#current-jobs-urgent: 42
#current-jobs-ready: 92
#current-jobs-reserved: 18
#current-jobs-delayed: 7
#current-jobs-buried: 0
#pid: 416
#version: dev
#HERE
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with do |stats|
  #    stats['current-jobs-urgent'].should == 42
  #    stats['current-jobs-ready'].should == 92
  #    stats['current-jobs-reserved'].should == 18
  #    stats['current-jobs-delayed'].should == 7
  #    stats['current-jobs-buried'].should == 0
  #    stats['pid'].should == 416
  #    stats['version'].should == 'dev'
  #  end
  #
  #  conn.received("OK #{msg.length}\r\n#{msg}\r\n")
  #end
  #
  #it 'should support job stats' do
  #  job = EM::Beanstalk::Job.new(nil, 42, "blah")
  #
  #  @connection_mock.should_receive(:send).once.with(:'stats-job', 42)
  #  conn = EM::Beanstalk.new
  #  conn.stats(:job, job)
  #end
  #
  #it 'should support tube stats' do
  #  @connection_mock.should_receive(:send).once.with(:'stats-tube', "mytube")
  #  conn = EM::Beanstalk.new
  #  conn.stats(:tube, "mytube")
  #end
  #
  #it 'should throw exception on invalid stats command' do
  #  @connection_mock.should_not_receive(:send)
  #  conn = EM::Beanstalk.new
  #  lambda { conn.stats(:blah) }.should raise_error(EM::Beanstalk::InvalidCommand)
  #end
  #
  #it 'should support listing tubes' do
  #  @connection_mock.should_receive(:send).once.with(:'list-tubes')
  #  conn = EM::Beanstalk.new
  #  conn.list
  #end
  #
  #it 'should support listing tube used' do
  #  @connection_mock.should_receive(:send).once.with(:'list-tube-used')
  #  conn = EM::Beanstalk.new
  #  conn.list(:used)
  #end
  #
  #it 'should support listing tubes watched' do
  #  @connection_mock.should_receive(:send).once.with(:'list-tubes-watched')
  #  conn = EM::Beanstalk.new
  #  conn.list(:watched)
  #end
  #
  #it 'should throw exception on invalid list command' do
  #  @connection_mock.should_not_receive(:send)
  #  conn = EM::Beanstalk.new
  #  lambda { conn.list(:blah) }.should raise_error(EM::Beanstalk::InvalidCommand)
  #end
  #
  #it 'should accept a response broken over multiple packets' do
  #  conn = EM::Beanstalk.new
  #
  #  msg1 = "First half of the message\r\n"
  #  msg2 = "Last half of the message"
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with do |job|
  #    job.body.should == "#{msg1}#{msg2}"
  #  end
  #
  #  conn.received("RESERVED 9 ")
  #  conn.received("#{(msg1 + msg2).length}")
  #  conn.received("\r\n#{msg1}#{msg2}\r\n")
  #end
  #
  #it 'should accept a response broken over multiple packets' do
  #  conn = EM::Beanstalk.new
  #
  #  msg1 = "First half of the message\r\n"
  #  msg2 = "Last half of the message"
  #
  #  df = conn.add_deferrable
  #  df.should_receive(:succeed).with do |job|
  #    job.body.should == "#{msg1}#{msg2}"
  #  end
  #
  #  conn.received("RESERVED 9 #{(msg1 + msg2).length}\r\n#{msg1}#{msg2}")
  #  conn.received("\r\n")
  #end
end
