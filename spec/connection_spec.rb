require 'spec/spec_helper'

describe EM::Beanstalk do
  before(:each) do
    @connection_mock = mock(:conn)
    EM.should_receive(:connect).and_return(@connection_mock)
  end

  it 'should use a default host of "localhost"' do
    conn = EM::Beanstalk.new
    conn.host.should == 'localhost'
  end

  it 'should use a default port of 11300' do
    conn = EM::Beanstalk.new
    conn.port.should == 11300    
  end

  it 'should watch and use a provided tube on connect' do
    @connection_mock.should_receive(:send).once.with(:use, "mytube")
    @connection_mock.should_receive(:send).once.with(:watch, "mytube")
    conn = EM::Beanstalk.new(:tube => "mytube")
  end

  it 'should send the "use" command' do
    @connection_mock.should_receive(:send).once.with(:use, "mytube")
    conn = EM::Beanstalk.new
    conn.use("mytube")
  end

  it 'should not send the use command to the currently used tube' do
    @connection_mock.should_receive(:send).once.with(:use, "mytube")
    conn = EM::Beanstalk.new
    conn.use("mytube")
    conn.use("mytube")
  end

  it 'should send the "watch" command' do
    @connection_mock.should_receive(:send).once.with(:watch, "mytube")
    conn = EM::Beanstalk.new
    conn.watch("mytube")
  end

  it 'should not send the watch command for a tube currently watched' do
    @connection_mock.should_receive(:send).once.with(:watch, "mytube")
    conn = EM::Beanstalk.new
    conn.watch("mytube")
    conn.watch("mytube")
  end

  it 'should send the "put" command' do
    msg = "my message"
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, msg, anything, anything, anything, msg.length)
    conn = EM::Beanstalk.new
    conn.put(msg)
  end

  it 'should default the delay, priority and ttr settings' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, 65536, 0, 300, anything)
    conn = EM::Beanstalk.new
    conn.put("msg")
  end

  it 'should accept a delay setting' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, anything, 42, anything, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :delay => 42)
  end

  it 'should accept a ttr setting' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, anything, anything, 999, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :ttr => 999)
  end

  it 'should accept a priority setting' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, 233, anything, anything, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :priority => 233)
  end

  it 'shoudl accept a priority, delay and ttr setting' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, 99, 42, 2000, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :priority => 99, :delay => 42, :ttr => 2000)
  end

  it 'should force delay to be >= 0' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, anything, 0, anything, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :delay => -42)
  end

  it 'should force ttr to be >= 0' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, anything, anything, 300, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :ttr => -42)
  end

  it 'should force priority to be >= 0' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, 65536, anything, anything, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :priority => -42)
  end

  it 'should force priority to be < 2**32' do
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, anything, (2 ** 32), anything, anything, anything)
    conn = EM::Beanstalk.new
    conn.put("msg", :priority => (2 ** 32 + 1))
  end

  it 'should handle a non-string provided as the put message' do
    msg = 22
    @connection_mock.should_receive(:send_with_data).once.
                      with(:put, msg.to_s, anything, anything, anything, msg.to_s.length)
    conn = EM::Beanstalk.new
    conn.put(msg) 
  end

  it 'should send the "delete" command' do
    @connection_mock.should_receive(:send).once.with(:delete, 1)
    job = EM::Beanstalk::Job.new(nil, 1, "body")
    conn = EM::Beanstalk.new
    conn.delete(job)
  end

  it 'should handle a nil job sent to the "delete" command' do
    @connection_mock.should_not_receive(:send).with(:delete, nil)
    conn = EM::Beanstalk.new
    conn.delete(nil)
  end

  it 'should send the "reserve" command' do
    @connection_mock.should_receive(:send).with(:reserve)
    conn = EM::Beanstalk.new
    conn.reserve
  end

  it 'should raise exception if reconnect fails more then RETRY_COUNT times' do
    EM.should_receive(:add_timer).exactly(5).times

    conn = EM::Beanstalk.new
    5.times { conn.disconnected }
    lambda { conn.disconnected }.should raise_error(EM::Beanstalk::Disconnected)
  end

  it 'should reset the retry count on connection' do
    EM.should_receive(:add_timer).at_least(1).times

    conn = EM::Beanstalk.new
    5.times { conn.disconnected }
    conn.connected
    lambda { conn.disconnected }.should_not raise_error(EM::Beanstalk::Disconnected)
  end

  %w(OUT_OF_MEMORY INTERNAL_ERROR DRAINING BAD_FORMAT
     UNKNOWN_COMMAND EXPECTED_CRLF JOB_TOO_BIG DEADLINE_SOON
     TIMED_OUT NOT_FOUND).each do |cmd|
    it "should handle #{cmd} messages" do
       conn = EM::Beanstalk.new

       df = conn.add_deferrable
       df.should_receive(:fail).with(cmd.downcase.to_sym)

       conn.received("#{cmd}\r\n")
     end
  end

  it 'should handle deleted messages' do
    conn = EM::Beanstalk.new

    df = conn.add_deferrable
    df.should_receive(:succeed)

    conn.received("DELETED\r\n")
  end

  it 'should handle inserted messages' do
    conn = EM::Beanstalk.new

    df = conn.add_deferrable
    df.should_receive(:succeed).with(40)

    conn.received("INSERTED 40\r\n")
  end

  it 'should handle buried messages' do
    conn = EM::Beanstalk.new

    df = conn.add_deferrable
    df.should_receive(:fail).with(:buried, 40)

    conn.received("BURIED 40\r\n")
  end

  it 'should handle using messages' do
    conn = EM::Beanstalk.new

    df = conn.add_deferrable
    df.should_receive(:succeed).with("mytube")

    conn.received("USING mytube\r\n")
  end

  it 'should handle watching messages' do
    conn = EM::Beanstalk.new

    df = conn.add_deferrable
    df.should_receive(:succeed).with(24)

    conn.received("WATCHING 24\r\n")
  end

  it 'should handle reserved messages' do
    conn = EM::Beanstalk.new

    msg = "This is my message"

    df = conn.add_deferrable
    df.should_receive(:succeed).with do |job|
      job.class.should == EM::Beanstalk::Job
      job.jobid.should == 42
      job.body.should == msg
    end

    conn.received("RESERVED 42 #{msg.length}\r\n#{msg}\r\n")
  end

  it 'should handle receiving multiple replies in one packet' do
    conn = EM::Beanstalk.new

    df = conn.add_deferrable
    df.should_receive(:succeed).with(24)

    df2 = conn.add_deferrable
    df2.should_receive(:succeed).with("mytube")

    conn.received("WATCHING 24\r\nUSING mytube\r\n")
  end

  it 'should handle receiving data in chunks' do
    conn = EM::Beanstalk.new
    
    msg1 = "First half of the message\r\n"
    msg2 = "Last half of the message"
    
    df = conn.add_deferrable
    df.should_receive(:succeed).with do |job|
      job.body.should == "#{msg1}#{msg2}"
    end

    conn.received("RESERVED 9 #{(msg1 + msg2).length}\r\n#{msg1}")
    conn.received("#{msg2}\r\n")
  end
  
  it 'should send the stat command' do
    @connection_mock.should_receive(:send).once.with(:stats)
    conn = EM::Beanstalk.new
    conn.stats
  end

  it 'should handle receiving the OK command' do
    conn = EM::Beanstalk.new

    msg =<<-HERE
---
current-jobs-urgent: 42
current-jobs-ready: 92
current-jobs-reserved: 18
current-jobs-delayed: 7
current-jobs-buried: 0
pid: 416
version: dev
HERE

    df = conn.add_deferrable
    df.should_receive(:succeed).with do |stats|
      stats['current-jobs-urgent'].should == 42
      stats['current-jobs-ready'].should == 92
      stats['current-jobs-reserved'].should == 18
      stats['current-jobs-delayed'].should == 7
      stats['current-jobs-buried'].should == 0
      stats['pid'].should == 416
      stats['version'].should == 'dev'
    end

    conn.received("OK #{msg.length}\r\n#{msg}\r\n")
  end

  it 'should support job stats' do
    job = EM::Beanstalk::Job.new(nil, 42, "blah")

    @connection_mock.should_receive(:send).once.with(:'stats-job', 42)
    conn = EM::Beanstalk.new
    conn.stats(:job, job)
  end

  it 'should support tube stats' do
    @connection_mock.should_receive(:send).once.with(:'stats-tube', "mytube")
    conn = EM::Beanstalk.new
    conn.stats(:tube, "mytube")
  end

  it 'should throw exception on invalid stats command' do
    @connection_mock.should_not_receive(:send)
    conn = EM::Beanstalk.new
    lambda { conn.stats(:blah) }.should raise_error(EM::Beanstalk::InvalidCommand)
  end

  it 'should support listing tubes' do
    @connection_mock.should_receive(:send).once.with(:'list-tubes')
    conn = EM::Beanstalk.new
    conn.list
  end

  it 'should support listing tube used' do
    @connection_mock.should_receive(:send).once.with(:'list-tube-used')
    conn = EM::Beanstalk.new
    conn.list(:used)
  end

  it 'should support listing tubes watched' do
    @connection_mock.should_receive(:send).once.with(:'list-tubes-watched')
    conn = EM::Beanstalk.new
    conn.list(:watched)
  end

  it 'should throw exception on invalid list command' do
    @connection_mock.should_not_receive(:send)
    conn = EM::Beanstalk.new
    lambda { conn.list(:blah) }.should raise_error(EM::Beanstalk::InvalidCommand)
  end

  it 'should accept a response broken over multiple packets' do
    conn = EM::Beanstalk.new

    msg1 = "First half of the message\r\n"
    msg2 = "Last half of the message"

    df = conn.add_deferrable
    df.should_receive(:succeed).with do |job|
      job.body.should == "#{msg1}#{msg2}"
    end

    conn.received("RESERVED 9 ")
    conn.received("#{(msg1 + msg2).length}")
    conn.received("\r\n#{msg1}#{msg2}\r\n")
  end

  it 'should accept a response broken over multiple packets' do
    conn = EM::Beanstalk.new

    msg1 = "First half of the message\r\n"
    msg2 = "Last half of the message"

    df = conn.add_deferrable
    df.should_receive(:succeed).with do |job|
      job.body.should == "#{msg1}#{msg2}"
    end

    conn.received("RESERVED 9 #{(msg1 + msg2).length}\r\n#{msg1}#{msg2}")
    conn.received("\r\n")
  end
end
