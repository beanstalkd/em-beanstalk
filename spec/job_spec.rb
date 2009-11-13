require 'spec/spec_helper'

describe EMJack::Job do
  it 'should convert id to an integer' do
    j = EMJack::Job.new(nil, "123", "body")
    j.id.should == 123
  end
  
  it 'should fail if id is not an integer' do
    proc {
      j = EMJack::Job.new(nil, "asd", "body")
    }.should raise_error
  end
  
  it 'should send a delete command to the connection' do
    conn = mock(:conn)
    conn.should_receive(:default_priority)
    conn.should_receive(:default_delay)
    conn.should_receive(:default_ttr)
    
    j = EMJack::Job.new(conn, 1, "body")

    conn.should_receive(:delete).with(j)
    
    j.delete
  end

  it 'should send a stats command to the connection' do
    conn = mock(:conn)
    conn.should_receive(:default_priority)
    conn.should_receive(:default_delay)
    conn.should_receive(:default_ttr)

    j = EMJack::Job.new(conn, 2, 'body')
    conn.should_receive(:stats).with(:job, j)

    j.stats
  end
end