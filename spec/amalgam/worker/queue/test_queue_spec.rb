require 'amalgam/worker'

describe Amalgam::Worker::Queue::TestQueue do

  before :all do
    @queue = Amalgam::Worker::Queue::TestQueue.new
  end

  it 'should return the job descriptions added in order' do
    a = {:type => 'build'}
    b = {:type => 'test'}
    @queue.enqueue(a)
    @queue.enqueue(b)

    expect(@queue.poll).to eq(a)
    expect(@queue.poll).to eq(b)
  end

  it 'should return nil if there is no job available' do
    expect(@queue.poll).to be_nil
  end
end
