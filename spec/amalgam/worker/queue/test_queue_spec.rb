require 'amalgam/worker'
require 'logger'

describe Amalgam::Worker::Queue::TestQueue do

  before :all do
    Amalgam::Worker.logger = Logger.new('/dev/null')
    @queue = Amalgam::Worker::Queue.create(:test, nil)
  end

  after :all do
    Amalgam::Worker.logger = nil
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
