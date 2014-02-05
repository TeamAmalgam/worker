class Amalgam::Worker::Queue::TestQueue

  def initialize
    @items = []
  end

  def enqueue(job_description)
    @items << job_description
  end

  def poll
    @items.shift
  end

end
