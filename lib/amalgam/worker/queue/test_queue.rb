class Amalgam::Worker::Queue::TestQueue

  def initialize(options, old_queue)
    @items = []
  end

  def enqueue(job_description)
    @items << job_description
  end

  def poll
    @items.shift
  end

end

Amalgam::Worker::Queue.register_queue(
    :test,
    Amalgam::Worker::Queue::TestQueue
)