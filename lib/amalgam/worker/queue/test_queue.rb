class Amalgam::Worker::Queue::TestQueue

  def initialize(options, old_queue)
    Amalgam::Worker.logger.info("Created test queue")

    options ||= {}
    @items = []

    unless options[:start_items].nil?
      Amalgam::Worker.logger.info("Start items are:")
      options[:start_items].each do |item|
        Amalgam::Worker.logger.info("\t" + item.inspect)
      end 

      options[:start_items].each do |item|
        enqueue(item)
      end
    end
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