class Amalgam::Worker::Queue
  def poll
    raise "Attempt to poll an abstract queue."
  end
end

require_relative 'queue/sqs_queue'
require_relative 'queue/test_queue'
