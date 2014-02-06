class Amalgam::Worker::Queue::SqsQueue
  def initialize(sqs_queue_name, idle_timeout)
    sqs_client = AWS::SQS.new
    @sqs_queue = sqs_client.queues.named(sqs_queue_name)
    @idle_timeout = idle_timeout
  end

  def poll
    message = @sqs_queue.poll(:idle_timeout => @idle_timeout)

    return nil if message.nil?
    return YAML.safe_load(message.body)
  end
end
