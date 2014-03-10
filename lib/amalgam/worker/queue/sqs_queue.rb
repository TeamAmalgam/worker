class Amalgam::Worker::Queue::SqsQueue
  def initialize(options, old_queue)
    sqs_client = AWS::SQS.new
    @sqs_queue = sqs_client.queues.named(options[:sqs_queue])
  end

  def poll
    message = @sqs_queue.receive_message
  
    return nil if message.nil?

    message_body = YAML.safe_load(message.body)

    return nil if message_body[:version] != 2

    message_body[:secret_key] = message.id

    message.delete

    return message_body
  end
end

Amalgam::Worker::Queue.register_queue(
    :sqs,
    Amalgam::Worker::Queue::SqsQueue
)
