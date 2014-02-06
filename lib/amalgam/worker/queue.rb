class Amalgam::Worker::Queue
  def poll
    raise "Attempt to poll an abstract queue."
  end

  class << self
    def register_queue(identifier, klass)
      @queues ||= {}

      unless @queues[identifier].nil?
        raise "Queue with identiier '#{identifier}' already registered."
      end

      @queues[identifier] = klass
    end

    def unregister_queue(identifier)
      @queues[identifier] = nil
    end

    def create(identifier, options, previous_queue = nil)
      queues = @queues || {}

      if queues[identifier].nil?
        raise "No queue type registered for identifier '#{identifier}'."
      end

      return queues[identifier].new(options, previous_queue)
    end
  end
end

require_relative 'queue/sqs_queue'
require_relative 'queue/test_queue'
