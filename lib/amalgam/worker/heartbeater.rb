class Amalgam::Worker::Heartbeater
  
  attr_reader :worker_id

  def initialize
    raise "Attempt to initialize abstract Heartbeater."
  end


  def register
    raise "Attempt to invoke abstract method register."
  end

  def heartbeat(current_job_id = nil)
    raise "Attempt to invoke abstract method heartbeat."
  end

  def signal_start(job_id)
    raise "Attempt to invoke abstract method signal_start."
  end

  def signal_completion(job_id, result_body)
    raise "Attempt to invoke abstract method signal_completion."
  end

  def unregister
    raise "Attempt to invoke abstract method unregister."
  end

  class << self
    def register_heartbeater(identifier, klass)
      @heartbeaters ||= {}

      unless @heartbeaters[identifier].nil?
        raise "Heartbeater with identifier '#{identifier}' already registered."
      end

      @heartbeaters[identifier] = klass
    end

    def unregister_queue(identifier)
      @heartbeaters[identifier] = nil
    end

    def create(identifier, options, previous_queue = nil)
      heartbeaters = @heartbeaters || {}

      if heartbeaters[identifier].nil?
        raise "No heartbeater type registered for identifier '#{identifier}'."
      end

      return heartbeaters[identifier].new(options, previous_queue)
    end
  end
end

require_relative "heartbeater/http_heartbeater"
require_relative "heartbeater/test_heartbeater"
