class Amalgam::Worker::Heartbeater::TestHeartbeater
  
  attr_reader :worker_id

  def initialize(options, old_heartbeater = nil)
    @worker_id = nil

    unless old_heartbeater.nil?
      @worker_id = old_heartbeater.worker_id
    end
  end

  def register
    @worker_id = 1234
  end

  def heartbeat(current_job_id = nil)
  end

  def signal_start(job_id)
  end

  def signal_completion(job_id, result_body)
  end

  def unregister
    @worker_id = nil
  end

end

Amalgam::Worker::Heartbeater.register_heartbeater(
  :test,
  Amalgam::Worker::Heartbeater::TestHeartbeater
)