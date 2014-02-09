class Amalgam::Worker::Manager
  
  def initialize(configuration)
    @configuration = configuration
    @thread = nil
    @time_of_last_heartbeat = nil
    @configuration_update_requested = false
    @termination_requested = false
  end

  def run
    @thread = Thread.new {
      thread_main
    }
  end

  def join
    unless @thread.nil?
      @thread.join
    end
    @thread = nil
  end

  def request_termination
    @termination_requested = true
  end

  def terminate_current_job
    @job_termination_requested = true
  end

  private

  def thread_main
    Amalgam::Worker.logger.info("Manager trying to register.")
    begin
      register
    rescue => err
      Amalgam::Worker.logger.error("Manager failed to register.")
      Amalgam::Worker.logger.error(err.inspect)
      Amalgam::Worker.logger.error(err.backtrace.join("\n"))
      return
    end

    Amalgam::Worker.logger.info("Manager registered successfully.")

    begin
      while (!@termination_requested)
        Amalgam::Worker.logger.info("Polling for job.")
        job = poll_for_job

        unless job.nil?
          Amalgam::Worker.logger.info("Got job.")
          run_job(job)
        end
      end
    rescue => err
      Amalgam::Worker.logger.error("Manager encountered exception.")
      Amalgam::Worker.logger.error(err.inspect)
      Amalgam::Worker.logger.error(err.backtrace.join("\n"))
    ensure
      Amalgam::Worker.logger.info("Manager unregistering.")
      unregister
    end
  end

  def register
    heartbeater = @configuration.heartbeater
    heartbeater.register
  end

  def unregister
    heartbeater = @configuration.heartbeater
    heartbeater.unregister
  end

  def poll_for_job
    result = nil

    while (result.nil? && !@termination_requested)
      queue = @configuration.queue
      result = queue.poll

      maybe_do_heartbeat
      maybe_update_configuration
    end

    return result
  end

  def maybe_do_heartbeat(job_id = nil)
    if (@time_of_last_heartbeat.nil? ||
        seconds_since_last_heartbeat >= @configuration.heartbeat_period)

      heartbeater = @configuration.heartbeater
      heartbeater.heartbeat(job_id)

      @time_of_last_heartbeat = Time.now
    end
  end

  def maybe_update_configuration
    if @configuration_update_requested
      @configuration.reload
    end
  end

  def seconds_since_last_heartbeat
    return (Time.now - @time_of_last_heartbeat)
  end

  def run_job(job_description)
    job_id = job_description[:job_id]
    Amalgam::Worker.logger.info("Signalling start of job: #{job_id}")
    @configuration.heartbeater.signal_start(job_id)

    # Ensure that a previous job termination will not carry-over
    @job_termination_requested = false

    @runner = Amalgam::Worker::Runner.new(@configuration, job_description)
    @runner.run

    @job_start = Time.now

    while (@runner.running?)
      maybe_do_heartbeat(job_id)
      maybe_update_configuration
      maybe_terminate_job

      sleep(@configuration.sleep_interval)
    end

    @runner.join
    result = {
      :secret_key => job_description[:secret_key]
    }.merge(@runner.result)
    
    Amalgam::Worker.logger.info("Job Completed, Result: #{result}")
    Amalgam::Worker.logger.info("Signalling compleion of job: #{job_id}")
    @configuration.heartbeater.signal_completion(job_id, result)
  end

  def seconds_since_job_start
    return (Time.now - @job_start)
  end

  def maybe_terminate_job
    Amalgam::Worker.logger.info("Job has timed-out.")
    if (seconds_since_job_start >=
          @configuration.worker_timeout ||
        @job_termination_requested)
      @runner.terminate
      @job_termination_requested = false
    end
  end
end
