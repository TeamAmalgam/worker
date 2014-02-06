require 'fileutils'

class Amalgam::Worker::Runner

  attr_reader :result

  def initialize(configuration, job_description)
    @configuration = configuration
    @job_description = job_description
    @thread = nil
    @result = nil
  end

  def run
    @thread = Thread.new {
      thread_main
    }
  end

  def join
    @thread.join
    @thread = nil
  end

  def terminate

  end

  private

  def thread_main
    original_working_dir = Dir.getwd
    temp_dir = Dir.mktmpdir(configuration.tmp_dir)
    error_caught = fale

    begin
      @job = Amalgam::Worker::Job.create(@job_description)
      @result = @job.run
    rescue => err
      Amalgam::Worker.logger.error("Worker caught error.")
      Amalgam::Worker.logger.error(err.inspect)
      error_caught = true
      @result = { :return_code => 255 }
    end

    Dir.chdir(original_working_dir)
    unless error_caught
      Amalgam::Worker.logger.error("Leaving the job directory behind.")
      Amalgam::Worker.logger.error(temp_dir)
      FileUtils.rm_rf(temp_dir)
    end
  end

end
