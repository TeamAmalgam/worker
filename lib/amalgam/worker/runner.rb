require 'fileutils'
require 'tmpdir'

class Amalgam::Worker::Runner

  attr_reader :result

  def initialize(configuration, job_description)
    @configuration = configuration
    @job_description = job_description
    @thread = nil
    @result = nil
    @job = nil
    @running = false
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
    unless @job.nil?
      @job.terminate
    end
  end

  def running?
    return @running
  end

  private

  def thread_main
    begin
      @running = true

      original_working_dir = Dir.getwd
      
      temp_dir = Dir.mktmpdir(nil, @configuration.tmp_dir)
      Dir.chdir(temp_dir)

      error_caught = false

      begin
        @job = Amalgam::Worker::Job.create(@job_description, @configuration)
        @result = @job.run
      rescue => err
        Amalgam::Worker.logger.error("Worker caught error.")
        Amalgam::Worker.logger.error(err.inspect)
        Amalgam::Worker.logger.error(err.backtrace.join("\n"))

        error_caught = true
        @result = { :return_code => 255 }
      end

      if !@result[:return_code].nil? && @result[:return_code] != 0
        error_caught = true
      end

      Dir.chdir(original_working_dir)
      if error_caught
        Amalgam::Worker.logger.error("Leaving the job directory behind.")
        Amalgam::Worker.logger.error(temp_dir)
      else
        FileUtils.rm_rf(temp_dir)
      end
    ensure
      @running = false
    end
  end

end
