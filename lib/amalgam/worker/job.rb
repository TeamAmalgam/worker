class Amalgam::Worker::Job

  def initialize(description)
    raise "Attempt to create abstract job."
  end

  def run
    raise "Attempt to run abstract job."
  end

  def terminate
    raise "Attempt to terminate abstract job."
  end

  class << self
    def register_job(identifier, klass)
      @jobs ||= {}

      unless @jobs[identifier].nil?
        raise "Job with identifier '#{identifier}' already registered."
      end

      @jobs[identifier] = klass
    end

    def unregister_job(identifier)
      @jobs[identifier] = nil
    end

    def create(job_description, configuration)
      jobs = @jobs || {}

      job_identifier = job_description[:job_type]
     
      if job_identifier.nil?
        raise "Job description does not specify a job type."
      end

      if @jobs[job_identifier].nil?
        raise "No job type registered for identifier '#{job_identifier}'"
      end

      return @jobs[job_identifier].new(job_description, configuration)
    end
  end

end

require_relative 'job/build_job'
require_relative 'job/run_job'
