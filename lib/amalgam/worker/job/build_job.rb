class Amalgam::Worker::Job::BuildJob < Amalgam::Worker::Job
    def initialize(job_description, configuration)
      Amalgam::Worker.logger.info("Creating BuildJob")
      @job_description = job_description
      @configuration = configuration
    end

    def run
      Amalgam::Worker.logger.info("Running BuildJob")
    end
end

Amalgam::Worker::Job.register_job(:build, Amalgam::Worker::Job::BuildJob)
