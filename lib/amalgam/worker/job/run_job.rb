class Amalgam::Worker::Job::RunJob < Amalgam::Worker::Job
    def initialize(job_description, configuration)
      Amalgam::Worker.logger.info("Creating RunJob")
      @job_description = job_description
      @configuration = configuration
    end

    def run
      Amalgam::Worker.logger.info("Running RunJob")
    end
end

Amalgam::Worker::Job.register_job(:run, Amalgam::Worker::Job::RunJob)
