class Amalgam::Worker::Job::RunJob < Amalgam::Worker::Job
    def initialize(job_description)
      Amalgam::Worker.logger.info("Creating RunJob")
    end

    def run
      Amalgam::Worker.logger.info("Running RunJob")
    end
end

Amalgam::Worker::Job.register_job(:run, Amalgam::Worker::Job::RunJob)
