class Amalgam::Worker::Job::BuildJob < Amalgam::Worker::Job
    def initialize(job_description)
      Amalgam::Worker.logger.info("Creating BuildJob")
    end

    def run
      Amalgam::Worker.logger.info("Running BuildJob")
    end
end

Amalgam::Worker::Job.register_job(:build, Amalgam::Worker::Job::BuildJob)
