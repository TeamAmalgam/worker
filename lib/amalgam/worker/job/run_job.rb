class Amalgam::Worker::Job::RunJob < Amalgam::Worker::Job

end

Amalgam::Worker::Job.register_job('run', Amalgam::Worker::Job::RunJob)
