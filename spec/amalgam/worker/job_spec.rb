require 'amalgam/worker'
require 'logger'

class SampleJob < Amalgam::Worker::Job
  def initialize(job_description, configuration)

  end

  def run
  end
end

describe Amalgam::Worker::Job do
  before :all do 
    Amalgam::Worker.logger = Logger.new('/dev/null')
    Amalgam::Worker::Job.unregister_job('asdf')
  end

  after :each do
    Amalgam::Worker::Job.unregister_job('asdf')
  end

  after :all do
    Amalgam::Worker.logger = nil
  end

  it "disallows duplicate identifier registrations" do
    Amalgam::Worker::Job.register_job('asdf', SampleJob)

    expect {
      Amalgam::Worker::Job.register_job('asdf', SampleJob)
    }.to raise_error
  end

  it "creates the correctly registered job" do
    Amalgam::Worker::Job.register_job('asdf', SampleJob) 
    job = Amalgam::Worker::Job.create({:type => 'asdf'}, nil)

    expect(job).to be_an_instance_of(SampleJob)
  end

  it "should raise an error when creating an unregistered job" do
    expect {
      Amalgam::Worker::Job.create({:type => 'asdf'}, nil)
    }.to raise_error
  end
end
