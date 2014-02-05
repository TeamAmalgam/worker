require 'amalgam/worker'

class SampleJob < Amalgam::Worker::Job
  def initialize(job_description)

  end

  def run
  end
end

describe Amalgam::Worker::Job do
  before :all do 
    Amalgam::Worker::Job.unregister_job('asdf')
  end

  after :each do
    Amalgam::Worker::Job.unregister_job('asdf')
  end

  it "disallows duplicate identifier registrations" do
    Amalgam::Worker::Job.register_job('asdf', SampleJob)

    expect {
      Amalgam::Worker::Job.register_job('asdf', SampleJob)
    }.to raise_error
  end

  it "creates the correctly registered job" do
    Amalgam::Worker::Job.register_job('asdf', SampleJob) 
    job = Amalgam::Worker::Job.create({:type => 'asdf'})

    expect(job).to be_an_instance_of(SampleJob)
  end

  it "should raise an error when creating an unregistered job" do
    expect {
      Amalgam::Worker::Job.create({:type => 'asdf'})
    }.to raise_error
  end
end
