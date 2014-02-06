class Amalgam::Worker::Runner
  def initialize(configuration, job_description)
    @thread = nil
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

  private

  def thread_main

  end
end
