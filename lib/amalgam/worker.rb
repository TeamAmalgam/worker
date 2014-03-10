require 'logger'
require_relative '../amalgam'

class Amalgam::Worker
  class << self
    def logger=(value)
      @logger = value
    end

    def logger
      @logger ||= Logger.new(STDERR)
      return @logger
    end
  end

  def initialize(configuration_path)
    @configuration = Amalgam::Worker::Configuration.new(configuration_path)
    @manager = Amalgam::Worker::Manager.new(@configuration)
  end

  def run
    @manager.run
  end

  def join
    @manager.join
  end

  def termination_request
    @manager.request_termination
  end

  def terminate_current_job
    @manager.terminate_job
  end

  def update_configuration
    @manager.update_configuration
  end
end

require_relative 'worker/manager'
require_relative 'worker/runner'
require_relative 'worker/job'
require_relative 'worker/queue'
require_relative 'worker/configuration'
require_relative 'worker/heartbeater'
require_relative 'worker/downloader'
require_relative 'worker/uploader'
