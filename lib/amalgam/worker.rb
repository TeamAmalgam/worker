require_relative '../amalgam'

class Amalgam::Worker
  def initialize

  end

  def run

  end
end

require_relative 'worker/manager'
require_relative 'worker/runner'
require_relative 'worker/job'
require_relative 'worker/queue'
require_relative 'worker/configuration'
require_relative 'worker/heartbeater'
