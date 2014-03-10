class Amalgam::Worker::Downloader
  def initialize
    raise "Attempt to initialize abstract Downloader."
  end

  def download(key, destination_path)
    raise "Call to abstract Downloader download."
  end

  class << self
    def register_downloader(identifier, klass)
      @downloaders ||= {}

      unless @downloaders[identifier].nil?
        raise "Downloader with identifier '#{identifier} already registered."
      end

      @downloaders[identifier] = klass
    end

    def unregister_downloader(identifier)
      @downloaders[identifier] = nil
    end

    def create(identifier, options, previous_downloader = nil)
      downloaders = @downloaders || {}

      if downloaders[identifier].nil?
        raise "No downloader type registered for identifier '#{identifier}."
      end

      return downloaders[identifier].new(options, previous_downloader)
    end
  end
end

require_relative "downloader/test_downloader"
require_relative "downloader/s3_downloader"
