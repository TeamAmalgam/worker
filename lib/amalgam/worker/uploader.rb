class Amalgam::Worker::Uploader
  def initialize
    raise "Attempt to initialize abstract Uploader."
  end

  def upload(src_path, key)
    raise "Attempt to invoke abstract method upload."
  end

  class << self
    def register_uploader(identifier, klass)
      @uploaders ||= {}

      unless @uploaders[identifier].nil?
        raise "Uploader with identifier '#{identifier}' already registered."
      end

      @uploaders[identifier] = klass
    end

    def unregister_uploader(identifier)
      @uploaders[identifier] = nil
    end

    def create(identifier, options, previous_uploader = nil) 
      uploaders = @uploaders || {}

      if uploaders[identifier].nil?
        raise "No uploader type registered for identifier '#{identifier}'."
      end

      return uploaders[identifier].new(options, previous_uploader)
    end
  end
end

require_relative "uploader/test_uploader"
require_relative "uploader/s3_uploader"
