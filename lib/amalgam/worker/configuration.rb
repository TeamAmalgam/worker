require 'aws-sdk'

class Amalgam::Worker::Configuration

  SECONDS_PER_SECOND = 1
  SECONDS_PER_MINUTE = 60
  MINUTES_PER_HOUR = 60
  SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR

  SETTINGS = [
    # AWS Credentials.
    :access_key_id,
    :secret_access_key,

    :tmp_dir, # The temporary directory to work in.
    :git_repo, # URL of the git repo containing moolloy.
    :ssh_key, # The ssh key to use when cloning the repo.
    :worker_timeout, # Maximum amount of time a worker will spec on a job.
    :heartbeat_period, # Time between heartbeats.
    :sleep_interval, # Time to sleep between worker checks.
    :idle_timeout, # Time to spend polling for jobs before checking other state

    :heartbeater_type, # The type of heartbeater to use.
    :uploader_type, # The type of uploader to use.
    :downloader_type, # The type of downloader to use.
    :queue_type, # The type of queue to use.

    :heartbeater_options, # Options to pass to the heartbeater.
    :uploader_options, # Options to pass to the uploader.
    :downloader_options, # Options to pass to the downloader.
    :queue_options # Options to pass to the queue.
  ]

  MANDATORY_SETTINGS = [
    :access_key_id,
    :secret_access_key,
    :git_repo,
    :heartbeater_type,
    :uploader_type,
    :downloader_type,
    :queue_type,
    :heartbeater_options,
    :uploader_options,
    :downloader_options,
    :queue_options
  ]

  NON_MANDATORY_SETTINGS = SETTINGS - MANDATORY_SETTINGS

  SETTING_DEFAULTS = {
    :worker_timeout   => 48 * SECONDS_PER_HOUR,
    :heartbeat_period => 5  * SECONDS_PER_MINUTE,
    :sleep_interval   => 15 * SECONDS_PER_SECOND,
    :tmp_dir          => nil,
    :idle_timeout     => 2  * SECONDS_PER_MINUTE,
    :ssh_key          => nil
  }

  # All Non-Mandatory settings must have an entry in the
  # SETTING_DEFAULTS hash (even if they are nil by default)
  # to ensure we have considered what happens by default
  # for the Non-Mandatory settings.
  NON_MANDATORY_SETTINGS.each do |name|
    unless SETTING_DEFAULTS.has_key?(name)
      raise "No default for non-mandatory setting #{name}"
    end
  end

  ATTRIBUTES = SETTINGS + [
    :uploader,
    :downloader,
    :queue,
    :heartbeater
  ]


  ATTRIBUTES.each do |name|
    define_method(name) do
      @configuration_mutex.synchronize {
        return instance_variable_get("@#{name}")
      }
    end
  end

  def initialize(config_file_path)
    ATTRIBUTES.each do |name|
      default_value = SETTING_DEFAULTS[name]
      instance_variable_set("@#{name}", default_value)
    end


    @config_file_path = File.absolute_path(config_file_path)
    @configuration_mutex = Mutex.new
    self.reload
  end

  def reload
    @configuration_mutex.synchronize {
      load_configuration
      update_global_objects
      update_configuration_objects
    }
  end

  private

  def load_configuration
    conf = YAML.safe_load(File.read(@config_file_path))
    validate_configuration_hash(conf)

    conf.each do |key, value|
      self.instance_variable_set("@#{key}", value)
    end

    if @worker_timeout.is_a?(Hash)
      @worker_timeout = SECONDS_PER_HOUR   * (@worker_timeout[:hours] || 0) +
                        SECONDS_PER_MINUTE * (@worker_timeout[:minutes] || 0) +
                        SECONDS_PER_SECOND * (@worker_timeout[:seconds] || 0)
    end
  end

  def update_global_objects
    ::AWS.config(:access_key_id => @access_key_id,
                 :secret_access_key => @secret_access_key)
  end

  def update_configuration_objects
    @uploader = Amalgam::Worker::Uploader.create(@uploader_type,
                                                 @uploader_options,
                                                 @uploader)
    @downloader = Amalgam::Worker::Downloader.create(@downloader_type,
                                                     @downloader_options,
                                                     @downloader)
    @queue = Amalgam::Worker::Queue.create(@queue_type, @queue_options, @queue)
    @heartbeater = Amalgam::Worker::Heartbeater.create(@heartbeater_type,
                                                       @heartbeater_options,
                                                       @heartbeater)
  end

  def validate_configuration_hash(hash)
    # All hash keys provided must be settings
    hash.each_key do |key|
      unless SETTINGS.include?(key)
        raise "Unknown setting #{key} specified in the configuration file."
      end
    end
    
    # All mandatory settings must be provided
    MANDATORY_SETTINGS.each do |setting|
      unless hash.has_key?(setting)
        raise "#{setting} was not specified in the configuration file."
      end
    end
  end
end
