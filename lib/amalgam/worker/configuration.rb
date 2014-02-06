class Amalgam::Worker::Configuration

  SECONDS_PER_SECOND = 1
  SECONDS_PER_MINUTE = 60
  MINUTES_PER_HOUR = 60
  SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR

  SETTINGS = [
    :access_key_id,
    :secret_access_key,
    :s3_bucket,
    :sqs_queue_name,
    :server_base_url,
    :username,
    :password,
    :tmp_dir,
    :git_repo,
    :ssh_key,
    :worker_timeout,
    :heartbeat_period,
    :sleep_interval
  ]

  MANDATORY_SETTINGS = [
    :access_key_id,
    :secret_access_key,
    :s3_bucket,
    :sqs_queue_name,
    :server_base_url,
    :git_repo,
    :heartbeat_period,
  ]

  NON_MANDATORY_SETTINGS = SETTINGS - MANDATORY_SETTINGS

  SETTING_DEFAULTS = {
    :worker_timeout   => 48 * SECONDS_PER_HOUR,
    :heartbeat_period => 5  * SECONDS_PER_MINUTE,
    :sleep_interval   => 15 * SECONDS_PER_SECOND,
    :tmp_dir          => "/tmp",
    :username         => nil,
    :password         => nil,
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
    @attributes.each do |name|
      instance_variable_set("@#{name}", nil)
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
    AWS.config(:access_key_id => @access_key_id,
               :secret_access_key => @secret_access_key)
  end

  def update_configuration_objects
    @uploader = Amalgam::Worker::Uploader.new(@s3_bucket)
    @downloader = Amalgam::Worker::Downloader.new(@s3_bucket)
    @queue = Amalgam::Worker::Queue::SqsQueue.new(@sqs_queue_name)
    @heartbeater = Amalgam::Worker::Heartbeater.new(@server_base_url,
                                                    @username,
                                                    @password,
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
