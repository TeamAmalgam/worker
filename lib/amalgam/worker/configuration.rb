class Amalgam::Worker::Configuration

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
    :worker_timeout
  ]

  MANDATORY_SETTINGS = [
    :access_key_id,
    :secret_access_key,
    :s3_bucket,
    :sqs_queue_name,
    :server_base_url,
    :git_repo
  ]

  ATTRIBUTES = SETTINGS + [
    :uploader,
    :downloader,
    :queue,
    :heartbeater
  ]

  SECONDS_PER_SECOND = 1
  SECONDS_PER_MINUTE = 60
  MINUTES_PER_HOUR = 60
  SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR

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
    @heartbeater = Amalgam::Worker::Heartbeater.new(@server_base_url, @username, @password)
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
