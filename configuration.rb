require 'aws-sdk'
require 'yaml'

class Configuration

  SETTINGS = [
    :access_key_id,
    :secret_access_key,
    :command,
    :s3_bucket,
    :sqs_queue_name,
    :server_base_url,
    :username,
    :password,
    :tmp_dir,
    :git_repo,
    :ssh_key,
    :seed_repo_path,
    :worker_timeout
  ]

  MANDATORY_SETTINGS = [
    :access_key_id,
    :secret_access_key,
    :command,
    :s3_bucket,
    :sqs_queue_name,
    :server_base_url,
    :git_repo
  ]

  SECONDS_PER_SECOND = 1
  SECONDS_PER_MINUTE = 60
  MINUTES_PER_HOUR = 60
  SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR

  # Define a public accessor for each setting.
  # We don't use attr_reader because we want to synchronize the read.
  SETTINGS.each do |setting|
    define_method(setting) do
      @configuration_mutex.synchronize do
        return instance_variable_get("@#{setting}")
      end

    end
  end

  def initialize(config_file_path)
    @config_file_path = File.absolute_path(config_file_path)
    @configuration_mutex = Mutex.new

    self.update
  end

  def update
    @configuration_mutex.synchronize do
      load_configuration
      update_global_objects
    end
  end

  # Atomically reads multiple setting from the configuration.
  def read_multiple(settings)
    values_to_return = {}
   
    # Grab the configuration mutex to ensure atomic execution 
    @configuration_mutex.synchronize do
      settings.each do |setting|
        unless SETTINGS.include?(setting)
          raise "Setting #{setting} does not exist"
        end

        values_to_return[setting] = self.instance_variable_get("@#{setting}")
      end
    end

    return values_to_return
  end

  private

  def update_global_objects
    AWS.config(:access_key_id => @access_key_id,
               :secret_access_key => @secret_access_key)
  end

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

  def validate_configuration_hash(hash)
    # All hash keys must be settings
    hash.each_key do |key|
      unless SETTINGS.include?(key)
        raise "Unknown setting #{key} specified in the configuration file."
      end
    end

    # All mandatory settings must be provided.
    MANDATORY_SETTINGS.each do |setting|
      unless hash.has_key?(setting)
        raise "#{setting} was not specified in the configuration file."
      end
    end
  end
end
