class Amalgam::Worker::Job::BuildJob < Amalgam::Worker::Job
    def initialize(job_description, configuration)
      Amalgam::Worker.logger.info("Creating BuildJob")
      validate_job(job_description)

      @job_description = job_description
      @configuration = configuration
    end

    def run
      run_main
    end

    def terminate
      # Need to implement
    end

    private

    REQUIRED_PARAMETERS = [
      :commit
    ]

    def validate_job(job_description)
      REQUIRED_PARAMETERS.each do |param|
        if job_description[param].nil?
          raise "Job missing required parameter: #{param}"
        end
      end
    end

    def run_with_ssh_key(cmd)
      `ssh-agent bash -c 'ssh-add #{@configuration.ssh_key}; #{cmd}'`
    end

    def run_main
      Amalgam::Worker.logger.info("Running BuildJob")
      working_directory = Dir.getwd

      # Clone the repo.
      repo_url = @configuration.git_repo
      clone_results = run_with_ssh_key("git clone #{repo_url} moolloy")
      if $? != 0
        return {
          :return_code => $?,
          :error_message => "Failed to clone git repo.",
          :error_details => clone_results
        }
      end

      begin
        Dir.chdir(File.join(working_directory, "moolloy"))

        # Checkout the commit we want.
        commit = @job_description[:commit]
        checkout_results = `git checkout #{commit}`
        if $? != 0
          return {
            :return_code => $?,
            :error_message => "Failed to checkout commit.",
            :error_details => checkout_results
          }
        end

        # Git submodule init / update

        submodule_results = `git submodule init`
        if $? != 0
          return {
            :return_code => $?,
            :error_message => "Failed to submodule init.",
            :error_details => submodule_results
          }
        end

        submodule_resuls = run_with_ssh_key('git submodule update')
        if $? != 0
          return {
            :return_code => $?,
            :error_message => "Failed to submodule update.",
            :error_details => submodule_results
          }
        end

        # ant dist

        build_results = `ant deps && ant configure && ant dist`
        if $? != 0
          return {
            :return_code => $?,
            :error_message => "Failed to build.",
            :error_details => build_results
          }
        end

        # Sanity test

        alloy_path = File.join(working_directory,
                               "moolloy",
                               "dist",
                               "alloy-dev.jar")
        unless File.exists?(alloy_path)
          return {
            :return_code => 255,
            :error_message => "Build failed to produce moolloy.",
            :error_details => "Unable to find moolloy at: #{alloy_path}"
          }
        end

        # Upload jar

        uploader = @configuration.uploader
        uploader.upload(alloy_path, "builds/#{commit}.jar")

        # Generate result

        return {
          :return_code => 0,
          :build_key => "builds/#{commit}.jar"
        }

      ensure
        Dir.chdir(working_directory)
      end
    end
end

Amalgam::Worker::Job.register_job(:build, Amalgam::Worker::Job::BuildJob)
