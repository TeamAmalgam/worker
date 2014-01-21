require 'fileutils'
class Runner

  def initialize(configuration)
    @configuration = configuration
    @termination_requested = false
    @current_test_result_id = nil
    @run_start_time = nil
    @worker_process_group = nil
  end

  def run(worker_id)
    @worker_id = worker_id
    @thread = Thread.new do
      catch(:termination) do
        while !@termination_requested
          get_sqs_queue.poll(:idle_timeout => 2 * 60) do |message|
            begin
              process_message(message)
            rescue Exception => e
              puts e.inspect
              raise e
            end
            throw :termination if @termination_requested
          end
        end
      end
    end
  end

  def terminated?
    !@thread.status
  end

  def request_termination
    @termination_requested = true
  end

  def terminate
    @termination_requested = true
    terminate_job
    @thread.exit
  end

  def join
    @thread.join
  end

  def current_test_result_id
    return @current_test_result_id
  end

  def run_start_time
    return @run_start_time
  end

  def terminate_job
    unless @worker_process_group.nil?
      Process.kill(-15, @worker_process_group)
    end
  end

private

  def process_message(message)
    puts "Received Job:"
    job_description = YAML.safe_load(message.body)
    puts job_description.inspect

    @current_test_result_id = job_description[:test_id]

    started_at = Time.now

    post_start(job_description[:test_id])

    results = nil
    tarball_s3_key = nil

    Dir.mktmpdir(nil, @configuration.tmp_dir) do |temp_dir|
      puts "Using temporary directory #{temp_dir}"
      Dir.chdir(temp_dir) do
        download_model(temp_dir, job_description[:model_s3_key])

        # Delete the message since we are about to do a lot of work
        # and we don't want another worker to pick it up.
        message.delete

        compile_moolloy(temp_dir, job_description[:commit])
        results = run_moolloy(temp_dir)
        tarball_s3_key = upload_results(temp_dir, message.id)
      end
    end

    post_results(job_description[:test_id],
                 message.id,
                 started_at,
                 results,
                 tarball_s3_key)

    puts "Finished processing job: #{message.id}."
    @current_test_result_id = nil
  end

  def download_model(temporary_directory, s3_key)
    obj = get_s3_bucket.objects[s3_key]
    tarball_filename = File.basename(s3_key)
    model_directory = File.join(temporary_directory, "model")

    Dir.mkdir(model_directory)
    Dir.chdir(model_directory) do
      # Download the model
      puts "Downloading the model"

      tarball_path = File.join(model_directory, tarball_filename)
      File.open(tarball_path, "w") do |f|
        obj.read do |chunk|
          f.write(chunk)
        end
      end

      # Unpack the model
      puts "Unpacking the model"
      `tar -xf #{tarball_path}`
      raise "Failed to extract" unless $?.to_i == 0
    end
  end

  def compile_moolloy(temporary_directory, commit)
    puts "Cloning repo."

    # If we have a seed repo then we will copy it into place and pull
    # instead of cloning. This saves us bandwidth since the alloy repo is quite
    # large. By using a seed repo we only need to pull the latest commits.
    seed_repo_path = @configuration.seed_repo_path
    if seed_repo_path
      # We use --reflink=auto to reduce disk usage, it performs a shallow copy
      # with copy-on-write if the operating system supports it. Otherwise, it
      # will perform a regular copy.
      puts "cp -r #{seed_repo_path} ./moolloy"
      `cp -r #{seed_repo_path} ./moolloy`
      raise "Failed to copy seed repo." unless $?.to_i == 0
    else
      # Clone the repo using the ssh key specified in the configuration.
      # We accomplish this by spawning a new ssh agent for the command and
      # loading the key into it.
      puts "ssh-agent bash -c 'ssh-add #{@configuration.ssh_key}; git clone #{@configuration.git_repo} moolloy'"
      `ssh-agent bash -c 'ssh-add #{@configuration.ssh_key}; git clone #{@configuration.git_repo} moolloy'`
      raise "Failed to clone git repo." unless $?.to_i == 0
    end

    Dir.chdir(File.join(temporary_directory, "moolloy")) do
      if seed_repo_path
        # If we copied a seed we need to pull it to get the latest commits.
        # Once again we use the key specified in the configuration.
        puts "ssh-agent bash -c 'ssh-add #{@configuration.ssh_key}; git pull"
        `ssh-agent bash -c 'ssh-add #{@configuration.ssh_key}; git pull'`
        raise "Failed to pull git repo." unless $?.to_i == 0
      end

      # Checkout the specific commit referenced by the job.
      puts "Checking out commit #{commit}"
      `git checkout #{commit}`
      raise "Commit checkout failed." unless $?.to_i == 0

      # Assert that we have successfully checked out the commit.
      actual_commit = `git rev-parse HEAD`.chomp
      unless $?.to_i == 0 && commit == actual_commit
        raise "Didn't checkout the correct commit." 
      end
  
      `git submodule init`
      raise "Submodule init failed." unless $?.to_i == 0

      commit_file = File.absolute_path(File.join(temporary_directory, "commit"))
      `git rev-parse HEAD > #{commit_file}`
      `git submodule foreach 'echo $path \`git rev-parse HEAD\` >> #{commit_file}'`

      # Update the submodules using the ssh key given by the configuration.
      puts "ssh-agent bash -c 'ssh-add #{@configuration.ssh_key}; git submodule update'"
      `ssh-agent bash -c 'ssh-add #{@configuration.ssh_key}; git submodule update'`
      raise "Submodule update failed." unless $?.to_i == 0

      # Build Moolloy
      puts "Building moolloy"
      `ant deps`
      raise "Failed to download dependencies." unless $?.to_i == 0
      `ant configure`
      raise "Failed to configure build." unless $?.to_i == 0
      `ant dist`
      raise "Failed to build." unless $?.to_i == 0
    end


    puts "Acquiring jar file"
    dist_path = File.join(temporary_directory,
                          "moolloy",
                          "dist",
                          "alloy-dev.jar")
    FileUtils.mv(dist_path, File.join(temporary_directory, "moolloy.jar"))
  end

  def run_moolloy(temporary_directory)
    model_directory = File.join(temporary_directory, "model")

    puts "Running moolloy."

    @run_start_time = Time.now
    benchmark_result = Benchmark.measure do
      pid = Process.spawn("#{@configuration.command} -jar #{File.join(temporary_directory, "moolloy.jar")} \"#{model_directory}/model.als\" > stdout.out 2> stderr.out", :pgroup => true)
      @worker_process_group = Process.getpgid(pid)
      Process.wait(pid)
      @worker_process_group = nil
    end
    @run_start_time = nil

    return_code = $?.to_i

    # Determine if the solutions match the model solutions
    correct = (return_code == 0)
    test_solution_files = Dir[File.join(temporary_directory, "alloy_solutions_*.xml")]
    model_solution_files = Dir[File.join(model_directory, "alloy_solutions_*.xml")]

    if test_solution_files.count != model_solution_files.count
      puts "Wrong number of solutions generated."
      correct = false
    end

    hash_to_model_solution = {}
    model_solution_files.each do |model_file|
      `tail -n +2 #{model_file} > #{model_file}.trimmed`
      raise "Failed to trim model solution #{model_file}." unless $?.to_i == 0
      hash = sha2_hash(model_file + ".trimmed")
      hash_to_model_solution[hash] ||= []
      hash_to_model_solution[hash] << model_file
    end

    test_solution_files.each do |test_file|
      `tail -n +2 #{test_file} > #{test_file}.trimmed`
      raise "Failed to trim test solution #{model_file}." unless $?.to_i == 0
      hash = sha2_hash(test_file + ".trimmed")
      if !hash_to_model_solution[hash].nil? &&
         !hash_to_model_solution[hash].empty?
        matching_file = hash_to_model_solution[hash].shift
        puts "#{test_file} matches #{matching_file}."
      else
        puts "#{test_file} has no match."
        correct = false
      end
    end

    if correct
      puts "The solutions are correct."
    else
      puts "The solutions are incorrect."
    end

    return {
      :correct => correct,
      :return_code => return_code,
      :benchmark_result => benchmark_result
    }
  end

  def upload_results(temporary_directory, message_id)
    package_directory = File.join(temporary_directory, "package")
    stdout_path = File.join(temporary_directory, "stdout.out")
    stderr_path = File.join(temporary_directory, "stderr.out")
    model_path = File.join(temporary_directory, "model")
    alloy_solutions_path = File.join(temporary_directory, "alloy_solutions_*.xml")
    commit_path = File.join(temporary_directory, "commit")

    FileUtils.mkdir(package_directory)
    FileUtils.mv(stdout_path, package_directory)
    FileUtils.mv(stderr_path, package_directory)
    FileUtils.mv(model_path, package_directory)
    FileUtils.mv(commit_path, package_directory)

    Dir[alloy_solutions_path].each do |fpath|
      FileUtils.mv(fpath, package_directory)
    end

    # Tarball the entire temporary directory
    tarball_path = File.join(temporary_directory, "tarball.tar.bz2")
    Dir.chdir(package_directory) do
      `hostname > ./hostname`
      `tar -cjf "#{tarball_path}" #{File.join(".", "*")}`
      raise "Failed to create package archive." unless $?.to_i == 0
    end

    # Upload the tarball to s3
    key = "results/" + message_id + ".tar.bz2"
    get_s3_bucket.objects[key].write(:file => tarball_path)

    return key
  end

  def post_start(test_id)
    post_url = "#{@configuration.server_base_url}/workers/#{@worker_id}/start"
    puts "Making post request to #{post_url}"

    body = {
      :test_id => test_id
    }

    HTTParty.post(post_url, {
      :body => body.to_json,
      :basic_auth => get_http_auth
    })
  end

  def post_results(test_id, message_id, started_at, results, s3_key)
    post_url = "#{@configuration.server_base_url}/workers/#{@worker_id}/result"
    puts "Making post request to #{post_url}"

    completion_body = {
      :test_id => test_id,
      :secret_key => message_id,
      :return_code => results[:return_code],
      :correct => results[:correct] ? 1 : 0,
      :started_at => started_at,
      :runtime_seconds => results[:benchmark_result].real,
      :cpu_time_seconds => results[:benchmark_result].total,
      :tarball_s3_key => s3_key
    }

    HTTParty.post(post_url, {
      :body => completion_body.to_json,
      :basic_auth => get_http_auth
    })
  end

  def get_http_auth
    auth = @configuration.read_multiple([:username, :password])
    if auth[:username] || auth[:password]
      return auth
    else
      return nil
    end
    return auth
  end

  def get_s3_bucket
    s3_client = AWS::S3.new
    s3_bucket = s3_client.buckets[@configuration.s3_bucket]
    return s3_bucket
  end

  def get_sqs_queue
    sqs_client = AWS::SQS.new
    sqs_queue = sqs_client.queues.named(@configuration.sqs_queue_name)
    return sqs_queue
  end
end
