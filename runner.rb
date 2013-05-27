class Runner
 
  def initialize (command, s3_bucket, sqs_queue, server_base_url, http_auth, temp_root, git_repo, ssh_key)
    @command = command
    @s3_bucket = s3_bucket
    @sqs_queue = sqs_queue
    @server_base_url = server_base_url
    @http_auth = http_auth
    @termination_requested = false
    @current_test_result_id = nil
    @temp_root = temp_root
    @repo_url = git_repo
    @ssh_key = ssh_key
  end

  def run(worker_id)
    @worker_id = worker_id
    @thread = Thread.new do
      catch(:termination) do
        while !@termination_requested
          @sqs_queue.poll(:idle_timeout => 2 * 60) do |message|
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
    @thread.exit
  end

  def join
    @thread.join
  end

  def current_test_result_id
    return @current_test_result_id
  end

private
  
  def process_message(message)
    puts "Received Job:"
    job_description = YAML.load(message.body)
    puts job_description.inspect

    @current_test_result_id = job_description[:test_id]

    started_at = Time.now

    post_start(job_description[:test_id])

    results = nil
    tarball_s3_key = nil

    Dir.mktmpdir(nil, @temp_root) do |temp_dir|
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
    obj = @s3_bucket.objects[s3_key] 
    model_directory = File.join(temporary_directory, "model")

    Dir.mkdir(model_directory) 
    Dir.chdir(model_directory) do
      # Download the model
      puts "Downloading the model"

      tarball_path = File.join(model_directory, "model.tar.gz")
      File.open(tarball_path, "w") do |f|
        obj.read do |chunk|
          f.write(chunk)
        end
      end

      # Unpack the model
      puts "Unpacking the model"
      `tar -xzf #{tarball_path}`
      raise "Failed to extract" unless $?.to_i == 0
    end
  end

  def compile_moolloy(temporary_directory, commit)
    puts "Cloning repo."
    puts "ssh-agent bash -c 'ssh-add #{@ssh_key}; git clone #{@repo_url} moolloy'"
    `ssh-agent bash -c 'ssh-add #{@ssh_key}; git clone #{@repo_url} moolloy'`

    Dir.chdir(File.join(temporary_directory, "moolloy")) do
      puts "Checking out commit #{commit}"
      `git checkout #{commit}`
      `git submodule init`

      puts "ssh-agent bash -c 'ssh-add #{@ssh_key}; git submodule update'"
      `ssh-agent bash -c 'ssh-add #{@ssh_key}; git submodule update'`

      puts "Building moolloy"
      `ant deps`
      `ant configure`
      `ant dist`
    end


    puts "Acquiring jar file"
    dist_path = File.join(temporary_directory,
                          "moolloy",
                          "dist",
                          "alloy-dev.jar")
    `mv #{dist_path} #{File.join(temporary_directory, "moolloy.jar")}`
  end

  def run_moolloy(temporary_directory)
    model_directory = File.join(temporary_directory, "model")

    puts "Running moolloy."
    benchmark_result = Benchmark.measure do
      `#{@command} -jar #{File.join(temporary_directory, "moolloy.jar")} "#{model_directory}/model.als" > stdout.out 2> stderr.out`
    end

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
      hash = sha2_hash(model_file)
      hash_to_model_solution[hash] ||= []
      hash_to_model_solution[hash] << model_file
    end

    test_solution_files.each do |test_file|
      hash = sha2_hash(test_file)
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
    # Tarball the entire temporary directory
    tarball_path = File.join(temporary_directory, "tarball.tar.gz")
    `tar -czf "#{tarball_path}" ./*`

    # Upload the tarball to s3
    key = "results/" + message_id + ".tar.gz"
    @s3_bucket.objects[key].write(:file => tarball_path)

    return key
  end

  def post_start(test_id)
    post_url = "#{@server_base_url}/workers/#{@worker_id}/start"
    puts "Making post request to #{post_url}"

    body = {
      :test_id => test_id
    }

    HTTParty.post(post_url, {
      :body => body.to_json,
      :basic_auth => @http_auth
    })
  end

  def post_results(test_id, message_id, started_at, results, s3_key)
    post_url = "#{@server_base_url}/workers/#{@worker_id}/result"
    puts "Making post request to #{post_url}"

    completion_body = {
      :test_id => test_id,
      :secret_key => message_id,
      :return_code => results[:return_code],
      :correct => results[:correct] ? 1 : 0,
      :started_at => started_at,
      :runtime_seconds => results[:benchmark_result].real,
      :tarball_s3_key => s3_key
    }

    HTTParty.post(post_url, {
      :body => completion_body.to_json,
      :basic_auth => @http_auth
    })
  end
end
