class Runner
 
  def initialize (command, s3_bucket, sqs_queue, server_base_url, http_auth, temp_root)
    @command = command
    @s3_bucket = s3_bucket
    @sqs_queue = sqs_queue
    @server_base_url = server_base_url
    @http_auth = http_auth
    @termination_requested = false
    @current_test_result_id = nil
    @temp_root
  end

  def run
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

    Dir.mktmpdir(nil, @temp_root) do |temp_dir|
      puts "Using temporary directory #{temp_dir}"
      Dir.chdir(temp_dir) do
        obj = @s3_bucket.objects[job_description[:model_s3_key]]

        Dir.mkdir(File.join(temp_dir, "model"))
        Dir.chdir(File.join(temp_dir, "model")) do
          puts "Downloading the model"
          # Download the model
          File.open(File.join(temp_dir, "model", "model.tar.gz"), "w") do |f|
            obj.read do |chunk|
              f.write(chunk)
            end
          end
          
          puts "Unpacking the model"

          # Unpack the model
          `tar -xzf model.tar.gz`
          raise "Failed to extract" unless $?.to_i == 0
        end
        
        # Delete the message since we are about to do a lot of work and we
        # don't want another worker to pick it up.
        message.delete

        # Run moolloy on the model
        puts "Running moolloy."
        benchmark_result = Benchmark.measure do
          `#{@command} "model/model.als" > stdout.out 2> stderr.out`
        end

        return_value = $?.to_i

        # Determine if the solutions match the model solutions
        correct = true
        test_solution_files = Dir["./alloy_solutions_*.xml"]
        model_solution_files = Dir["./model/alloy_solutions_*.xml"]

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

        # Tarball the entire directory
        `tar -czf "tarball.tar.gz" ./*`
        
        # Upload the tarball to s3
        key = "results/" + message.id + ".tar.gz"
        @s3_bucket.objects[key].write(:file => "tarball.tar.gz")

        puts "Uploaded tarball to bucket with key #{key}."

        post_url = "#{@server_base_url}/result"
        puts "Making post request to #{post_url}"
        
        completion_body = {
          :test_id => job_description[:test_id],
          :secret_key => message.id,
          :return_code => return_value,
          :correct => correct ? 1 : 0,
          :started_at => started_at,
          :runtime_seconds => benchmark_result.real,
          :tarball_s3_key => key
        }

        HTTParty.post(post_url, {
          :body => completion_body.to_json,
          :basic_auth => @http_auth
        })
      end
    end
    puts "Finished processing job: #{message.id}."
    @current_test_result_id = nil
  end
end
