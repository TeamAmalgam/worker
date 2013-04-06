#!/usr/bin/env ruby

require 'aws-sdk'
require 'optparse'
require 'yaml'
require 'digest'
require 'tmpdir'
require 'benchmark'
require 'json'
require 'httparty'

def sha2_hash(filename)
  digest = Digest::SHA2.new
  
  File.open(filename) do |file|
    while not file.eof
      digest << file.read(digest.block_length)
    end
  end

  digest.to_s
end

options = {}
OptionParser.new do |opts|

  opts.on("-a", "--access-key-id ACCESS_KEY",
          "The AWS Access Key id to use") do |access_key|
    options[:access_key_id] = access_key
  end

  opts.on("-s", "--secret-access-key SECRET_KEY",
          "The AWS Secret Access Key to use") do |secret_key|
    options[:secret_access_key] = secret_key
  end

  opts.on("-b", "--bucket BUCKET_NAME",
          "The S3 bucket to download models from and upload results to.") do |bucket|
    options[:s3_bucket] = bucket
  end
  
  opts.on("-q", "--queue QUEUE_NAME",
          "The SQS queue to listen to.") do |queue_name|
    options[:sqs_queue_name] = queue_name
  end

  opts.on("-e", "--executable EXECUTABLE_PATH",
          "The command to execute on each model.") do |command|
    options[:command] = command
  end

  opts.on("-d", "--working-directory DIRECTORY",
          "The directory to work in.") do |directory|
    options[:tmp_dir] = directory
 
  end
  
  opts.on("-P", "--post-url POST_URL",
          "The url to post to once the job is complete") do |url|
    options[:post_url] = url
  end

  opts.on("-u", "--username USERNAME",
          "The username to use for HTTP Auth on the post request") do |username|
    options[:username] = username
  end

  opts.on("-p", "--password PASSWORD",
          "The password to use for HTTP Auth on the post request") do |password|
    options[:password] = password
  end
  
  opts.on("-c", "--config-file CONFIG_FILE",
          "The config-file to use.") do |config_file|
    file_options = YAML.load(File.read(config_file))
    options = options.merge(file_options)
  end
end.parse!

raise "Must provide AWS credentials." if options[:access_key_id].nil? || options[:secret_access_key].nil?
AWS.config(:access_key_id => options[:access_key_id],
           :secret_access_key => options[:secret_access_key])

s3 = AWS::S3.new

raise "Must specify a bucket" if options[:s3_bucket].nil?

bucket = s3.buckets[options[:s3_bucket]]

sqs = AWS::SQS.new

queue = sqs.queues.named(options[:sqs_queue_name])

raise "Could not find queue with name: #{options[:sqs_queue_name]}." if queue.nil?

queue.poll do |message|
  puts "Received Job:"
  job_description = YAML.load(message.body)
  puts job_description.inspect

  started_at = Time.now

  Dir.mktmpdir(nil, options[:tmp_dir]) do |temp_dir|
    puts "Using temporary directory #{temp_dir}"
    Dir.chdir(temp_dir) do
      obj = bucket.objects[job_description[:model_s3_key]]

      Dir.mkdir(File.join(temp_dir, "model"))
      Dir.chdir(File.join(temp_dir, "model")) do
        # Download the model
        File.open(File.join(temp_dir, "model", "model.tar.gz"), "w") do |f|
          obj.read do |chunk|
            f.write(chunk)
          end  
        end
       
        # Unpack the model 
        `tar -xzf model.tar.gz`
        raise "Failed to extract" unless $?.to_i == 0
      end

      # Delete the message since we are about to do a lot of work and we
      # don't want another server to pick it up.
      message.delete

      # Run moolloy on the model
      puts "Running moolloy."
      benchmark_result = Benchmark.measure do
        `#{options[:command]} "model/model.als" > stdout.out 2> stderr.out`
      end
     
      return_value = $?.to_i

      # Determine if the solutions match the model solutions.
      correct = true
      test_solution_files = Dir["./alloy_solutions_*.xml"]
      model_solution_files = Dir["./model/alloy_solutions_*.xml"]

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
      bucket.objects[key].write(:file => "tarball.tar.gz")

      puts "Uploaded tarball to bucket with key #{key}."
      
      if !options[:post_url].nil?
        puts "Making post request to #{options[:post_url]}"
        completion_body = {
          :test_id => job_description[:test_id],
          :secret_key => message.id,
          :return_code => return_value,
          :correct => correct ? 1 : 0,
          :started_at => started_at,
          :runtime_seconds => benchmark_result.real,
          :tarball_s3_key => key
        }

        auth_info = nil
        if options[:username] || options[:password]
          auth_info = {
            :username => options[:username],
            :password => options[:password]
          }
        end

        HTTParty.post(options[:post_url], {
          :body => completion_body.to_json,
          :basic_auth => auth_info
        })
      end
    end
  end
  puts "Finished processing job: #{message.id}."
end
