#!/usr/bin/env ruby

require 'aws-sdk'
require 'optparse'
require 'yaml'
require 'digest'
require 'tmpdir'
require 'benchmark'
require 'json'
require 'httparty'

require_relative 'common.rb'
require_relative 'runner.rb'
require_relative 'manager.rb'

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

manager = Manager.new(options[:run_command] || options[:command],
                      options[:convert_command],
                      options[:compile_command],
                      options[:classpath],
                      options[:s3_bucket],
                      options[:sqs_queue_name],
                      options[:server_base_url],
                      options[:username],
                      options[:password],
                      options[:tmp_dir])

manager.run

int_count = 0
Signal.trap("USR1") do 
  int_count += 1
  if int_count == 1
    puts "Requesting Terminate"
    manager.request_termination
  elsif int_count == 2
    puts "Forcing Terminate"
    manager.terminate 
  else
    Kernel.exit!
  end
end

manager.join
