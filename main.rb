#!/usr/bin/env ruby

require 'aws-sdk'
require 'safe_yaml'
require 'digest'
require 'tmpdir'
require 'benchmark'
require 'json'
require 'httparty'

# Configuration depends on deserializing symbols.
# Worst case is that we run out of memory.
SafeYAML::OPTIONS[:deserialize_symbols] = true

require_relative 'common.rb'
require_relative 'configuration.rb'
require_relative 'runner.rb'
require_relative 'manager.rb'

config_file_path = ARGV[0]
raise "Configuration file path must be specified." if config_file_path.nil?
configuration = Configuration.new(config_file_path)

puts "Timeout is: #{configuration.worker_timeout}"

manager = Manager.new(configuration)
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

Signal.trap("HUP") do
  puts "Requesting configuration update"
  manager.update_configuration
end

manager.join
