require 'socket'

class Manager

  def initialize(configuration)
    @configuration = configuration

    @worker = Runner.new(configuration)
  end

  def run
    puts "Running with PID: #{Process.pid}"
    @thread = Thread.new {
      register
      
      @worker.run(@worker_id)
      catch(:terminate) do 
        while true
          16.times do 
            sleep(15)
            if @termination_requested
              @worker.request_termination
              @termination_requested = false
            end
            
            if @termination_required
              @worker.terminate
              @termination_required = false
            end

            if @configuration_update_requested
              @configuration.update
              @configuration_update_requested = false
              puts "Configuration updated"
            end

            if !@worker.run_start_time.nil? &&
               ((Time.now - @worker.run_start_time) >= @configuration.worker_timeout)
              terminate_job
            end

            if @worker.terminated?
              throw :terminate
            end
          end
          
          if @termination_requested
            @worker.request_termination
            @termination_requested = false
          end

          heartbeat(@worker.current_test_result_id)
        end
      end
      @worker.join
      unregister
    }
  end

  def join
    @thread.join
  end

  def request_termination
    @termination_requested = true
  end

  def terminate
    @termination_required = true
  end

  def update_configuration
    @configuration_update_requested = true
  end

  def terminate_job
    puts "Terminating the current job"
    @worker.terminate_job
  end

private

  def register
    settings = @configuration.read_multiple([:server_base_url, :username, :password])
    puts "Registering with #{register_url(settings[:server_base_url])}"
    response = HTTParty.post(register_url(settings[:server_base_url]), {
      :body => { :hostname => hostname }.to_json,
      :basic_auth => http_auth_params(settings)
    })

    parsed_response = JSON.parse(response.parsed_response)
    @worker_id = parsed_response["worker_id"].to_i
  end

  def unregister
    settings = @configuration.read_multiple([:server_base_url, :username, :password])
    HTTParty.post(unregister_url(settings[:server_base_url]), {
      :basic_auth => http_auth_params(settings)
    })
  end

  def heartbeat(current_test_result_id)
    settings = @configuration.read_multiple([:server_base_url, :username, :password])
    begin
      HTTParty.post(heartbeat_url(settings[:server_base_url]), {
        :body => { :test_id => current_test_result_id }.to_json,
        :basic_auth => http_auth_params(settings)
      })
    rescue Exception => e
      puts "Manager failed to heartbeat:"
      puts e.inspect
    end
  end

  def hostname
    Socket.gethostname
  end

  def http_auth_params(params_hash)
    if params_hash[:username] || params_hash[:password]
      return {
        :username => params_hash[:username],
        :password => params_hash[:password]
      }
    else
      return nil
    end
  end

  def register_url(base_url)
    "#{base_url}/workers/register"
  end

  def unregister_url(base_url)
    "#{base_url}/workers/#{@worker_id}/unregister"
  end

  def heartbeat_url(base_url)
    "#{base_url}/workers/#{@worker_id}/heartbeat"
  end

end
