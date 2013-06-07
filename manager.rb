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

private

  #TODO: Need to grab register_url and auth params atomically
  def register
    puts "Registering with #{register_url}"
    response = HTTParty.post(register_url, {
      :body => { :hostname => hostname }.to_json,
      :basic_auth => http_auth_params
    })

    parsed_response = JSON.parse(response.parsed_response)
    @worker_id = parsed_response["worker_id"].to_i
  end

  #TODO: Need to grab unregister_rul and auth params atomically
  def unregister
    HTTParty.post(unregister_url, {
      :basic_auth => http_auth_params
    })
  end

  #TODO: Need to grab heartbeat_url and auth params atomically
  def heartbeat(current_test_result_id)
    begin
      HTTParty.post(heartbeat_url, {
        :body => { :test_id => current_test_result_id }.to_json,
        :basic_auth => http_auth_params
      })
    rescue Exception => e
      puts "Manager failed to heartbeat:"
      puts e.inspect
    end
  end

  def hostname
    Socket.gethostname
  end

  def http_auth_params
    auth_params = @configuration.read_multiple([:username, :password])
    if auth_params[:username] || auth_params[:password]
      return auth_params
    else
      return nil
    end
  end

  def register_url
    "#{@configuration.server_base_url}/workers/register"
  end

  def unregister_url
    "#{@configuration.server_base_url}/workers/#{@worker_id}/unregister"
  end

  def heartbeat_url
    "#{@configuration.server_base_url}/workers/#{@worker_id}/heartbeat"
  end

end
