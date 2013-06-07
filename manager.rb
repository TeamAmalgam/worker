require 'socket'

class Manager

  def initialize(configuration)
    @server_base_url = configuration.server_base_url

    @http_auth = nil
   
    auth_settings = configuration.read_multiple([:username, :password])

    if auth_settings[:username] || auth_settings[:password] 
      @http_auth = {
        :username => auth_settings[:username],
        :password => auth_settings[:password]
      }
    end

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

  def register
    puts "Registering with #{register_url}"
    response = HTTParty.post(register_url, {
      :body => { :hostname => hostname }.to_json,
      :basic_auth => @http_auth
    })

    parsed_response = JSON.parse(response.parsed_response)
    @worker_id = parsed_response["worker_id"].to_i
  end

  def unregister
    HTTParty.post(unregister_url, {
      :basic_auth => @http_auth
    })
  end

  def heartbeat(current_test_result_id)
    begin
      HTTParty.post(heartbeat_url, {
        :body => { :test_id => current_test_result_id }.to_json,
        :basic_auth => @http_auth
      })
    rescue Exception => e
      puts "Manager failed to heartbeat:"
      puts e.inspect
    end
  end

  def hostname
    Socket.gethostname
  end

  def register_url
    "#{@server_base_url}/workers/register"
  end

  def unregister_url
    "#{@server_base_url}/workers/#{@worker_id}/unregister"
  end

  def heartbeat_url
    "#{@server_base_url}/workers/#{@worker_id}/heartbeat"
  end

end
