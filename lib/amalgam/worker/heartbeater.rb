require 'socket'

class Amalgam::Worker::Heartbeater
  
  attr_reader :worker_id

  def initialize(server_base_url, username, password, old_heartbeater = nil)
    @server_base_url = server_base_url

    @worker_id = nil
    unless old_heartbeater.nil?
      @worker_id = old_heartbeater.worker_id
    end

    @auth_params = nil
    unless username.nil? && password.nil?
      @auth_params = {
        :username => username,
        :password => password
      }
    end
  end


  def register
    raise "Already registered." unless @worker_id.nil?

    Amalgam::Worker.logger.info("Registering with server.")

    response = HTTParty.post(register_url, {
      :body => { :hostname => hostname }.to_json,
      :basic_auth => @auth_params
    })

    parsed_response = JSON.parse(response.parsed_response)

    # Validate the response.

    if parsed_response["worker_id"].nil?
      raise "Response did not contain worker_id."
    end

    @worker_id = parsed_response["worker_id"].to_i
  end

  def heartbeat(current_job_id = nil)
    raise "Not registered." if @worker_id.nil?

    begin
      HTTTParty.post(heartbeat_url, {
          :body => { :job_id => current_job_id }.to_json,
          :basic_auth => @auth_params
        })
    rescue => err
      Amalgam::Worker.logger.error("Manager Failed to Heartbeat")
      Amalgam::Worker.logger.error(err.inspect)
    end
  end

  def signal_start(job_id)
    raise "Not registered." if @worker_id.nil?
    Amalgam::Worker.logger.info("Signalling start to server.")
    HTTParty.post(start_url(job_id), {
      :basic_auth => @auth_params
    })
  end

  def signal_completion(job_id, result_body)
    raise "Not registered." if @worker_id.nil?
    Amalgam::Worker.logger.info("Signalling completion to server.")
    HTTParty.post(completion_url(job_id), {
      :body => result_body.to_json,
      :basic_auth => @auth_params
    })
  end

  def unregister
    raise "Not registered." if @worker_id.nil?
    Amalgam::Worker.logger.info("Unregistering with server.")

    HTTParty.post(unregister_url, {
      :basic_auth => @auth_params
    })
  end

  private

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

  def start_url(job_id)
    "#{@server_base_url}/jobs/#{job_id}/start"
  end

  def completion_url(job_id)
    "#{@server_base_url}/jobs/#{job_id}/complete"
  end

end
