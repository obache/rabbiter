require 'net/http'
require 'uri'
require 'json'
require 'oauth'

module Rabbiter

  class TwitterStream
  
    FILTER_ENDPOINT_URI = "https://stream.twitter.com/1.1/statuses/filter.json"

    def initialize(logger, oauth, params=nil)
      @logger = logger
      @consumer = oauth[:consumer]
      @access = oauth[:access]
      @params = {
        :connection_timeout => 90,
        :http_error_initial_wait => 5,
        :http_error_max_wait => 320,
        :http_420_error_initial_wait => 60,
        :http_420_error_max_wait => nil,
        :tls_verify => true,
      }
      @params.merge!(params) unless params.nil?
      @http = nil
      self
    end
    
    def filter(params=nil)
      raise ArgumentError, "params is not hash" unless params.nil? || params.kind_of?(Hash)
      start(params) do |status|
        yield status
      end
    end
    
    def track(track, params=nil)
      raise ArgumentError, "track is neither array nor string" unless track.kind_of?(Array) || track.kind_of?(String)
      raise ArgumentError, "params is not hash" unless params.nil? || params.kind_of?(Hash)
      
      p = { 'track' => track.kind_of?(Array) ? track.join(",") : track }
      
      p.merge!(params) if params
      start(p) do |status|
        yield status
      end
    end
    
    def close
      @http.finish unless @http.nil? || !@http.started?
    end
    
    private
  
    def start(filters=nil)
      raise ArgumentError, "filters is not hash" unless filters.nil? || filters.kind_of?(Hash)
      
      uri = URI.parse(FILTER_ENDPOINT_URI)
      request_headers = {
        'Accept-Encoding' => 'identity',
      }
      request_headers["User-Agent"] = @params[:user_agent] if @params[:user_agent]
      
      filters["stall_warnings"] = "true" if @logger.warning?
      
      @http = Net::HTTP.new(uri.host, uri.port, :ENV)
      @http.read_timeout = @params[:connection_timeout]
      @http.use_ssl = true if uri.scheme == "https"
      @http.verify_mode = OpenSSL::SSL::VERIFY_NONE unless @params[:tls_verify]
      
      request = Net::HTTP::Post.new(uri.request_uri, request_headers)
      request.set_form_data(filters) if filters
      request.oauth!(@http, @consumer, @access)
      
      time_wait = nil
      
      begin
        @http.start do
          begin
            @http.request(request) do |response|
              response.value
              time_wait = nil
              buffer = ''
              response.read_body do |chunk|
                buffer += chunk
                next unless chunk.match(/\r\n$/)
                begin
                  message = JSON.parse(buffer)
                rescue
                  next
                ensure
                  buffer = ''
                end
                if @logger.warning?
                  warning = message["warning"]
                  @logger.warning("[twitter] #{warning}") unless warning.nil?
                end
                if @logger.error?
                  disconnect = message["disconnect"]
                  @logger.error("[twitter] #{disconnect}") unless disconnect.nil?
                end
                yield message
              end
            end
          rescue Net::HTTPServerException => error
            @logger.warning("[twitter] #{error.message}")
            if error.response.code == "420" then
              if time_wait.nil? then
                time_wait = @params[:http_420_error_initial_wait]
              else
                time_wait = time_wait * 2
              end
              retry
            end
            return
          rescue Net::HTTPFatalError => error
            @logger.warning("[twitter] #{error}")
            if time_wait.nil? then
              time_wait = @params[:http_error_initial_wait]
            else
              time_wait = time_wait * 2
              if time_wait > @params[:http_error_max_wait] then
                @logger.warning("[twitter] max reconnecting reached.")
                return
              end
            end
            retry
          ensure
            unless time_wait.nil? then
              @logger.warning("[twitter] waiting retry #{time_wait} sec.")
              sleep(time_wait)
            end
          end
        end
      rescue Net::OpenTimeout 
        @logger.warning("[twitter] Open timeout, retrying.")
        retry
      rescue Net::ReadTimeout 
        @logger.warning("[twitter] Read timeout, retrying.")
        retry
      rescue EOFError 
        @logger.warning("[twitter] EOF, retrying.")
        retry
      rescue Errno::ECONNRESET
        @logger.warning("[twitter] Connection reset, retrying.")
        retry
      ensure
        close
        unless reconnection_wait.nil? then
          @logger.warning("[twitter] waiting reconnection #{time_wait} sec.")
          sleep(reconnection_wait)
        end
      end
    end
  end
end
