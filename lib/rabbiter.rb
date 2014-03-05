# Copyright (C) 2010-2014  Kouhei Sutou <kou@cozmixng.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

require "shellwords"
require "pathname"
require "yaml"

require "gio2"
require "twitter/json_stream"
require "twitter_oauth"

require "rabbit/utils"

require "rabbiter/version"
require "rabbiter/gettext"

module Rabbiter
  class Client
    CONSUMER_KEY = "wT9WSC0afRw94fxUw0iIKw"
    CONSUMER_SECRET = "mwY35vfQfmWde9lZbyNNB15QzCq3k2VwGj3X1IAkQ8"

    include GetText

    def initialize(logger)
      @logger = logger
      @oauth_parameters = nil
      @config_file_path = Pathname.new("~/.rabbit/twitter-oauth.yaml")
      @config_file_path = @config_file_path.expand_path
      @listeners = []
      @connection = nil
    end

    def register_listener(&block)
      @listeners << block
    end

    def setup
      return unless @oauth_parameters.nil?
      setup_access_token unless @config_file_path.exist?
      oauth_access_parameters = YAML.load(@config_file_path.read)
      @oauth_parameters = {
        :consumer_key => CONSUMER_KEY,
        :consumer_secret => CONSUMER_SECRET,
        :access_key => oauth_access_parameters[:access_token],
        :access_secret => oauth_access_parameters[:access_secret],
      }
    end

    def close
      return if @connection.nil?
      @connection.close
      @connection = nil
    end

    def start(*filters, &block)
      register_listener(&block) if block_given?
      setup if @oauth_parameters.nil?
      return if @oauth_parameters.nil?

      stream_options = {
        :oauth => @oauth_parameters,
        :user_agent => "Rabbiter #{Rabbiter::VERSION}",
        :method => "POST",
        :filters => filters,
      }
      @stream = ::Twitter::JSONStream.allocate
      @stream.send(:initialize, stream_options)
      @stream.send(:reset_state)
      @connection = GLibConnection.new(@logger, @stream)

      @stream.each_item do |item|
        status = JSON.parse(item)
        @listeners.each do |listener|
          listener.call(status)
        end
      end

      @stream.on_error do |message|
        @logger.error("[twitter] #{message}")
      end

      @stream.on_reconnect do |timeout, n_retries|
        format = _("%{timeout} seconds (%{n_retries})")
        message = format % {:timeout => timeout, :n_retries => retries}
        @logger.info("[twitter][reconnect] #{message}")
      end

      @stream.on_max_reconnects do |timeout, n_retries|
        format = _("Failed after %{n_retries} failed reconnects")
        message = format % {:n_retries => retries}
        @logger.info("[twitter][max-reconnects] #{message}")
      end

      @connection.connect
    end

    private
    def setup_access_token
      FileUtils.mkdir_p(@config_file_path.dirname)
      oauth_options = {
        :consumer_key => CONSUMER_KEY,
        :consumer_secret => CONSUMER_SECRET,
        :proxy => ENV["http_proxy"],
      }
      client = TwitterOAuth::Client.new(oauth_options)
      request_token = client.request_token
      authorize_url = request_token.authorize_url
      puts( _("1) Access this URL: %{url}") % {:url => authorize_url})
      show_uri(authorize_url)
      print(_("2) Enter the PIN: "))
      pin = $stdin.gets.strip
      access_token = request_token.get_access_token(:oauth_verifier => pin)
      oauth_parameters = {
        :access_token => access_token.token,
        :access_secret => access_token.secret,
      }
      @config_file_path.open("w") do |config_file|
        config_file.chmod(0600)
        config_file.puts(YAML.dump(oauth_parameters))
      end
    end

    def show_uri(uri)
      return unless Gtk.respond_to?(:show_uri)
      begin
        Gtk.show_uri(uri)
      rescue GLib::Error
        @logger.warning("[twitter][show-uri] #{$!}")
      end
    end

    class GLibConnection
      def initialize(logger, handler)
        @logger = logger
        @handler = handler
        @options = @handler.instance_variable_get("@options")
        @client = nil
        @connection = nil
        @socket = nil
        @source_ids = []
      end

      def connect
        close
        @client = Gio::SocketClient.new
        @client.tls = @options[:ssl]
        @client.tls_validation_flags = :validate_all
        if Rabbit::Utils.windows?
          @client.tls_validation_flags -= :unknown_ca
        end
        @connection = @client.connect_to_host(@options[:host], @options[:port])
        @input = @connection.input_stream
        @output = @connection.output_stream

        reader_source = @input.create_source do |stream|
          @logger.debug("[twitter][read][start]")
          begin
            data = @input.read_nonblocking(8192)
          rescue Gio::IOError::WouldBlock
              data = ""
          rescue => error
            @logger.debug("[twitter][read][error] #{error}")
            @handler.send(:receive_error, "#{error.class}: #{error}")
            data = nil
          end
          if data
            @logger.debug("[twitter][read][done] #{data.bytesize}")
            @handler.receive_data(data) unless data.empty?
            true
          else
            false
          end
        end
        @source_ids << reader_source.attach

        @handler.extend(GLibAdapter)
        @handler.connection = self
        @handler.connection_completed
      end

      def send_data(data)
        rest = data.bytesize
        writer_source = @output.create_source do |stream|
          if rest.zero?
            @logger.debug("[twitter][flush][start]")
            @output.flush
            @logger.debug("[twitter][flush][done]")
            @source_ids.reject! {|id| id == writer_source.id}
            false
          else
            @logger.debug("[twitter][write][start]")
            written_size = @output.write_nonblocking(data)
            @logger.debug("[twitter][write][done] #{written_size}")
            rest -= written_size
            data[0, written_size] = ""
            true
          end
        end
        @source_ids << writer_source.attach
      end

      def close
        return if @client.nil?
        @source_ids.reject! do |id|
          GLib::Source.remove(id)
          true
        end
        @input = nil
        @output = nil
        @connection = nil
        @client = nil
      end

      def reconnect(options={})
        close
        after = options[:after] || 0
        if after.zero?
          connect
        else
          id = GLib::Timeout.add(after) do
            connect
            false
          end
          @source_ids << id
        end
      end
    end

    module GLibAdapter
      attr_accessor :connection
      def start_tls
      end

      def send_data(data)
        @connection.send_data(data)
      end

      def reconnect_after(timeout)
        @reconnect_callback.call(timeout, @reconnect_retries) if @reconnect_callback
        @connection.reconnect(:after => timeout)
      end

      def reconnect(server, port)
        @connection.reconnect
      end

      def close_connection
        @connection.close
      end
    end
  end
end
