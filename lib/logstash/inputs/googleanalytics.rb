# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::GoogleAnalytics < LogStash::Inputs::Base
  config_name "googleanalytics"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # Most of these inputs are described in the Google Analytics API docs.
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#q_details
  # Any changes from the format described above have been noted.

  # A comma separated list of view (profile) ids, in the format 'ga:XXXX'
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#ids
  config :ids, :validate => :string, :required => true
  # In the format YYYY-MM-DD, or relative by using today, yesterday, or the NdaysAgo pattern
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#startDate
  config :start_date, :validate => :string, :default => 'yesterday'
  # In the format YYYY-MM-DD, or relative by using today, yesterday, or the NdaysAgo pattern
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#endDate
  config :end_date, :validate => :string, :default => 'yesterday'
  # The aggregated statistics for user activity to your site, such as clicks or pageviews.
  # Maximum of 10 metrics for any query
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#metrics
  # For a full list of metrics, see the documentation
  # https://developers.google.com/analytics/devguides/reporting/core/dimsmets
  config :metrics, :validate => :string, :required => true
  # Breaks down metrics by common criteria; for example, by ga:browser or ga:city
  # Maximum of 7 dimensions in any query
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#dimensions
  # For a full list of dimensions, see the documentation
  # https://developers.google.com/analytics/devguides/reporting/core/dimsmets
  config :dimensions, :validate => :string, :default => nil
  # Used to restrict the data returned from your request
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#filters
  config :filters, :validate => :string, :default => nil
  # A list of metrics and dimensions indicating the sorting order and sorting direction for the returned data
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#sort
  config :sort, :validate => :string, :default => nil
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#segment
  config :segment, :validate => :string, :default => nil
  # Valid values are DEFAULT, FASTER, HIGHER_PRECISION
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#samplingLevel
  config :sampling_level, :validate => :string, :default => nil
  # This is the result to start with, beginning at 1
  # You probably don't need to change this but it has been included here for completeness
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#startIndex
  config :start_index, :validate => :number, :default => 1
  # This is the number of results in a page. This plugin will start at
  # @start_index and keep pulling pages of data until it has all results.
  # You probably don't need to change this but it has been included here for completeness
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#maxResults
  config :max_results, :validate => :number, :default => 10000
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#include-empty-rows
  config :include_empty_rows, :validate => :boolean, :default => true

  # These values need to be pulled from your Google Developers Console
  # For more information, see the docs. Be sure to enable Google Analytics API
  # access for your application.
  # https://developers.google.com/identity/protocols/OAuth2ServiceAccount

  # This should be the path to the public/private key as a standard P12 file
  config :key_file_path, :validate => :string, :required => true
  # The key secret doe the file above. If not prompted for a secret,
  # it seems to default to notasecret
  config :key_secret, :validate => :string, :default => 'notasecret'
  # The service email account found in the Google Developers Console after
  # generating the key file.
  config :service_account_email, :validate => :string, :required => true

  # The service name to connect to. Should not change unless Google changes something
  config :service_name, :validate => :string, :default => 'analytics'
  # The version of the API to use.
  config :api_version, :validate => :string, :default => 'v3'

  # This will store the query in the resulting logstash event
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#data_response
  config :store_query, :validate => :boolean, :default => true
  # This will store the profile information in the resulting logstash event
  # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#data_response
  config :store_profile, :validate => :boolean, :default => true

  # Interval to run the command. Value is in seconds. If no interval is given,
  # this plugin only fetches data once.
  config :interval, :validate => :number, :required => false

  public
  def register
    require 'google/api_client'
  end # def register

  def run(queue)
    # we can abort the loop if stop? becomes true
    while !stop?
      start = Time.now
      client, analytics = get_service
      results_index = @start_index
      while !stop?
        results = client.execute(
          :api_method => analytics.data.ga.get,
          :parameters => client_options(results_index))

        if results.data.rows.first
          query = results.data.query.to_hash
          profile_info = results.data.profile_info.to_hash
          column_headers = results.data.column_headers.map { |c|
            c.name
          }

          results.data.rows.each do |r|
            event = LogStash::Event.new()
            decorate(event)
            event['containsSampledData'] = results.data.containsSampledData
            event['query'] = query if @store_query
            event['profileInfo'] = profile_info if @store_profile
            column_headers.zip(r).each do |head,data|
              if is_num(data)
                float_data = Float(data)
                # Sometimes GA returns infinity. if so, the number it invalid
                # so set it to zero.
                if float_data == Float::INFINITY
                  event[head.gsub(':','_')] = 0.0
                else
                  event[head.gsub(':','_')] = float_data
                end
              else
                event[head.gsub(':','_')] = data
              end
            end
            # Try to add a date unless it was already added
            if @start_date == @end_date
              if !event.include?('ga_date')
                if @start_date == 'today'
                  event['ga_date'] = Date.parse(Time.now().strftime("%F"))
                elsif @start_date == 'yesterday'
                  event['ga_date'] = Date.parse(Time.at(Time.now.to_i - 86400).strftime("%F"))
                elsif @start_date.include?('daysAgo')
                  days_ago = @start_date.sub('daysAgo','').to_i
                  event['ga_date'] = Date.parse(Time.at(Time.now.to_i - (days_ago*86400)).strftime("%F"))
                else
                  event['ga_date'] = Date.parse(@start_date)
                end
              else
                event['ga_date'] = Date.parse(event['ga_date'].to_s)
              end
            end
            queue << event
          end
        end
        nextLink = results.data.nextLink rescue nil
        if nextLink
          start_index+=@max_results
        else
          break
        end
      end
      if @interval.nil?
        break
      else
        duration = Time.now - start
        # Sleep for the remainder of the interval, or 0 if the duration ran
        # longer than the interval.
        sleeptime = [0, @interval - duration].max
        if sleeptime == 0
          @logger.warn("Execution ran longer than the interval. Skipping sleep.",
                       :duration => duration,
                       :interval => @interval)
        else
          Stud.stoppable_sleep(sleeptime) { stop? }
        end
      end
    end # loop
  end # def run

  def stop
  end

  private
  def client_options(results_index)
    options = {
      'ids' => @ids,
      'start-date' => @start_date,
      'end-date' => @end_date,
      'metrics' => @metrics,
      'max-results' => @max_results,
      'output' => 'json',
      'start-index' => results_index
    }
    options.merge!({ 'dimensions' => @dimensions }) if @dimensions
    options.merge!({ 'filters' => @filters }) if @filters
    options.merge!({ 'sort' => @sort }) if @sort
    options.merge!({ 'segment' => @segment }) if @segment
    options.merge!({ 'samplingLevel' => @sampling_level }) if @sampling_level
    options.merge!({ 'include-empty-rows' => @include_empty_rows }) if !@include_empty_rows.nil?
    return options
  end

  def get_service
    client = Google::APIClient.new(
      :application_name => 'Google Analytics Logstash Input',
      :application_version => '1.0.0')

    puts @key_file_path
    puts @key_secret
    puts @service_account_email
    # Load our credentials for the service account
    key = Google::APIClient::KeyUtils.load_from_pkcs12(@key_file_path, @key_secret)
    client.authorization = Signet::OAuth2::Client.new(
      :token_credential_uri => 'https://accounts.google.com/o/oauth2/token',
      :audience => 'https://accounts.google.com/o/oauth2/token',
      :scope => 'https://www.googleapis.com/auth/analytics.readonly',
      :issuer => @service_account_email,
      :signing_key => key)

    # Request a token for our service account
    client.authorization.fetch_access_token!
    analytics = client.discovered_api(@service_name, @api_version)
    return client, analytics
  end

  private
  def is_num(a)
    return (Float(a) and true) rescue false
  end
end # class LogStash::Inputs::GoogleAnalytics
