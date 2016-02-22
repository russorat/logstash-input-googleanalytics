# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/googleanalytics"
require "vcr"
require "json"

ENV['SSL_CERT_FILE'] = "/Users/rsavage/Downloads/cacert.pem"

VCR.configure do |config|
  config.cassette_library_dir = File.join(File.dirname(__FILE__), '..', 'fixtures', 'vcr_cassettes')
  config.hook_into :webmock
end

RSpec.describe LogStash::Inputs::GoogleAnalytics do
  describe "inputs/googleanalytics" do
    context "get audience overview" do
      let(:options) do
        {
          'ids' => 'ga:97869209',
          'start_date' => '27daysAgo',
          'end_date' => '27daysAgo',
          'metrics' => 'ga:pageviews',
          'dimensions' => 'ga:date',
          'key_file_path' => '/Users/rsavage/workspace/logstash-1.4.2/logstash-input-googleanalytics-1c933e55eca6.p12',
          'key_secret' => 'notasecret',
          'service_account_email' => '759646568999-hto359j2lud906ae7ufts01djuu0n7j0@developer.gserviceaccount.com'
        }
      end
      let(:input) { LogStash::Inputs::GoogleAnalytics.new(options) }
      let(:expected_fields_result) { ["ga_pageviews"] }
      let(:queue) { [] }
      subject { input }
      it "loads pageviews" do
        #VCR.use_cassette("get_audience_overview") do
          subject.register
          subject.run(queue)
          expect(queue.length).to eq(1)
          e = queue.pop
          expected_fields_result.each do |f|
            expect(e.to_hash).to include(f)
          end
        #end
      end
    end
  end

end
