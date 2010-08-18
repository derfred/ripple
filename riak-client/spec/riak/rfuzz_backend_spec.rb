# Copyright 2010 Sean Cribbs, Sonian Inc., and Basho Technologies, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
require File.expand_path("../spec_helper", File.dirname(__FILE__))

$server = MockServer.new
at_exit { $server.stop }

describe Riak::Client::RFuzzBackend do
  before :each do
    @client = Riak::Client.new(:port => $server.port)
    @backend = Riak::Client::RFuzzBackend.new(@client)
  end

  after :each do
    $server.detach
  end

  def setup_http_mock(method, uri, options={})
    method = method.to_s.upcase
    uri = URI.parse(uri)
    path = uri.path || "/"
    query = uri.query || ""
    status = options[:status] ? Array(options[:status]).first.to_i : 200
    body = options[:body] || []
    headers = options[:headers] || {}
    headers['Content-Type'] ||= "text/plain"
    $server.attach do |env|
      env["REQUEST_METHOD"].should == method
      env["PATH_INFO"].should == path
      env["QUERY_STRING"].should == query
      [status, headers, Array(body)]
    end
  end

  it_should_behave_like "HTTP backend"

  it "should split long Link headers into smaller pieces" do
    links = (1...200).map { |i| "</riak/my_extra_long_link_names/cause_mochiweb_buffers_to>; riaktag=\"fillup\"" }.join(", ")
    expected = [1..102, 103...200].map do |range|
      range.map { |i| "</riak/my_extra_long_link_names/cause_mochiweb_buffers_to>; riaktag=\"fillup\"" }.join(", ")
    end
    RFuzz::HttpClient.stub!(:new).and_return(client = mock(:client))
    client.should_receive(:put).with(anything, :head => { "Link" => expected }, :body => nil).and_return(mock(:response, :http_status => "200", :each => nil, :http_body => ""))
    @backend.send(:perform, :put, @backend.path("/foo"), { "Link" => links }, 200)
  end
end
