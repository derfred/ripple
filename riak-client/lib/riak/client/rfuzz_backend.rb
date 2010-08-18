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
require 'riak'

module Riak
  class Client
    # Uses the Ruby standard library Net::HTTP to connect to Riak.
    # We recommend using the CurbBackend, which will
    # be preferred when the 'curb' library is available.
    # Conforms to the Riak::Client::HTTPBackend interface.
    class RFuzzBackend < HTTPBackend

      def initialize(*args)
        require "rfuzz/client"
        super
      end

      private
      def perform(method, uri, headers, expect, data=nil) #:nodoc:
        client = RFuzz::HttpClient.new(uri.host, uri.port)
        response = client.send(method, uri.request_uri, :head => split_and_arrayify_headers(headers, 8000), :body => data)

        {}.tap do |result|
          if valid_response?(expect, response.http_status)
            result.merge!({:headers => translate_keys(response), :code => response.http_status.to_i})
            (response.raw_chunks||[response.http_body]).each { |chunk| yield chunk } if block_given?
            if return_body?(method, response.http_status, block_given?)
              result[:body] = response.http_body
            end
          else
            raise FailedRequest.new(method, expect, response.http_status, response.to_hash, response.http_body)
          end
        end
      end

      def translate_keys(hash)
        {}.tap do |result|
          hash.each do |k, v|
            result[k.downcase.sub("_", "-")] = [v]
          end
        end
      end

      def split_and_arrayify_headers(headers, max_size)
        {}.tap do |result|
          headers.each do |k, v|
            if k == "Link" and v.size > max_size
              result[k] = split_links_header(v, max_size)
            else
              result[k] = v
            end
          end
        end
      end

      def split_links_header(links, max_size)
        links = links.split(", ")
        [].tap do |result|
          current = ""
          links.each do |link|
            if current.size + link.size + 2 > max_size
              result << current
              current = link
            else
              current << ", " unless current.empty?
              current << link
            end
          end
          result << current unless current.empty?
        end
      end
    end
  end
end
