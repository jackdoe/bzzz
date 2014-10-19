#!/usr/bin/ruby
require 'curb'
require 'json'
require 'sinatra'
require 'haml'
require 'cgi'

class String
  def escape
    CGI::escapeHTML(self)
  end
end

class Store
  @index = "example-index"
  @host = "http://localhost:3000"

  def Store.save(docs = [])
    docs = [docs] if !docs.kind_of?(Array)
    JSON.parse(Curl.post(@host, {index: @index, documents: docs, analyzer: Store.analyzer}.to_json).body_str)
  end

  def Store.delete(query)
    JSON.parse(Curl.http(:DELETE, @host, {index: @index, query: query}.to_json).body_str)
  end

  def Store.find(query, options = {})
    JSON.parse(Curl.http(:GET, @host, {index: @index,
                                       query: query,
                                       analyzer: Store.analyzer,
                                       highlight: { field: options[:highlight] || 'content_store_index',
                                                    separator: "__SEPARATOR__",
                                                    pre: "__HSTART__",
                                                    post: "__HEND__",
                                                  },
                                       page: options[:page] || 0,
                                       size: options[:size] || 10,
                                       refresh: options[:refresh] || false}.to_json).body_str)
  end

  def Store.analyzer
    { content_store_index: {use: "standard"}, filename_index: {use: "standard"}}
  end

  def Store.stat
    JSON.parse(Curl.get("#{@host}/_stat").body_str)
  end
end

def walk_and_index(path, every)
  raise "need block" unless block_given?
  docs = []
  puts "indexing #{path}/**/*\.c"
  Dir.glob("#{path}/**/*\.c").each do |f|
    blob = File.read(f)
    name = f.gsub(path,'')
    doc = {
      id: name,
      content_store_index: blob,
      filename_index: name
    }
    docs << doc
    if docs.length > every
      yield docs
      docs = []
    end
  end
  yield docs
end

if ARGV[0] == 'do-index'
  ["/usr/src/linux"].each do |dir|
    walk_and_index(dir,10000) do |slice|
      puts "sending #{slice.length} docs"
      Store.save(slice)
    end
  end
  p Store.stat
  exit 0
end

get '/' do
  @q = @params[:q]
  @results = []
  @total = 0
  @took = -1
  if @q
    res = Store.find("content_store_index:\"#{@q}\" OR filename_index:\"#{@q}\"^2")
    @total = res["total"]
    @took = res["took"]
    res["hits"].each do |h|
      @results << {
        score: h["_score"],
        highlight: h["_highlight"].escape.gsub("__HEND__","</b>").gsub("__HSTART__","<b>").gsub("__SEPARATOR__","\n---- cut ----\n"),
        id: h["id"]
      }
    end
  end
  haml :index
end

get '/*' do
  @id = @request.path
  res = Store.find({ term: { field: "id", value: @id }})
  error 404 if res["total"] == 0
  @doc = res["hits"].first
  haml :item
end

__END__

@@ layout
!!! 5
%html
  %head
    %title= "bzzz."
    %link{:rel => :stylesheet, :type => :"text/css", :href => "//maxcdn.bootstrapcdn.com/bootswatch/3.2.0/spacelab/bootstrap.min.css"}
  %body
    %form{ action: '/', method: 'GET' }
      %table{ border: 0, width: "100%", height: "10%" }
        %tr{ align: "center", valign: "center" }
          %td
            %input{ type: "text", name: "q", value: @q }
            %input{ type: "submit", name: "submit", value: "search" }
    %hr
    = yield

@@ index
%table{ border: 0, width: "100%", height: "100%" }
  %tr
    %td{collspan: 2, align: "center", valign: "center" }
      total: #{@total}, took: #{@took}ms
  - @results.each do |r|
    %tr
      %td
        %pre
          = preserve do
            %a(href= "#{r[:id]}")> score: #{r[:score]} file: #{r[:id]}
            %br
            #{r[:highlight]}

@@ item
%center
  #{@id}
%hr
%pre
  = preserve do
    #{@doc["content_store_index"].escape}
