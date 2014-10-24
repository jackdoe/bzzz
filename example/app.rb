#!/usr/bin/ruby
require 'curb'
require 'json'
require 'sinatra'
require 'haml'
require 'cgi'
PER_PAGE = 10
class String
  def escape
    CGI::escapeHTML(self)
  end
  def escapeCGI
    CGI::escape(self)
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
                                       explain: options[:explain] || false,
                                       analyzer: Store.analyzer,
                                       highlight: { fields: options[:highlight] || ['content','filename'],
                                                    "use-text-fragments" => options[:use_text_fragments] || false,
                                                    separator: "__SEPARATOR__",
                                                    pre: "__HSTART__",
                                                    post: "__HEND__",
                                                  },
                                       page: options[:page] || 0,
                                       size: options[:size] || PER_PAGE,
                                       refresh: options[:refresh] || false}.to_json).body_str)
  end

  def Store.analyzer
    {
#      content: {
#        type: "custom",
#        tokenizer: "letter",
#        filter: [
#          {type: "lowercase"},
#          {type: "ngram",min_gram: 3,max_gram: 5}
#        ]
#      },
      content: {
        type: "standard"
      },
      filename: {
        type: "standard"
      }
    }
  end

  def Store.stat
    JSON.parse(Curl.get("#{@host}/_stat").body_str)
  end
end

def walk_and_index(path, every)
  raise "need block" unless block_given?
  docs = []
  pattern = "#{path}/**/*\.{c,java,pm,pl}"
  puts "indexing #{pattern}"
  Dir.glob(pattern).each do |f|
    name = f.gsub(path,'')
    doc = {
      id: name,
      content: File.read(f),
      filename: name
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
  v = ARGV
  v.shift
  v = ["/usr/src/linux"] unless ARGV.count > 0
  v.each do |dir|
    walk_and_index(dir,1000) do |slice|
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
  @page = @params[:page].to_i || 0
  @pages = 0

  if @q
    queries = []
    queries << {
      "query-parser" => {
        "defailt-operator" => "and",
        "default-field" => "content",
        query: @q
      }
    }

    queries << {
      "query-parser" => {
        "default-field" => "content",
        query: "\"#{@q}\""
      }
    } unless @q['"']

    queries << {
      "query-parser" => {
        "default-field" => "filename",
        query: @q,
        boost: 2
      }
    }
    begin
      res = Store.find({ bool: { should: queries } },explain: true, page: @page)
      @err = nil
      @total = res["total"]
      @took = res["took"]
      @pages = @total/PER_PAGE
      res["hits"].each do |h|
        row = {
          score: h["_score"],
          highlight: [],
          explain: h["_explain"],
          id: h["id"]
        }
        if h["_highlight"] && h["_highlight"]["content"]
          row[:highlight] = h["_highlight"]["content"].map { |x| x["text"] }.join("\n<i>---- cut ----</i>\n").escape.gsub("__HEND__","</b>").gsub("__HSTART__","<b>")
        end
        @results << row
      end
    rescue Exception => ex
      @total = -1
      @err = ex.message
    end
  end

  haml :index
end

get '/*' do
  @q = @params[:q]
  @id = @request.path
  @pages = 0
  @page = 0
  query = {
    bool: {
      must: [
        { term: { field: "id", value: @id }},
      ]
    }
  }
  if @q
    query[:bool][:must] << {
      "query-parser" => {
        "defailt-operator" => "and",
        "default-field" => "content",
        query: @q || ""
      }
    }
  end
  res = Store.find(query, {use_text_fragments: true})
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
    %script{src: "//ajax.googleapis.com/ajax/libs/jquery/2.1.1/jquery.min.js"}
  %body
    %form{ action: '/', method: 'GET' }
      %table{ border: 0, width: "100%", height: "8%" }
        %tr{ align: "center", valign: "center" }
          %td
            %input{ type: "text", name: "q", value: @q }
            %input{ type: "submit", name: "submit", value: "search" }
            - if @pages > 0
              - if @page - 1 > -1
                %a(href= "?q=#{@q}&page=#{@page - 1}")> &nbsp;prev
              - if @page < @pages * PER_PAGE
                %a(href= "?q=#{@q}&page=#{@page + 1}")> &nbsp;next
    %hr
    = yield

@@ index
%table{ border: 0, width: "100%", height: "100%" }
  %tr
    %td{collspan: 2, align: "center", valign: "center" }
      total: #{@total}, took: #{@took}ms, pages: #{@pages}, page: #{@page}
      - if @err
        -if @err["ParseException"]
          %br
          oops, seems like we received a <b>ParseException</b>, some type of queries are not parsable by the 
          %a(href="https://lucene.apache.org/core/4_9_0/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html") Lucene QueryParser
          %br
          For example <a href="?q=Time::HiRes">Time::HiRes</a> breaks it because of <b>:</b>. You can search for those using quotes like: <a href='?q="Time::HiRes"'>"Time::HiRes"</a>

        %br
        <pre>#{@err}</pre>
  - @results.each do |r|
    %tr
      %td
        %a{ onclick: "$(this).parent().find('.explain').toggle()"}
          explain
        %a(href= "#{r[:id]}?q=#{@q.escapeCGI}") score: #{r[:score]} file: #{r[:id]}
        %br
        %pre.explain{style: 'display:none'}
          #{r[:explain]}
        = preserve do
          <pre ondblclick="window.location = '#{r[:id]}?q=#{@q.escapeCGI}'">#{r[:highlight]}</pre>

@@ item
%center
  #{@id}
%hr
%pre
  #{@doc["content"].escape}
