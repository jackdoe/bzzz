#!/usr/bin/env ruby
# encoding: utf-8

require 'zlib'
require 'curb'
require 'json'
require 'sinatra'
require 'haml'
require 'cgi'

set :sessions, false
set :logging, false
set :dump_errors, true
set :environment, :production
set :raise_errors, false
set :show_exceptions, true

PER_PAGE = 10
SHOW_AROUND_MATCHING_LINE = 2
IMPORTANT_LINE_SCORE = 1
IN_FILE_PATH_SCORE = 10

SEARCH_FIELD = "content_payload_no_norms_no_store"
DISPLAY_FIELD = "content_no_index"
F_IMPORTANT_LINE = 1 << 29
F_IS_IN_PATH = 1 << 30
LINE_SPLITTER = /[\r\n]/
class NilClass
  def empty?
    true
  end
end
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
    JSON.parse(Curl.post(@host, {index: @index, documents: docs, analyzer: Store.analyzer, "force-merge" => 1}.to_json).body_str)
  end

  def Store.delete(query)
    JSON.parse(Curl.http(:DELETE, @host, {index: @index, query: query}.to_json).body_str)
  end

  def Store.find(query, options = {})
    JSON.parse(Curl.http(:GET, @host, {index: @index,
                                       query: query,
                                       explain: options[:explain] || false,
                                       page: options[:page] || 0,
                                       size: options[:size] || PER_PAGE,
                                       refresh: options[:refresh] || false}.to_json).body_str)
  end

  def Store.analyzer
    {
      SEARCH_FIELD => { type: "custom", tokenizer: "whitespace", filter: [{ type: "delimited-payload" }] },
    }
  end

  def Store.stat
    JSON.parse(Curl.get("#{@host}/_stat").body_str)
  end
end

def is_important(x)
  return x.match(/\b(sub|public|private|package)\b/)
end
def encode(content)
  content.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
end

def tokenize_and_encode_payload(content,encode, init_flags = 0)
  lines = encode(content).split(LINE_SPLITTER);

  tokens = []

  lines.each_with_index do |line,line_index|
    flags = init_flags
    if is_important(line)
      flags |= F_IMPORTANT_LINE
    end
    line.split(/[^\w]+/).each_with_index do |token,token_index|
      if token.length > 0
        if encode
          token_flags = flags
          if is_important(token)
            token_flags &= ~F_IMPORTANT_LINE;
          end
          payload = token_flags | ((token_index & 0xFF) << 20) | (line_index & 0xFFFFF)
          tokens << "#{token}|#{payload}"
        else
          tokens << token
        end
      end
    end
  end

  tokens
end

def walk_and_index(path, every)
  raise "need block" unless block_given?
  docs = []
  pattern = "#{path}/**/*\.{c,java,pm,pl,rb}"
  puts "indexing #{pattern}"
  Dir.glob(pattern).each do |f|
    name = f.gsub(path,'')
    content = encode(File.read(f))
    tokenized = tokenize_and_encode_payload(content,true)
    tokenized.push(tokenize_and_encode_payload(f,true,F_IS_IN_PATH))

    doc = {
      id: name,
      DISPLAY_FIELD => content,
      SEARCH_FIELD => tokenized.join(" ")
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
    walk_and_index(dir,100) do |slice|
      puts "sending #{slice.length} docs"
      Store.save(slice)
    end
  end
  p Store.stat
  exit 0
end

get '/:page' do |page|
  status 404
  "not found"
end

get '/' do
  @q = @params[:q]
  @results = []
  @total = 0
  @took = -1
  @page = @params[:page].to_i || 0
  @pages = 0
  if !@q.empty?
    queries = []
    if !params[:id].empty?
      queries << {
        term: {
          field: "id",
          value: params[:id]
        }
      }
    end
    tokens = tokenize_and_encode_payload(@q,false)
    tokens.each_with_index do |t,t_index|
      queries << {
        "term-payload-clj-score" => {
          field: SEARCH_FIELD,
          value: t,
          "clj-eval" => %{
(fn [^bzzz.java.query.ExpressionContext ctx]
  (while (>= (.current-freq-left ctx) 0)
    (let [payload (.payload-get-int ctx)
          line-no (bit-and payload 0xFFFFF)
          line-key (str (if (.explanation ctx) "explain-" "no-explain-")
                        (.global_docID ctx)
                        "-"
                        line-no)
          seen-on-this-line (.local-state-get ctx line-key 0)
          on-important-line (if (> (bit-and payload #{F_IMPORTANT_LINE}) 0) #{IMPORTANT_LINE_SCORE} 0)
          in-file-path (> (bit-and payload #{F_IS_IN_PATH}) 0)
          pos-in-line (bit-and (bit-shift-right payload 20) 0xFF)]

      (if-not in-file-path
        (do
          (.result-state-append ctx {:payload payload, :query-token-index #{t_index}})
          (.local-state-set ctx line-key (+ 1 seen-on-this-line))))

      (when (.explanation ctx)
        (.explanation-add ctx seen-on-this-line (str "seen (" seen-on-this-line ") on line (" line-no ") line-key (" line-key ")")))

      (.current-score-add ctx seen-on-this-line)

      (if (> on-important-line 0)
        (do
          (when (.explanation ctx)
            (.explanation-add ctx on-important-line (str "important line: (" line-no ")")))
          (.current-score-add ctx on-important-line)))

      (when in-file-path
        (let [in-file-path-score (+ #{IN_FILE_PATH_SCORE} pos-in-line)]
          (when (.explanation ctx)
            (.explanation-add ctx in-file-path-score (str "in-file-path, #{IN_FILE_PATH_SCORE} + pos in path (" pos-in-line ")")))
          (.current-score-add ctx in-file-path-score)))

      (.postings-next-position ctx)))
  (when (.explanation ctx)
    (.explanation-add ctx (.maxed_tf_idf ctx) "maxed_tf_idf"))
  (float (+ (.maxed_tf_idf ctx) (.current-score ctx))))}
        }
      }
    end

    begin
      res = Store.find({ bool: { must: queries } },explain: true, page: @page)
      raise res["exception"] if res["exception"]
      @err = nil
      @total = res["total"] || 0
      @took = res["took"]
      @pages = @total/PER_PAGE

      res["hits"].each do |h|
        row = {
          score: h["_score"],
          explain: h["_explain"],
          id: h["id"],
          n_matches: 0,
        }

        state = h["_result_state"] || []

        matching = {}
        best_line_nr_matches = 0
        state.flatten.each do |item|
          payload = item["payload"]
          line_no = payload & 0xFFFFF
          matching[line_no] ||= {}
          matching[line_no][item["query-token-index"]] = true
          if best_line_nr_matches < matching[line_no].count
            best_line_nr_matches = matching[line_no].count
          end
        end

        highlighted = []
        around = 0
        h["content_no_index"].split(LINE_SPLITTER).each_with_index do |line,line_index|
          item = { show: false, bold: false, line_no: line_index, line: line.escape }

          if matching[line_index]
            item[:bold] = true
            item[:show] = matching[line_index].count == best_line_nr_matches

            row[:n_matches] += 1
            if item[:show]
              if highlighted.count > 1
                1.upto(SHOW_AROUND_MATCHING_LINE).each do |i|
                  begin
                    highlighted[-i][:show] = true
                  rescue
                  end
                end
              end

              around = SHOW_AROUND_MATCHING_LINE
            end
          else
            if around > 0
              item[:show] = true
            end
            around -= 1
          end

          highlighted << item
        end

        highlighted.each do |x|
          x[:line] = "#{x[:bold] ? '<b>' : ''}#{x[:line]}#{x[:bold] ? '</b>' : ''}"
        end

        if params[:id].empty?
          row[:highlight] = highlighted.select { |x| x[:show] }.map { |x| x[:line] }.join("\n")
        else
          row[:highlight] = highlighted.map { |x| x[:line] }.join("\n")
        end

        @results << row
      end

    rescue Exception => ex
      @total = -1
      @err = [ex.message,ex.backtrace.first(10)].flatten.join("\n")
    end
  end

  haml :index
end

__END__

@@ form
%form{ action: '/', method: 'GET' }
  %input{ type: "text", name: "q", value: @q, autofocus: true}
  %input{ type: "submit", value: "search" }
  &nbsp;
  - if @pages > 0
    - if @page - 1 > -1
      %a(href= "?q=#{@q}&page=#{@page - 1}")> prev
    - else
      <strike>prev</strike>
    &nbsp;
    - if @page < @pages
      %a(href= "?q=#{@q}&page=#{@page + 1}")> next
    -else
      <strike>next</strike>

  &nbsp;took: #{@took}ms, matching documents: #{@total}, pages: #{@pages}, page: #{@page}

@@ layout
!!! 5
%html
  %head
    %title= "bzzz."
    =preserve do
      <style>.section { display: none;} .section:target {display: block;} table {border-collapse: collapse;} table, th, td {border: 1px solid black;} a { text-decoration: none; color: gray;}</style>

  %body
    = yield

@@ index
%table{ border: 1, width: "100%", height: "100%" }
  %tr
    %td{id: "top"}
      #{haml :form}
  - if @err
    %tr
      %td{align: "left", valign: "left" }
        -if @err["ParseException"]
          %br
          oops, seems like we received a <b>ParseException</b>, some type of queries are not parsable by the 
          %a(href="https://lucene.apache.org/core/4_9_0/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html") Lucene QueryParser
          %br
          For example <a href="?q=Time::HiRes">Time::HiRes</a> breaks it because of <b>:</b>. You can search for those using quotes like: <a href='?q="Time::HiRes"'>"Time::HiRes"</a>

        %br
        <pre>#{@err}</pre>

  - if @results.count > 0
    %tr
      %td
        %ul
          current page:
          - @results.each do |r|
            %li
              %a{ href: "##{r[:id]}"}
                #{r[:id]}
              matching lines: #{r[:n_matches]}

  - @results.each_with_index do |r,r_index|
    %tr
      %td{id: r[:id]}
        %div{id: "menu_#{r_index}"}
          %a{ href: "#menu_#{r_index - 1}"} &#9668;
          %a{ href: "#top"} &#9650;
          %a{ href: "#menu_#{r_index + 1}"} &#9658;

          %a{ href: "#explain_#{r_index}"} explain score: #{r[:score]}
          file: <a href="?q=#{@q}&id=#{r[:id]}">#{r[:id]}</a>

        %pre.section{id: "explain_#{r_index}"}
          <br><a href="##{r[:id]}">hide explain #{r[:id]}</a><br><font color="red">---</font><br>#{r[:explain]}

        = preserve do
          <pre id="highlighted_#{r_index}">#{r[:highlight]}</pre>

  -if @results.count > 0
    %tr
      %td
        #{haml :form}

  %tr
    %td
      - if @results.count == 0 && @q.empty?
        %ul <b>case sensitive</b> indexed: linux-git, glibc-2.2.0, perl-git, lucene-solar-git, hadoop-git, zookeeper-git, clojure-git.
        %li
          %a{ href: "?q=goto+drop+udp"} goto drop udp
        %li
          %a{ href: "?q=PayloadHelper+encodeFloat"} PayloadHelper encodeFloat
        %li
          %a{ href: "?q=IndexSearcher"} IndexSearcher
        %li
          %a{ href: "?q=postings+nextPosition"} postings nextPosition

      using <a href="https://github.com/jackdoe/bzzz">github.com/jackdoe/bzzz</a> lucene wrapper, __FILE__ lives at: <a href="https://github.com/jackdoe/bzzz/blob/master/example/app.rb">https://github.com/jackdoe/bzzz/blob/master/example/app.rb</a> <b>patches/issues welcome</b>
