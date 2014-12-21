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
ALL_TOKENS_MATCH_SCORE = 10

SEARCH_FIELD = "content_payload_no_norms_no_store"
DISPLAY_FIELD = "content_no_index"
F_IMPORTANT_LINE = 1 << 29
F_IS_IN_PATH = 1 << 30
F_IS_SYMBOL = 1 << 31

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
    JSON.parse(Curl.post(@host, {index: @index, documents: docs, analyzer: Store.analyzer }.to_json).body_str)
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

def bold_and_color(x)
  line = "<span id='line_#{x[:line_no]}'>#{x[:line]}</span>"
  if !x[:color].empty?
    line = "<font color='#{x[:color]}'>#{line}</font>"
  end
  "#{x[:bold] ? '<b>' : ''}#{line}#{x[:bold] ? '</b>' : ''}"
end

def tokenize_and_encode_payload(content, encode = false, init_flags = 0, line_index_offset = 0)
  lines = content.split(LINE_SPLITTER);

  tokens = []

  lines.each_with_index do |line,line_index|
    flags = init_flags
    if is_important(line)
      flags |= F_IMPORTANT_LINE
    end
    line_index += line_index_offset

    line.split(/([^\w])+/).select { |x| !(x =~ /^\s+$/) }.each_with_index do |token,token_index|
      if token.length > 0 && token != "|" # XXX: | is our payload delimiter
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
  pattern = "#{path}/**/*\.{c,java,pm,pl,rb,clj}"
  puts "indexing #{pattern}"
  Dir.glob(pattern).each do |f|
    name = f.gsub(path,'')
    content = encode(File.read(f))

    tokenized = tokenize_and_encode_payload(content,true)
    tokenized.push(tokenize_and_encode_payload(f,true,F_IS_IN_PATH,1000000))

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
    walk_and_index(dir,1000) do |slice|
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
    if !@params[:id].empty?
      queries << {
        term: {
          field: "id",
          value: @params[:id]
        }
      }
    end

    tokens = tokenize_and_encode_payload(@q,false)

    all_tokens_match_mask = (0xffffffff >> (32 - tokens.count))

    tokens.each_with_index do |user_input_token,t_index|
      term = {
        "term-payload-clj-score" => {
          field: SEARCH_FIELD,
          value: user_input_token,
          "clj-eval" => %{
;; NOTE: user input here simply leads to RCE!
(fn [^bzzz.java.query.ExpressionContext ctx]
  (while (>= (.current-freq-left ctx) 0)
    (let [payload (.payload-get-int ctx)
          line-no (bit-and payload 0xFFFFF)
          line-key (str (if (.explanation ctx) "explain-" "no-explain-")
                        (.global_docID ctx)
                        "-"
                        line-no)

          ;; translates to matches[line] |= current position big
          uniq-tokens-seen-on-this-line (bit-or (.local-state-get ctx line-key 0) (bit-shift-left 1 #{t_index}))

          on-important-line (if (> (bit-and payload #{F_IMPORTANT_LINE}) 0) #{IMPORTANT_LINE_SCORE} 0)
          in-file-path (> (bit-and payload #{F_IS_IN_PATH}) 0)
          pos-in-line (bit-and (bit-shift-right payload 20) 0xFF)]

      (.postings-next-position ctx)

      (.local-state-set ctx line-key uniq-tokens-seen-on-this-line)
      (if (= uniq-tokens-seen-on-this-line #{all_tokens_match_mask})
        (do
          (.current-score-add ctx #{ALL_TOKENS_MATCH_SCORE})
          (.explanation-add ctx #{ALL_TOKENS_MATCH_SCORE} (str "line: <" line-no "> all uniq tokens match with mask #{all_tokens_match_mask}"))
          (.result-state-append ctx {:payload payload, :query-token-index #{t_index}})
          (if (and (> on-important-line 0) (not (.local-state-get ctx (str "important-" line-key))))
            (do
              ;; score important lines only once
              (.local-state-set ctx (str "important-" line-key) true)
              (when (.explanation ctx)
                (.explanation-add ctx on-important-line (str "line: <" line-no "> considered important")))
              (.current-score-add ctx on-important-line)))))))

  (if (> (.current-score ctx) (float 0.0))
    (do
      (when (.explanation ctx)
        (.explanation-add ctx (.maxed_tf_idf ctx) "maxed_tf_idf"))
      (float (+ (.maxed_tf_idf ctx) (.current-score ctx))))
    (float 0)))}
        }
      }
      queries << term
    end

    begin
      res = Store.find({ "no-zero-score" => { query: { bool: { must: queries } } } } ,explain: true, page: @page)
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
        colors = ["#3B4043","#666699"]
        h["content_no_index"].split(LINE_SPLITTER).each_with_index do |line,line_index|
          item = { show: false, bold: false, line_no: line_index, line: line.escape }

          if matching[line_index]
            item[:bold] = true
            item[:show] = matching[line_index].count == best_line_nr_matches
            item[:color] = colors[0]
            row[:n_matches] += 1
            row[:first_match] ||= line_index
            if item[:show]
              if @params[:id].empty? && highlighted.count > 1
                1.upto(SHOW_AROUND_MATCHING_LINE).each do |i|
                  begin
                    highlighted[-i][:show] = true
                    highlighted[-i][:color] ||= colors[0]
                  rescue
                  end
                end
              end

              around = SHOW_AROUND_MATCHING_LINE
            end
          else
            if around > 0
              item[:show] = true
              item[:color] = colors[0]
              around -= 1
              if around == 0
                colors.rotate!
              end
            end
          end

          highlighted << item
        end

        if @params[:id].empty?
          row[:highlight] = highlighted.select { |x| x[:show] }.map { |x| bold_and_color(x) }.join("\n")
        else
          row[:highlight] = highlighted.map { |x| bold_and_color(x) }.join("\n")
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
  %input{ type: "text", name: "q", value: @q, autofocus: @q.empty?, placeholder: "defn reduce"}
  %input{ type: "submit", value: "search" }
  &nbsp;
  - if @pages > 0
    - if @page - 1 > -1
      %a(href= "?q=#{@q.escapeCGI}&page=#{@page - 1}")> prev
    - else
      <strike>prev</strike>
    &nbsp;
    - if @page < @pages
      %a(href= "?q=#{@q.escapeCGI}&page=#{@page + 1}")> next
    -else
      <strike>next</strike>
  &nbsp;took: #{@took}ms, matching documents: #{@total}, pages: #{@pages}, page: #{@page}

@@ layout
!!! 5
%html
  %head
    %title= "bzzz."
    =preserve do
      <style>.section { display: none;} .section:target {display: block;} table { border-collapse: collapse; border-style: hidden; } table td, table th { border: 1px solid black; } a { text-decoration: none; color: gray;}</style>

  %body
    = yield

@@ index
%table{ border: 1, width: "100%", height: "100%" }
  %tr
    %td{id: "top"}
      #{haml :form}
  - if @err
    %tr
      %td
        <pre>#{@err}</pre>

  - if @params[:id].empty? && @results.count > 0
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
          -if @params[:id].empty?
            %a{ href: "#menu_#{r_index - 1}"} &#9668;
            %a{ href: "#top"} &#9650;
            %a{ href: "#menu_#{r_index + 1}"} &#9658;
          - else
            %a{ href: "?q=#{@q.escapeCGI}"} &#9650;

          %a{ href: "#explain_#{r_index}"} explain score: #{r[:score]}
          file: <a href="?q=#{@q.escapeCGI}&id=#{r[:id]}&back=#{r_index}#line_#{r[:first_match]}">#{r[:id]}</a>

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
        %ul <b>case sensitive</b> indexed: clojure elasticsearch glibc-2.20 hadoop linux lucene-solr rails ruby zookeeper perl5
        %li
          %a{ href: "?q=struct+rtl8169_private+*"} struct rtl8169_private *
        %li
          %a{ href: "?q=PayloadHelper+encodeFloat"} PayloadHelper encodeFloat
        %li
          %a{ href: "?q=IndexSearcher"} IndexSearcher
        %li
          %a{ href: "?q=postings+nextPosition"} postings nextPosition

      using <a href="https://github.com/jackdoe/bzzz">github.com/jackdoe/bzzz</a> lucene wrapper, __FILE__ lives at: <a href="https://github.com/jackdoe/bzzz/blob/master/example/app.rb">https://github.com/jackdoe/bzzz/blob/master/example/app.rb</a> <b>patches/issues welcome</b>
