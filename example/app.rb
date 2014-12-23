#!/usr/bin/env ruby
# encoding: utf-8
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

F_IMPORTANT_LINE = 1 << 28
F_IS_IN_PATH = 1 << 29
IMPORTANT = { "sub" => true, "public" => true, "private" => true, "package" => true }

LINE_SPLITTER = /[\r\n]/
REQUEST_FILE_RE = /\B@\w+/

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
    JSON.parse(Curl.post(@host, {index: @index, documents: docs, analyzer: Store.analyzer, "force-merge" => 1 }.to_json).body_str)
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

ORD_a = 'a'.ord
ORD_aa = 'A'.ord
ORD_z = 'z'.ord
ORD_zz = 'Z'.ord
ORD_zero = '0'.ord
ORD_nine = '9'.ord
ORD_space = ' '.ord
ORD_semicolon = ';'.ord
ORD_colon = ':'.ord
ORD_at = '@'.ord
ORD_bang = '!'.ord
ORD_slash = '/'.ord
ORD_underscore = '_'.ord

def tokenize(line)
  # well... this is slow :)
  buf = ""
  pos = 0
  flags = 0
  chars = line.chars
  chars_max_index = chars.count
  i = 0

  while i < chars_max_index

    char = chars[i]
    code = char.ord

    if (code >= ORD_a && code <= ORD_z) || (code >= ORD_aa && code <= ORD_zz) || (code >= ORD_zero && code <= ORD_nine) || code == ORD_underscore
      buf << char
    else
      if buf.length > 0
        yield buf, flags, pos += 1
        flags |= F_IMPORTANT_LINE if IMPORTANT[buf]
        buf = ""
      end

      if code != ORD_space && code != ORD_semicolon && ((code >= ORD_colon && code < ORD_at) || (code >= ORD_bang && code <= ORD_slash))
        while chars[i] == char && i < chars_max_index
          buf << chars[i]
          i += 1
        end
        i -= 1 # will be advanced in the bottom of the while loop
        yield buf, flags, pos += 1
        buf = ""
      end
    end

    i += 1
  end

  yield buf, flags, pos if buf.length > 0
  nil
end

def tokenize_and_encode_payload(content, init_flags = 0, line_index_offset = 0)
  lines = content.split(LINE_SPLITTER);

  tokens = []
  lines.each_with_index do |line,line_index|

    line_index += line_index_offset

    tokenize(line) do |token, token_flags, token_index|
      token_flags |= init_flags
      payload = token_flags | ((token_index & 0xFF) << 20) | (line_index & 0xFFFFF)
      tokens << "#{token}|#{payload}"
    end
  end

  tokens
end

def walk_and_index(path, every)
  raise "need block" unless block_given?
  docs = []
  pattern = "#{path}/**/*\.{c,java,pm,pl,rb,clj,inc}"
  puts "indexing #{pattern}"

  files = Dir.glob(pattern)

  Dir.glob(pattern).each do |f|

    name = f.gsub(path,'')
    content = encode(File.read(f))

    tokenized = tokenize_and_encode_payload(content)
    tokenized.push(tokenize_and_encode_payload(f,F_IS_IN_PATH,1000000))

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
    t0 = Time.now
    walk_and_index(dir,1000) do |slice|
      puts "sending #{slice.length} docs, tokenizing took #{Time.now - t0} to tokenize"
      t0 = Time.now
      Store.save(slice)
    end
  end

  p Store.stat
  exit 0
end

EXPR_EXPLAIN_BIT = 4
EXPR_IN_FILE_BIT = 2
EXPR_IMPORTANT_BIT = 1
EXPR_SUM_SCORE_BIT = 8
def clojure_expression_terms(tokens, in_file = false)
  queries = []
  all_tokens_match_mask = (0xffffffff >> (32 - tokens.count)) # used inside the expression

  tokens.each_with_index do |user_input_token, t_index|
    token_bit = 1 << t_index # used inside the expression

    term = {
        "term-payload-clj-score" => {
          field: SEARCH_FIELD,
          value: user_input_token,
          "clj-eval" => %{
;; NOTE: user input here simply leads to RCE!
(fn [^bzzz.java.query.ExpressionContext ctx]
  (let [maxed-tf-idf (.maxed_tf_idf ctx)
        sum-score-key (bit-or (bit-shift-left (.global_docID ctx) 32)
                              #{EXPR_SUM_SCORE_BIT}
                              (if (.explanation ctx) #{EXPR_EXPLAIN_BIT} 0))
        sum-score (+ maxed-tf-idf (.local-state-get ctx sum-score-key 0))]
    (.local-state-set ctx sum-score-key sum-score)
    (.invoke-for-each-int-payload
     ctx
     (fn [payload]
       (let [line-no (bit-and payload 0xFFFFF)
             ;; (doc-id << 32) | (line-no << 8) | (explanation ? explanation-bit : 0) | (in-file ? in-file-bit : 0)
             line-key (bit-or (bit-shift-left (.global_docID ctx) 32)
                              (bit-shift-left line-no 8)
                              (if (.explanation ctx) #{EXPR_EXPLAIN_BIT} 0)
                              #{in_file ? "#{EXPR_IN_FILE_BIT}" : "0"})

             ;; translates to matches[line] |= current token position bit
             uniq-tokens-seen-on-this-line (bit-or (.local-state-get ctx line-key 0) #{token_bit})

             valid-match #{in_file ? "(> (bit-and payload #{F_IS_IN_PATH}) 0)" : "true"}]

         (when valid-match
           ;; TODO(bnikolov):
           ;; some tokens have matches on every line, so for 10k lines it will actually
           ;; do 10k sets, the easiest thing to do is just link those tokens to something
           ;; because they are pointless by themselves.
           ;; for example "int" or "."
           ;; so we can tokenize 'int main void' as 'int$main main void'
           ;; if searched with 'int main void' we look for 'int$main void'
           ;; but this line wont be findable with 'int' only
           ;; which greatly improves this case
           (.local-state-set ctx line-key uniq-tokens-seen-on-this-line))

         (if (and valid-match (= uniq-tokens-seen-on-this-line #{all_tokens_match_mask}))
           (do
             (.current-counter-set ctx 1)
             (.current-score-add ctx #{ALL_TOKENS_MATCH_SCORE})
             (when (.explanation ctx)
               (.explanation-add ctx #{ALL_TOKENS_MATCH_SCORE} (str "line: (" line-no ") match mask: #{all_tokens_match_mask}"))
               (.result-state-append ctx payload))
             (when (and (> (bit-and payload #{F_IMPORTANT_LINE}) 0) (not (.local-state-get ctx (bit-or line-key #{EXPR_IMPORTANT_BIT}))))
               ;; score important lines only once
               (.local-state-set ctx (bit-or line-key #{EXPR_IMPORTANT_BIT}) 1)
               (when (.explanation ctx)
                 (.explanation-add ctx #{IMPORTANT_LINE_SCORE} (str "line: (" line-no ") considered important")))
               (.current-score-add ctx #{IMPORTANT_LINE_SCORE})))))
       nil))

    (if (= (.current-counter ctx) 1)
      (do
        (when (.explanation ctx)
          (.explanation-add ctx sum-score "summed tf-idf scores"))
        (float (+ (.local-state-get ctx sum-score-key 0) (.current-score ctx))))
      (float 0))))}

        }
      }

    queries << term

  end

  return queries
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

    in_file_tokens = @q.scan(REQUEST_FILE_RE).map { |x| x.gsub(/^@/,"") }

    tokens = []

    tokenize(@q.gsub(REQUEST_FILE_RE,"")) do |token, flags, pos|
      tokens << token
    end

    if in_file_tokens.count > 0
      queries << {
        "no-zero-score"=> {
          query: {
            bool: {
              must: clojure_expression_terms(in_file_tokens,true)
            }
          }
        }
      }
    end

    if tokens.count > 0
      queries << {
        "no-zero-score"=> {
          query: {
            bool: {
              must: clojure_expression_terms(tokens,false)
            }
          }
        }
      }
    end

    begin
      res = Store.find({bool: { must: queries } } ,explain: true, page: @page)
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
        state.flatten.each do |payload|
          matching[payload & 0xFFFFF] = true
        end

        highlighted = []
        around = 0
        colors = ["#3B4043","#666699"]
        h["content_no_index"].split(LINE_SPLITTER).each_with_index do |line,line_index|
          item = { show: false, bold: false, line_no: line_index, line: line.escape }

          if matching[line_index]
            item[:bold] = true
            item[:show] = true
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
  %input{ type: "text", name: "q", value: @q, autofocus: (@results.count == 0), placeholder: "defn reduce"}
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
        %ul <b>case sensitive</b> indexed: clojure elasticsearch glibc-2.20 hadoop linux lucene-solr rails ruby zookeeper perl5, use <b>@</b> to request a match within the path name (like <b>@linux</b>)
        %li
          %a{ href: "?q=struct+rtl8169_private+*"} struct rtl8169_private *
        %li
          %a{ href: "?q=%40glibc+%40malloc+realloc"} @glibc @malloc realloc
        %li
          %a{ href: "?q=PayloadHelper+encodeFloat"} PayloadHelper encodeFloat
        %li
          %a{ href: "?q=IndexSearcher"} IndexSearcher
        %li
          %a{ href: "?q=postings+nextPosition"} postings nextPosition

      using <a href="https://github.com/jackdoe/bzzz">github.com/jackdoe/bzzz</a> lucene wrapper, __FILE__ lives at: <a href="https://github.com/jackdoe/bzzz/blob/master/example/app.rb">https://github.com/jackdoe/bzzz/blob/master/example/app.rb</a> <b>patches/issues welcome</b>
