#!/usr/bin/env ruby
# encoding: utf-8
require 'curb'
require 'json'
require 'sinatra'
require 'haml'
require 'cgi'
require 'digest/sha1'

set :sessions, false
set :logging, false
set :dump_errors, true
set :environment, :production
set :raise_errors, false
set :show_exceptions, true

PER_PAGE = 15
SHOW_AROUND_MATCHING_LINE = 2
IMPORTANT_LINE_SCORE = 1
ALL_TOKENS_MATCH_SCORE = 10

DEFAULT_SOURCE_ROOT = File.expand_path(File.join("..","..",File.dirname(__FILE__),"SOURCE-TO-INDEX"))
DEFAULT_SOURCE_ROOT_STATUS_NAME = "git.status"
DEFAULT_SOURCE_ROOT_STATUS = File.join(DEFAULT_SOURCE_ROOT,DEFAULT_SOURCE_ROOT_STATUS_NAME)

REPOSITORIES = ["https://github.com/apache/lucene-solr",
                "https://github.com/apache/hadoop",
                "https://github.com/mysql/mysql-server",
                "https://github.com/postgres/postgres",
                "https://github.com/torvalds/linux",
                "git://sourceware.org/git/glibc.git",
                "https://github.com/Perl/perl5",
                "https://github.com/openjdk-mirror/jdk7u-jdk",
                "https://github.com/ruby/ruby",
                "https://github.com/clojure/clojure",
                "https://github.com/elasticsearch/elasticsearch",
                "https://github.com/rails/rails",
                "https://github.com/antirez/redis",
                "https://github.com/antirez/sds",
                "https://github.com/redis/hiredis",
                "https://github.com/git/git",
                "https://github.com/rust-lang/rust",
                "https://github.com/jemalloc/jemalloc",
                "https://github.com/balzaczyy/golucene",
                "https://github.com/nginx/nginx",
                "https://github.com/unbit/uwsgi",
                "https://github.com/plack/Plack",
                "https://github.com/Sereal/Sereal",
                "https://github.com/golang/go"]

SEARCH_FIELD = "content_no_norms"
FILENAME_FIELD = "filename_no_norms_no_store"
HASH_FIELD = "hash"
ID_FIELD = "id"
STAMP_FIELD = "stamp_long"

F_IMPORTANT_LINE = 1 << 31
LINE_SPLITTER = /[\r\n]/
REQUEST_FILE_RE = /\B@\w+/

class NilClass
  def empty?
    true
  end
end
class Time
  def took
    sprintf "%.4f",Time.now - self
  end
end
class String
  def escape
    CGI::escapeHTML(self)
  end
  def sha
    Digest::SHA1.hexdigest(self)
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
    docs.each do |doc|
      doc[ID_FIELD] = doc[FILENAME_FIELD]
      doc[HASH_FIELD] = doc[SEARCH_FIELD].sha
      doc[STAMP_FIELD] = Time.now.to_i
    end

    # if we dont want to overwrite everything
    # we just look only for docs with sha changes
    if !ENV["BZZZ_EXAMPLE_OVERWRITE"]
      not_changed = {}

      # TODO(bnikolov): create fast multi-lookup thing in bzzz itself
      # will be cool if we can send query: { lookup: .. bunch of id }

      docs.each_slice(512) do |slice|

        queries = []
        slice.each do |doc|
          queries << {
            bool: {
              must: [
                {
                  term: {
                    field: ID_FIELD,
                    value: doc[ID_FIELD]
                  }
                },
                {
                  term: {
                    field: HASH_FIELD,
                    value: doc[HASH_FIELD]
                  }
                }
              ]
            }
          }
        end

        res = Store.find({ bool: { should: queries } }, {size: queries.count, fields: {ID_FIELD => true} })

        res["hits"].each do |hit|
          not_changed[hit[ID_FIELD]] = true
        end

      end

      docs = docs.select { |x| !not_changed[x[ID_FIELD]] }

    end

    docs.each do |x|
      x[SEARCH_FIELD] = encode(x[SEARCH_FIELD])
    end

    JSON.parse(Curl.post(@host, {index: @index, documents: docs, analyzer: Store.analyzer, "force-merge" => ENV["BZZZ_EXAMPLE_FORCE_MERGE"] || 0}.to_json).body_str)
    return docs
  end

  def Store.delete(query)
    JSON.parse(Curl.http(:DELETE, @host, {index: @index, query: query}.to_json).body_str)
  end

  def Store.find(query, options = {})
    JSON.parse(Curl.http(:GET, @host, {index: @index,
                                       query: query,
                                       fields: options[:fields],
                                       analyzer: Store.analyzer,
                                       explain: options[:explain] || false,
                                       page: options[:page] || 0,
                                       size: options[:size] || PER_PAGE,
                                       refresh: options[:refresh] || false}.to_json).body_str)
  end

  def Store.analyzer
    {
      SEARCH_FIELD => { type: "custom", tokenizer: "code" },
      FILENAME_FIELD => { type: "custom", tokenizer: "code", "line-offset" => 100000 }
    }
  end

  def Store.stat
    JSON.parse(Curl.get("#{@host}/_stat").body_str)
  end
end


def encode(content)
  content.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
end

def bold_and_color(x, max_line_digts = 0, link = nil)
  line_no = ""
  if max_line_digts > 0
    if link
      line_no = sprintf "<a style='color: %s' href='%s#line_%d'>%#{max_line_digts}d</a> | ",x[:color] || 'black', link, x[:line_no], x[:line_no]
    else
      line_no = sprintf "%#{max_line_digts}d | ", x[:line_no]
    end
  end

  line = "#{line_no}<span id='line_#{x[:line_no]}'>#{x[:line]}</span>"
  if !x[:color].empty?
    line = "<font color='#{x[:color]}'>#{line}</font>"
  end

  "#{x[:bold] ? '<b>' : ''}#{line}#{x[:bold] ? '</b>' : ''}"
end

def walk_and_index(path, every)
  raise "need block" unless block_given?
  docs = []
  pattern = "#{path}/**/*\.{c,java,pm,pl,rb,clj,inc,go,rs}"
  puts "indexing #{pattern}"

  files = Dir.glob(pattern)
  Dir.glob(pattern).sort.each do |f|

    doc = {
      SEARCH_FIELD => File.read(f),
      FILENAME_FIELD => f.gsub(path,'')
    }

    docs << doc

    if docs.length > every
      yield docs
      docs = []
    end

  end
  yield docs
end

def dump_git_status
  begin
    File.read(DEFAULT_SOURCE_ROOT_STATUS)
  rescue
    ".. unable to open the status file .."
  end
end

if ARGV[0] == 'do-index'
  # FIXME: do this properly
  system("for i in `ls -1 #{DEFAULT_SOURCE_ROOT} | grep -v #{DEFAULT_SOURCE_ROOT_STATUS_NAME}`; do pushd #{DEFAULT_SOURCE_ROOT}/$i; git pull ; popd; done")

  v = ARGV
  v.shift
  v = [DEFAULT_SOURCE_ROOT] unless ARGV.count > 0

  v.each do |dir|
    t0 = Time.now
    walk_and_index(dir,1000) do |slice|
      print "read/list for #{slice.count} documents took: #{t0.took} to list/read .. "
      t0 = Time.now
      slice = Store.save(slice)
      puts "save took: #{t0.took} for #{slice.count} changed documents"
      t0 = Time.now
    end
  end

  system("rm -f #{DEFAULT_SOURCE_ROOT_STATUS}.tmp; for i in `ls -1 #{DEFAULT_SOURCE_ROOT} | grep -v #{DEFAULT_SOURCE_ROOT_STATUS_NAME} | sort`; do pushd #{DEFAULT_SOURCE_ROOT}/$i && echo `date` $i `git rev-parse HEAD` >> #{DEFAULT_SOURCE_ROOT_STATUS}.tmp; popd; done; mv #{DEFAULT_SOURCE_ROOT_STATUS}.tmp #{DEFAULT_SOURCE_ROOT_STATUS}")

  p Store.stat
  exit 0
elsif ARGV[0] == 'source-init'
  # FIXME: do this properly
  system("mkdir -p #{DEFAULT_SOURCE_ROOT}; cd #{DEFAULT_SOURCE_ROOT} && for i in #{REPOSITORIES.join(" ")}; do git clone --depth 1 $i; done")
  exit 0
end

EXPR_EXPLAIN_BIT = 4
EXPR_IMPORTANT_BIT = 1
EXPR_SUM_SCORE_BIT = 8
def clojure_expression_terms(search_string)
  return {
    "term-payload-clj-score" => {
      field: SEARCH_FIELD,
      value: search_string,
      tokenize: true,
      "match-all-if-empty" => true,
      "no-zero" => true,
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
             ;; (doc-id << 32) | (line-no << 8) | (explanation ? explanation-bit : 0)
             line-key (bit-or (bit-shift-left (.global_docID ctx) 32)
                              (bit-shift-left line-no 8)
                              (if (.explanation ctx) #{EXPR_EXPLAIN_BIT} 0))

             ;; translates to matches[line] |= current token position bit
             uniq-tokens-seen-on-this-line (bit-or (.local-state-get ctx line-key 0) (.token_bit_position ctx))]

         ;; TODO(bnikolov):
         ;; some tokens have matches on every line, so for 10k lines it will actually
         ;; do 10k sets, the easiest thing to do is just link those tokens to something
         ;; because they are pointless by themselves.
         ;; for example "int" or "."
         ;; so we can tokenize 'int main void' as 'int$main main void'
         ;; if searched with 'int main void' we look for 'int$main void'
         ;; but this line wont be findable with 'int' only
         ;; which greatly improves this case
         (.local-state-set ctx line-key uniq-tokens-seen-on-this-line)

         (if (= uniq-tokens-seen-on-this-line (.token_count_mask ctx))
           (do
             (.current-counter-set ctx 1)
             (.current-score-add ctx #{ALL_TOKENS_MATCH_SCORE})
             (when (.explanation ctx)
               (.explanation-add ctx #{ALL_TOKENS_MATCH_SCORE} (str "line: (" line-no ") match mask: " (.token_count_mask ctx)))
               (.result-state-append ctx line-no))
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
          field: ID_FIELD,
          value: @params[:id]
        }
      }
    end

    in_file_tokens = @q.scan(REQUEST_FILE_RE).map { |x| x.gsub(/^@/,"") }

    in_file_tokens.each do |token|
      queries << {
        term: {
          field: FILENAME_FIELD,
          value: token
        }
      }
    end

    query_string = @q.gsub(REQUEST_FILE_RE,"")
    queries << clojure_expression_terms(query_string)

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
          id: h[ID_FIELD],
          updated: Time.at(h[STAMP_FIELD].to_i),
          n_matches: 0,
        }

        state = h["_result_state"] || []
        matching = {}
        state.flatten.each do |line_no|
          matching[line_no] = true
        end

        highlighted = []
        around = 0
        colors = ["#3B4043","#666699"]
        max_line_no = 0

        h[SEARCH_FIELD].split(LINE_SPLITTER).each_with_index do |line,line_index|
          item = { show: false, bold: false, line_no: line_index, line: line.escape }
          max_line_no = line_index if max_line_no < line_index

          if matching[line_index]
            item[:bold] = true
            item[:show] = true
            item[:color] = colors[0]
            row[:n_matches] += 1
            row[:first_match] ||= line_index
            if @params[:id].empty? && item[:show]
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

        max_line_digits = max_line_no.to_s.length

        if @params[:id].empty?
          row[:highlight] = highlighted.select { |x| x[:show] }.map { |x| bold_and_color(x,max_line_digits,"?q=#{@q.escapeCGI}&id=#{row[:id]}") }.join("\n")
        else
          row[:highlight] = highlighted.map { |x| bold_and_color(x,max_line_digits) }.join("\n")
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
              matching lines: #{r[:n_matches]}, updated #{r[:updated]}

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
        %ul <b>case sensitive</b> indexed the following repositories (date of pull / name / indexed sha):
        =preserve do
          <pre>#{dump_git_status}</pre>
        some examples:
        %li
          %a{ href: "?q=struct+rtl8169_private"} struct rtl8169_private
        %li
          %a{ href: "?q=%40glibc+%40malloc+realloc"} @glibc @malloc realloc
        %li
          %a{ href: "?q=PayloadHelper+encodeFloat"} PayloadHelper encodeFloat
        %li
          %a{ href: "?q=IndexSearcher"} IndexSearcher
        %li
          %a{ href: "?q=postings+nextPosition"} postings nextPosition

      using <a href="https://github.com/jackdoe/bzzz">github.com/jackdoe/bzzz</a> lucene wrapper, __FILE__ lives at: <a href="https://github.com/jackdoe/bzzz/blob/master/example/app.rb">https://github.com/jackdoe/bzzz/blob/master/example/app.rb</a> <b>patches/issues welcome</b>
