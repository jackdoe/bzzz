#!/usr/bin/ruby
require 'curb'
require 'json'

class Store
  @index = "example-index"
  @host = "http://localhost:3000"

  def Store.save(docs = [], analyzer = {})
    docs = [docs] if !docs.kind_of?(Array)
    JSON.parse(Curl.post(@host, {index: @index, documents: docs, analyzer: analyzer}.to_json).body_str)
  end

  def Store.delete(query)
    JSON.parse(Curl.http(:DELETE, @host, {index: @index, query: query}.to_json).body_str)
  end

  def Store.find(query, size: 10, page: 0, refresh: false)
    JSON.parse(Curl.http(:GET, @host, {index: @index,
                                       query: query,
                                       page: page,
                                       size: size,
                                       refresh: refresh}.to_json).body_str)
  end

  def Store.stat
    JSON.parse(Curl.get("#{@host}/_stat").body_str)
  end
end

p Store.save({name_index: "aaa"})
p Store.find({term: { field: "name_index", value: "aaa"}},refresh: true)
p Store.delete({term: { field: "name_index", value: "aaa"}})
p Store.stat
