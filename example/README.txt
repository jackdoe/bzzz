required:
    sinatra
    curb
    haml

for the first time run:
  $ ruby app.rb source-init # this will create ../../SOURCE-TO-INDEX and git clone linux,glibc .. etc
after that run:
  $ ruby app.rb

to update it, every once in a while do:
  $ ruby app.rb do-index # it will pull the cloned repositories in SOURCE-TO-INDEX and index the changed files
open:
  http://localhost:4567/?q=void+foo

  also this lives at http://zearch.io

screenshot:
  https://raw.githubusercontent.com/jackdoe/bzzz/master/example/screenshot.png
  https://raw.githubusercontent.com/jackdoe/bzzz/master/example/screenshot-explain.png
