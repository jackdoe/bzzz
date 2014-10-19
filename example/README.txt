required:
    sinatra
    curb
    haml
    some .c files in /usr/src/linux

for the first time run:
  $ ruby app.rb do-index # this will index /usr/src/linux
after that run:
  $ ruby app.rb

open:
  http://localhost:4567/?q=void+foo

screenshot:
  https://raw.githubusercontent.com/jackdoe/bzzz/master/example/screenshot.png
