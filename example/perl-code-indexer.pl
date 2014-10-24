#!/usr/bin/env perl
use v5.14;
use strict;
use warnings;

use Hijk;
use PPI;
use PPIx::IndexOffsets;

use JSON;
my $JSON = JSON->new->canonical;

my $path = shift @ARGV;
my $document = PPI::Document->new($path);
$document->index_offsets;

my $doc = {
    content => $document->content,
    path    => $path,
    tokens  => ""
};

foreach my $token (  $document->tokens  ) {
    next unless $token->significant;

    my $start_offset = $token->start_offset;
    my $stop_offset  = $token->stop_offset;
    # print "$start_offset .. $stop_offset $token\n";
    $doc->{tokens} .= " $token";
}

my $res = Hijk::request({
    host => "localhost",
    port => "3000",
    method => "POST",
    body => $JSON->encode({
        index => "perl-code",
        documents => [$doc],
        analyzer => {
            tokens => {
                type => "whitespace"
            },
            filename => {
                type => "standard"
            },
            content => {
                type => "standard"
            }
        }
    })
});

say $res->{body};
