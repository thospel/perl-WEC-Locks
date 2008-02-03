package WEC::Locks::Utils;
use 5.006;
use strict;
use warnings;
use Carp;

our $VERSION = '1.000';

use Exporter::Tidy
    other   => [qw(check_options)];

sub check_options {
    my $options = shift;
    for my $name (@_) {
	croak "$name is undef" if !defined $options->{$name};
	next if $options->{$name} =~ /^[^\s\xa0\0\x80-\xff]+\z/;
	croak "$name is empty" if $options->{$name} eq "";
	croak "$name contains \\0" if $options->{$name} =~ /\0/;
	croak "$name '$options->{$name}' contains \\xa0 (non-breaking space)"
	    if $options->{$name} =~ /\xa0/;
	# We should support UTF8 instead....
	croak "$name '$options->{$name}' contains high ASCII" if
	    $options->{$name} =~ /[\x80-\xff]/;
	croak "$name '' contains spaces" if $options->{name} =~ /\s/;
	croak "Assertion: $name '$options->{$name}' is invalid but I have no clue why";
    }
}

