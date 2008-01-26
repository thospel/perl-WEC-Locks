package WEC::Locks::Connection;
use 5.006;
use strict;
use warnings;
use Carp;

use Digest::SHA qw(sha256_base64);

use base qw(WEC::Connection);
our @CARP_NOT = qw(WEC::FieldConnection);

sub want_line {
    # Probably should check for line getting too long here
    my __PACKAGE__ $connection = shift;
    my $pos = index $_, "\n", $connection->{in_want}-1;
    if ($pos < 0) {
	$connection->{in_want} = 1+length;
	return;
    }
    $connection->{in_want} = 1;
    my $line = substr($_, 0, $pos+1, "");
    $line =~ s/(\r?\n)\z// || croak "Assertion: Incomplete line";
    my $eol = $1;
print STDERR "Connection $connection GOT '$line'\n";
    $connection->{in_state}->($connection, $line, $eol);
}

sub want_response {
    # Probably should check for line getting too long here
    my __PACKAGE__ $connection = shift;
    my $pos = index $_, "\n", $connection->{in_want}-1;
    if ($pos < 0) {
	$connection->{in_want} = 1+length;
	return;
    }
    $connection->{in_want} = 1;
    my $line = substr($_, 0, $pos+1, "");
    $line =~ s/([^\S\n]*\n)\z// or croak "Assertion: Incomplete line";
    $line =~ s/^(\d+)(-|)\s*// ||
	croak "Line does not start with a number";
    if (!defined $connection->{code}) {
	$connection->{code} = $1+0;
    } elsif ($connection->{code} != $1) {
	croak "Number $1 while expecting $connection->{code}";
    }
    push @{$connection->{lines}}, $line;
    return if $2;	# Continuation line
    my $lines = $connection->{lines};
    my $code = $connection->{code};
    $connection->{lines} = [];
    $connection->{code} = undef;
    $connection->{in_state}->($connection, $code, $lines);
}

my $last_challenge = rand;
challenge();

sub challenge {
    my ($connection) = @_;
    my $result = sha256_base64($last_challenge);
    $last_challenge ^= rand() . time() . "$<@_$result$$";
    croak "Already a pending challenge" if defined $connection->{challenge};
    return $connection->{challenge} = $result;
}

1;
