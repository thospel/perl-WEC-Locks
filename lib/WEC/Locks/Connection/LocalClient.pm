package WEC::Locks::Connection::LocalClient;
use 5.006;
use strict;
use warnings;
use Carp;

use WEC::Locks::Constants qw(INFINITY);
use WEC::Connection qw(CALLBACK PARENT);

our $VERSION = '1.000';

use base qw(WEC::Locks::Connection);

our @CARP_NOT = qw(WEC::FieldConnection);

use constant {
    # PARENT = 3 and last
    A_REALM => 4,
    A_NAME  => 5,
    A_FROM  => 6,
    A_TO    => 7,
};

sub init_client {
    my __PACKAGE__ $connection = shift;

    $connection->{host_mpx}	= 0;
    $connection->{peer_mpx}	= 0;
    $connection->{requests}	= [];
    $connection->{in_want}	= 1;
    $connection->{in_process}	= $connection->can("want_line") ||
        die "$connection has no want_line";
    $connection->{in_state}	= \&greeting;
    $connection->begin_handshake;
}

sub greeting {
    my ($connection, $line) = @_;
    my ($software, $version, $id, $server_challenge) =
	$line =~ /^220\s+LocalLockServer\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)/ or
	die "Unexpected greeting '$line'";
    $connection->{peer_software} = $software;
    $connection->{peer_version}  = $version;
    $connection->{peer_id}       = $id;
    my $options = $connection->options;
    my $client_challenge = $connection->challenge;
    $connection->send("HELO @$options{qw(ClientSoftware ClientVersion ClientId)} $client_challenge ====");
    $connection->{in_state} = \&client_identity;
}

sub client_identity {
    my ($connection, $line) = @_;
    my ($cresponse) = $line =~ /^250\s+(\S+)/ or
	die "Unexpected greeting response '$line'";
    $connection->{in_state} = \&server_chatter;
    $connection->end_handshake;
}

sub server_chatter {
    my ($connection, $line) = @_;
    $line =~ s/^\s*(\d+)(?:\s+|\z)// || die "Server chatter '$line' unknown";
    if ($1 == 252) {
	$connection->_callback("locked");
	return;
    }
    if ($1 == 253) {
	my ($seq_id) = $line =~ /(\d+)(?:\s+|\z)/ or
	    die "Could not parse 253 response";
	$connection->_callback("queued", $seq_id);
	return;
    }
    if ($1 == 254) {
	$connection->_callback("unlocked");
	return;
    }
    if ($1 == 255) {
	$connection->_callback("not_lock");
	return;
    }
    if ($1 == 451) {
	$connection->_callback("-locked");
	return;
    }
    if ($1 == 452) {
	$connection->_callback("-unlocked");
	return;
    }
    if ($1 == 151) {
	my ($seq_id) = $line =~ /(\d+)(?:\s+|\z)/ or
	    die "Could not parse 251 response '$line'";
	$connection->{options}{LockAcquired}->($connection, $seq_id);
	return;
    }
    if ($1 == 221) {
	$connection->expect_eof(1);
	$connection->{options}{Quit}->($connection) if $connection->{options}{Quit};
	return;
    }
    if ($1 == 121) {
	$connection->{options}{Terminate}->($connection) if $connection->{options}{Terminate};
	return;
    }
    die "Unknown code $1";
}

sub _callback {
    my $connection = shift;
    my $answer = shift @{$connection->{answers}} ||
        croak "No callback pending";
    if ($answer->[PARENT]) {
        my $parent = $answer->[PARENT];
        delete $parent->{events}{$answer};
        ($answer->[CALLBACK] || return)->($parent, $connection, shift, $answer, @_);
        return;
    }
    ($answer->[CALLBACK] || return)->($connection, shift, $answer, @_);
}

sub quit {
    my $connection = shift;
    $connection->send("QUIT");
}

sub try_lock {
    my $connection = shift;
    $connection->send_lock("TRY_LOCK", @_);
}

sub lock : method {
    my $connection = shift;
    $connection->send_lock("LOCK", @_);
}

sub query_lock {
    my $connection = shift;
    $connection->send_lock("QUERY_LOCK", @_);
}

sub unlock : method {
    my $connection = shift;
    $connection->send_lock("UNLOCK", @_);
}

sub try_alock {
    my $connection = shift;
    $connection->send_lock("TRY_ALOCK", @_);
}

sub alock : method {
    my $connection = shift;
    $connection->send_lock("ALOCK", @_);
}

sub query_alock {
    my $connection = shift;
    $connection->send_lock("QUERY_ALOCK", @_);
}

sub unalock : method {
    my $connection = shift;
    $connection->send_lock("UNALOCK", @_);
}

sub send_lock {
    my ($connection, $command, %options) = @_;

    my $filename = delete $options{filename};
    croak "No filename" if !defined $filename || $filename eq "";
    my $realm = delete $options{realm};
    $realm = "" if !defined $realm;
    my $from_range = delete $options{from};
    $from_range =
	!defined $from_range ? 0 :
	$from_range =~ /^\s*(\d+)\s*\z/ ? $1+0 :
	$from_range == INFINITY ||
	$from_range =~ /^\s*(?:inf|infinity)\s*\z/i ? INFINITY :
	croak "Could not parse from_range '$from_range' as a number";
    my $to_range = delete $options{to};
    $to_range =
	!defined $to_range || $to_range == INFINITY ||
	$to_range =~ /^\s*(?:inf|infinity)\s*\z/i ? INFINITY :
	$to_range =~ /^\s*(\d+)\s*\z/ ? $1+0 :
	croak "Could not parse to_range '$to_range' as a number";
    my $parent   = delete $options{parent};
    my $callback = delete $options{callback};
    die "Unknown option ", join(", ", keys %options) if %options;

    my $answer = [$command, $callback, "", $parent, $realm, $filename, $from_range, $to_range];

    $realm =~ s!([^a-zA-Z0-9/=~+_\.:-])!sprintf("%%%x%%", ord $1)!eg;
    $filename =~ s!^/+!! || croak "Filename is not absolute";
    $filename =~ s!/+\z!!;	# Directory names are passed without the /
    $filename =~ s!(//+|[^a-zA-Z0-9/=~+_\.:-])!
	length $1 > 1 ? "/" : sprintf("%%%x%%", ord $1)!eg;
    $command .= " $realm|/$filename";
    if ($from_range || $to_range != INFINITY) {
	croak "range [$from_range, $to_range] is invalid" if
	    $from_range > $to_range || $from_range == INFINITY;
	$command .= " $from_range";
	$command .= " $to_range" if $to_range != INFINITY;
    }

    croak "try_lock currently not supported during handshake" if
	$connection->{handshaking};
    $connection->send($command);
    push @{$connection->{answers}}, $answer;
}

sub send {
    my ($connection, $command) = @_;
    print STDERR "Connection sends '$command'\n";
    $connection->SUPER::send("$command\n");
}

1;
