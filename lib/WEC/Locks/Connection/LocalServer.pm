package WEC::Locks::Connection::LocalServer;
use 5.006;
use strict;
use warnings;
use Carp;

use WEC::Locks::Constants qw(:lock_types INFINITY);

our $VERSION = '0.01';

use base qw(WEC::Locks::Connection);

our @CARP_NOT = qw(WEC::FieldConnection);

sub init_server {
    my __PACKAGE__ $connection = shift;

    $connection->{host_mpx}	= 0;
    $connection->{peer_mpx}	= 0;

    $connection->{requests}	= [];
    $connection->{in_want}	= 0;
    $connection->{in_process}	= \&send_greeting;
}

sub send_greeting {
    my __PACKAGE__ $connection = shift;
    my $server_challenge = $connection->challenge;
    my $options = $connection->{options};
    $connection->send("220 LocalLockServer @$options{qw(ServerSoftware ServerVersion ServerId)} $server_challenge OK");
    $connection->{in_process}	= $connection->can("want_line") ||
        die "$connection has no want_line";
    $connection->{in_state} = \&client_identity;
}

sub client_identity {
    my ($connection, $line, $eol) = @_;
    my ($software, $version, $client_id, $challenge, $cresponse) =
        $line =~ /^HELO\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)/i or
	croak "Could not parse client identity '$line'";
    $connection->{peer_software} = $software;
    $connection->{peer_version}  = $version;
    # Setting peer_id indicates we have started
    $connection->{peer_id}       = $client_id;
    $connection->{presented_id}  = $client_id;
    if (my $message = $connection->{options}{AllowConnection}->($connection)) {
	$connection->send("458 $message");
	return;
    }
    $connection->send("250 ==== Hello $connection->{presented_id}, pleased to meet you");
    $connection->{in_state} = \&client_request;
}

my %local_command_to_code =
    (LOCK	=> LOCK_IMMEDIATE,
     TRY_LOCK	=> LOCK_DELAYED,
     QUERY_LOCK	=> LOCK_QUERY,
     UNLOCK	=> LOCK_DROP,
     ALOCK	=> LOCK_IMMEDIATE,
     TRY_ALOCK	=> LOCK_DELAYED,
     QUERY_ALOCK=> LOCK_QUERY,
     UNALOCK	=> LOCK_DROP,
);

my %local_command_to_atomic = 
    map {$_ => 1} qw(ALOCK TRY_ALOCK QUERY_ALOCK UNALOCK);

sub client_request {
    my ($connection, $line, $eol) = @_;
    if ($line =~ s/^(LOCK|TRY_LOCK|QUERY_LOCK|UNLOCK|ALOCK|TRY_ALOCK|QUERY_ALOCK|UNALOCK)(?:\s+|\z)//i) {
	my $type = $local_command_to_code{uc $1} ||
	    die "Assert: Unknown lock type '$1'";
	my $atomic = $local_command_to_atomic{uc $1};

	# Maybe we should exclude %0000% --Ton
	my ($name, $from, $to) =
	    $line =~ m!^([a-zA-Z0-9/=~+_\.:%-]*\|[a-zA-Z0-9/=~+_\.:%-]*)\s*(?:\s(\d+)\s*(?:\s(\d+)\s*)?)?\z! or die "Syntax error in $1";
	$from  = 0	  if !defined $from;
	$to    = INFINITY if !defined $to;
	die "Range [$from, $to] is invalid" if $from > $to;
        $connection->options->{LockRequest}->($connection, $type, $atomic || 0, $name, $from, $to);
        return;
    }
    if ($line =~ /^QUIT(?:\s|\z)/i) {
	$connection->eat_input;
	$connection->expect_eof(1);
	$connection->options->{Quit}->($connection);
	return;
    }
    die "Client request '$line' unknown";
}

sub send {
    my ($connection, $command) = @_;
    print STDERR "Connection sends '$command'\n";
    $connection->SUPER::send("$command\n");
}

1;
