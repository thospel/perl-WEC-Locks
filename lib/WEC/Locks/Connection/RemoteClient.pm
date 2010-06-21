package WEC::Locks::Connection::RemoteClient;
use 5.006;
use strict;
use warnings;
use Carp;

our $VERSIONl = '0.01';

use WEC::Locks::Constants qw(:response_types);

use base qw(WEC::Locks::Connection);

our @CARP_NOT	= qw(WEC::FieldConnection);

sub init_client {
    my __PACKAGE__ $connection = shift;

    $connection->{host_mpx}	= 0;
    $connection->{peer_mpx}	= 0;
    $connection->{requests}	= [];
    $connection->{in_want}	= 1;
    $connection->{in_process}	= $connection->can("want_line") ||
        die "$connection has no want_line";
    $connection->{in_state}	= \&greeting;
    $connection->{lines}	= [];
    $connection->{code}		= undef;
    $connection->{receive_timer} = WEC->add_alarm($connection->{options}{ReceivePeriod}, sub { too_silent($connection) });
    $connection->begin_handshake;
}

sub reset_receive_timer {
    my ($connection) = @_;
    WEC->delete_alarm($connection->{receive_timer});
    $connection->{receive_timer} = WEC->add_alarm($connection->{options}{ReceivePeriod}, sub { too_silent($connection) });
}

sub too_silent {
    my ($connection) = @_;
    my $peer_id = $connection->{peer_id} || "(unidentified)";
    $connection->close;
}

sub greeting {
    my ($connection, $line) = @_;
    $connection->reset_receive_timer;
    my ($software, $version, $id, $server_challenge) =
	$line =~ /^220\s+RemoteLockServer\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)/ or
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
    $connection->reset_receive_timer;
    my ($cresponse, $instance_id, $pending_from, $pending_to) =
	$line =~ /^250\s+(\S+)\s+(\S+)\s+0*(\w+)\s+0*(\w+)(?:\s|\z)/ or
	die "Unexpected greeting response '$line'";
    $connection->{in_state} = \&server_chatter;
    $connection->{send_timer} = WEC->add_alarm($connection->{options}{SendPeriod}, sub { send_probe($connection) });
    $connection->{options}{Sync}->
	($connection, $instance_id, $pending_from, $pending_to);
}

sub server_chatter {
    my ($connection, $line) = @_;
    $connection->reset_receive_timer;
    $line =~ s/^\s*(\d+)(?:\s+|\z)// || die "Server chatter '$line' unknown";
    return if $1 == 101;
    if ($1 == 251) {
        # Synced
	my ($instance_id) = $line =~ /^(\S+)(?:\s|\z)/ or
	    die "Unexpected 251 parameters '$line'";
	$connection->end_handshake($instance_id);
        return;
    }
    if ($1 == 252) {
	my ($req_id) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Unexpected 252 parameters '$line'";
	$connection->{options}{LockResponse}->
	    ($connection, RESPONSE_LOCKED, $req_id);
	return;
    }
    if ($1 == 253) {
	my ($req_id, $seq_id) = $line =~ /^0*(\w+)\s+0*(\d+)(?:\s|\z)/ or
	    die "Unexpected 253 parameters '$line'";
	$connection->{options}{LockResponse}->
	    ($connection, RESPONSE_QUEUED, $req_id, $seq_id);
	return;
    }
    if ($1 == 254) {
	my ($req_id) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Unexpected 254 parameters '$line'";
	$connection->{options}{LockResponse}->
	    ($connection, RESPONSE_UNLOCKED, $req_id);
	return;
    }
    if ($1 == 255) {
	my ($req_id) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Unexpected 255 parameters '$line'";
	$connection->{options}{LockResponse}->
	    ($connection, RESPONSE_NOT_LOCKED, $req_id);
	return;
    }
    if ($1 == 451) {
	my ($req_id) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Unexpected 451 parameters '$line'";
	$connection->{options}{LockResponse}->
	    ($connection, RESPONSE_LOCK_DENIED, $req_id);
	return;
    }
    if ($1 == 452) {
	my ($req_id) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Unexpected 452 parameters '$line'";
	$connection->{options}{LockResponse}->
	    ($connection, RESPONSE_UNLOCK_DENIED, $req_id);
	return;
    }
    if ($1 == 151) {
	my ($seq_id) = $line =~ /^0*(\d+)(?:\s|\z)/ or
	    die "Unexpected 151 parameters '$line'";
	$connection->{options}{LockAcquired}->($connection, $seq_id);
	return;
    }
    if ($1 == 256) {
	my ($req_id) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Unexpected 256 parameters '$line'";
	$connection->{options}{Dropped}->($connection, $req_id);
	return;
    }
    if ($1 == 122) {
	# We got probed. Ignore.
	return;
    }
    die "Server chatter '$1 $line' unknown";
}

sub drop_timers {
    my ($connection) = @_;
    WEC->delete_alarm(delete $connection->{send_timer}) if
	$connection->{send_timer};
    WEC->delete_alarm(delete $connection->{receive_timer});
}

sub send_probe {
    my ($connection) = @_;
    $connection->SUPER::send("PROBE\n") if $connection->{out_buffer} eq "";
    $connection->{send_timer} = WEC->add_alarm($connection->{options}{SendPeriod}, sub { send_probe($connection) });
}

sub send {
    my ($connection, $command) = @_;
    if ($connection->{send_timer}) {
	WEC->delete_alarm($connection->{send_timer});
	$connection->{send_timer} = WEC->add_alarm($connection->{options}{SendPeriod}, sub { send_probe($connection) });
    }
    print STDERR "Connection sends '$command'\n";
    $connection->SUPER::send("$command\n");
}

1;
