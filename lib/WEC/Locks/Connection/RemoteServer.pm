package WEC::Locks::Connection::RemoteServer;
use 5.006;
use strict;
use warnings;
use Carp;

use WEC::Locks::Constants qw(:lock_types :response_types 
			     INFINITY REQUEST_START);

our $VERSION = '0.01';

use base qw(WEC::Locks::Connection);

our @CARP_NOT = qw(WEC::FieldConnection);

my (%pending_replies, %expected_request_id);

sub init_server {
    my __PACKAGE__ $connection = shift;

    $connection->{host_mpx}	= 0;
    $connection->{peer_mpx}	= 0;

    $connection->{requests}	= [];
    $connection->{in_want}	= 0;
    $connection->{in_process}	= \&send_greeting;
    $connection->{receive_timer} = WEC->add_alarm($connection->{options}{ReceivePeriod}, sub { too_silent($connection) });
}

sub reset_receive_timer {
    my ($connection) = @_;
    WEC->delete_alarm($connection->{receive_timer});
    $connection->{receive_timer} = WEC->add_alarm($connection->{options}{ReceivePeriod}, sub { too_silent($connection) });
}

sub too_silent {
    my ($connection) = @_;
    my $peer_id = $connection->{peer_id} || "(unidentified)";
    print STDERR "Connection to $peer_id has been silent for too long\n";
    $connection->close;
}

sub send_greeting {
    my __PACKAGE__ $connection = shift;
    $connection->reset_receive_timer;
    my $options = $connection->options;
    my $server_challenge = $connection->challenge;
    $connection->send("220 RemoteLockServer @$options{qw(ServerSoftware ServerVersion ServerId)} $server_challenge OK");
    $connection->{in_process}	= $connection->can("want_line") ||
        die "$connection has no want_line";
    $connection->{in_state} = \&client_identity;
}

sub client_identity {
    my ($connection, $line) = @_;
    $connection->reset_receive_timer;
    my ($software, $version, $client_id, $challenge, $cresponse) =
        $line =~ /^HELO\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)/i or
	croak "Could not parse client identity '$line'";
    $connection->{peer_software} = $software;
    $connection->{peer_version}  = $version;
    $connection->{peer_id}	 = $client_id;
    if (my $message = $connection->{options}{AllowConnection}->($connection)) {
	$connection->send("458 $message");
	return;
    }
    
    $expected_request_id{$client_id} ||= REQUEST_START;
    my $from;
    if (my $replies = $pending_replies{$client_id}) {
	if (@$replies) {
	    ($from) = $replies->[0] =~ /^\d+\s+0*(\w+)(?:\s|\z)/ or
		croak "Could not parse first pending reply $replies->[0]";
	} else {
	    $from = $expected_request_id{$client_id};
	}
    } else {
	$from = "inf";
    }
    $connection->send("250 ==== $connection->{options}{InstanceId} $from $expected_request_id{$client_id} Hello $client_id, pleased to meet you");
    $connection->{send_timer} = WEC->add_alarm($connection->{options}{SendPeriod}, sub { send_probe($connection) });
    $connection->{in_state} = \&resync;
}

sub reset_replies {
    my ($connection) = @_;
    my $remote_id = $connection->{peer_id};
    $pending_replies{$remote_id}     = [];
    $expected_request_id{$remote_id} = REQUEST_START;
}

sub resend_replies {
    my ($connection, $resend_from) = @_;
    my $remote_id = $connection->{peer_id};
    if ($resend_from eq $expected_request_id{$remote_id}) {
	$pending_replies{$remote_id} = [];
    } else {
	my $pos = 0;
	my $match = qr/^\d+\s+\Q$resend_from\E(?:\s|\z)/;
	for my $pending (@{$pending_replies{$remote_id}}) {
	    last if $pending =~ $match;
	    $pos++;
	}
	die "Could not find request_id $resend_from" if 
	    $pos == @{$pending_replies{$remote_id}};
	splice(@{$pending_replies{$remote_id}}, 0, $pos);
    }
    $pending_replies{$remote_id} ||= [];
    $connection->send($_) for @{$pending_replies{$remote_id}};
}

sub resync {
    my ($connection, $line) = @_;
    $connection->reset_receive_timer;
    
    if ($line =~ /^NEW(?:\s|\z)/i) {
        $connection->send("101 WAIT");
	$connection->{in_state} = \&client_request;
	$connection->suspend_input;
	$connection->options->{New}->($connection);
    } elsif ($line =~ s/^REPLACE(?:\s+|\z)//i) {
	$connection->{in_state} = \&replace;
	$connection->{new_have_locks} = [];
	$connection->{new_want_locks} = [];
    } elsif ($line =~ s/^SYNCED(?:\s+|\z)//i) {
	my ($resend_from) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Syntax error in SYNCED $line";

	# Check if possible, but don't do it yet
	my $remote_id = $connection->{peer_id};
	if ($resend_from ne $expected_request_id{$remote_id}) {
	    my $pos = 0;
	    my $match = qr/^\d+\s+\Q$resend_from\E(?:\s|\z)/;
	    for my $pending (@{$pending_replies{$remote_id}}) {
		last if $pending =~ $match;
		$pos++;
	    }
	    die "Could not find request_id $resend_from" if 
		$pos == @{$pending_replies{$remote_id}};
	}

        $connection->send("101 WAIT");
	$connection->{in_state} = \&client_request;
	$connection->suspend_input;
	$connection->options->{Sync}->($connection, $resend_from);
    } elsif ($line =~ /^PROBE(?:\s|\z)/i) {
	# We got probed. Ignore.
    } else {
	die "Client sync command '$line' unknown";
    }
}

sub replace {
    my ($connection, $line) = @_;
    $connection->reset_receive_timer;

    if ($line =~ s/^(RELOCK|REALOCK)(?:\s+|\z)//i) {
	my $atomic = uc $1 eq "REALOCK" ? 1 : 0;
	my ($peer_id, $name, $from, $to) =
	    $line =~ m!^(\S+)\s+([a-zA-Z0-9/=~+_\.:%-]*\|[a-zA-Z0-9/=~+_\.:%-]*)\s*(?:\s(\d+)\s*(?:\s(\d+)\s*)?)?\z! or die "Syntax error in $line";
	$from  = 0	  if !defined $from;
	$to    = INFINITY if !defined $to;
	die "Range [$from, $to] is invalid" if $from > $to;
	push(@{$connection->{new_have_locks}},
	     [$peer_id, $atomic, $name, $from, $to]);
    } elsif ($line =~ s/^(RETRYLOCK|RETRYALOCK)(?:\s+|\z)//i) {
	my $atomic = uc $1 eq "RETRYALOCK" ? 1 : 0;
	my ($seq_id, $peer_id, $name, $from, $to) =
	    $line =~ m!^0*(\w+)\s+(\S+)\s+([a-zA-Z0-9/=~+_\.:%-]*\|[a-zA-Z0-9/=~+_\.:%-]*)\s*(?:\s(\d+)\s*(?:\s(\d+)\s*)?)?\z! or die "Syntax error in $line";
	$from  = 0	  if !defined $from;
	$to    = INFINITY if !defined $to;
	die "Range [$from, $to] is invalid" if $from > $to;
	push(@{$connection->{new_want_locks}},
	     [$peer_id, $atomic, $name, $from, $to, $seq_id]);
    } elsif ($line =~ s/^SYNCED(?:\s+|\z)//i) {
	my ($start_from) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    die "Syntax error in SYNCED $line";
	$connection->{in_state} = \&client_request;
	$connection->suspend_input;
        $connection->send("101 WAIT");
	$connection->{options}{Replace}->(delete $connection->{new_have_locks},
					  delete $connection->{new_want_locks},
					  $start_from);
    } elsif ($line =~ /^PROBE(?:\s|\z)/i) {
	# We got probed. Ignore.
    } else {
	die "Client sync command '$line' unknown";
    }
}

my %remote_command_to_code =
    (LOCK	=> LOCK_IMMEDIATE,
     TRY_LOCK	=> LOCK_DELAYED,
     QUERY_LOCK	=> LOCK_QUERY,
     UNLOCK	=> LOCK_DROP,
     ALOCK	=> LOCK_IMMEDIATE,
     TRY_ALOCK	=> LOCK_DELAYED,
     QUERY_ALOCK=> LOCK_QUERY,
     UNALOCK	=> LOCK_DROP,
);

my %remote_command_to_atomic =
    map {$_ => 1} qw(ALOCK TRY_ALOCK QUERY_ALOCK UNALOCK);

my %replies =
    (RESPONSE_UNLOCKED()	=> "254 req_id LOCK DROPPED",
     RESPONSE_UNLOCK_DENIED()	=> "452 req_id WASN'T LOCKED",
     RESPONSE_NOT_LOCKED()	=> "255 req_id LOCKABLE",
     RESPONSE_LOCKED()		=> "252 req_id GOT LOCK",
     RESPONSE_QUEUED()		=> "253 req_id seq_id QUEUED",
     RESPONSE_LOCK_DENIED()	=> "451 req_id ALREADY LOCKED",
     RESPONSE_DROPPED()		=> "256 req_id DROPPED",
     );

sub reply {
    my ($connection, $code, $seq_id) = @_;
    my $reply = $replies{$code+0} || croak "Unknown code '$code'";
    my $remote_id = $connection->{peer_id};
    my $req_id = $expected_request_id{$remote_id}++;
    $reply =~ s/seq_id/$seq_id/;
    $reply =~ s/req_id/$req_id/;
    push @{$pending_replies{$connection->{peer_id}}}, $reply;
    $connection->send($reply);
    # $connection->close if rand(6) < 1;
}

sub client_request {
    my ($connection, $line) = @_;
    $connection->reset_receive_timer;
    if ($line =~ s/^ACK(?:\s+|\z)//i) {
	my $pending = $pending_replies{$connection->{peer_id}};
	my $reply = $pending->[0] || croak "ACK without anything pending";
	my ($req_id) = $line =~ /^0*(\w+)(?:\s|\z)/ or
	    croak "Syntax error in ACK $line";
	$reply =~ /^\d+\s+$req_id(?:\s|\z)/ || 
	    croak "Expected ack to $reply";
	shift @$pending;
    } elsif ($line =~ s/^(LOCK|TRY_LOCK|QUERY_LOCK|UNLOCK|ALOCK|TRY_ALOCK|QUERY_ALOCK|UNALOCK)(?:\s+|\z)//i) {
	my $type = $remote_command_to_code{uc $1} ||
	    die "Assert: Unknown lock type '$1'";
	my $atomic = $remote_command_to_atomic{uc $1};

	my ($request_id, $peer_id, $name, $from, $to) =
	    $line =~ m!^0*(\w+)\s+(\S+)\s+([a-zA-Z0-9/=~+_\.:%-]*\|[a-zA-Z0-9/=~+_\.:%-]*)\s*(?:\s(\d+)\s*(?:\s(\d+)\s*)?)?\z! or die "Syntax error in $line";
	$from  = 0	  if !defined $from;
	$to    = INFINITY if !defined $to;
	die "Range [$from, $to] is invalid" if $from > $to;
	my $remote_id = $connection->{peer_id};
	$expected_request_id{$remote_id} eq $request_id ||
	    croak "Unexpected request id $request_id (expected $expected_request_id{$remote_id})";
        $connection->options->{LockRequest}->($connection, $type, $atomic || 0, $peer_id, $name, $from, $to);
	die "No reply" if $expected_request_id{$remote_id} eq $request_id;
    } elsif ($line =~ s/^DROP(?:\s+|\z)//i) {
	my ($request_id, $peer_id) =
	    $line =~ m!^0*(\w+)\s+(\S+)\s*\z! or die "Syntax error in $line";
	my $remote_id = $connection->{peer_id};
	$expected_request_id{$remote_id} eq $request_id ||
	    croak "Unexpected request id $request_id (expected $expected_request_id{$remote_id})";
	$connection->options->{Drop}->($connection, $peer_id);
	die "No reply" if $expected_request_id{$remote_id} eq $request_id;
    } elsif ($line =~ /^QUIT(?:\s|\z)/i) {
	$connection->options->{Quit}->($connection);
	my $remote_id = $connection->{peer_id};
	delete $expected_request_id{$remote_id};
	delete $pending_replies{$remote_id};
	$connection->eat_input;
	$connection->close_on_empty;
    } elsif ($line =~ /^PROBE(?:\s|\z)/i) {
	# We got probed. Ignore.
    } else {
	die "Client request '$line' unknown";
    }
}

sub drop_timers {
    my ($connection) = @_;
    WEC->delete_alarm(delete $connection->{send_timer}) if 
	$connection->{send_timer};
    WEC->delete_alarm(delete $connection->{receive_timer});
}

sub send_probe {
    my ($connection) = @_;
    $connection->SUPER::send("122 PROBED\n") if 
	$connection->{out_buffer} eq "";
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
