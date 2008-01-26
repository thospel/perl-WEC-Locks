package WEC::Locks::RemoteTransit;
use 5.006;
use strict;
use warnings;
use Carp;

use WEC::Locks::Constants qw(INFINITY REQUEST_START LOCAL_QUIT QUIT DROP
			     :lock_types);

our $VERSION = '0.01';

use constant {
    # indices on a transit queue element
    CODE	=> 0,
    REPLY	=> 1,
    REQUEST_ID	=> 2,
    CONNECTION	=> 3,
    NAME	=> 4,
    FROM	=> 5,
    TO		=> 6,
    ATOMIC	=> 7,
};

my @code_to_remote_command =
    ({LOCK_IMMEDIATE()	=> "LOCK",
      LOCK_DELAYED()	=> "TRY_LOCK",
      LOCK_QUERY()	=> "QUERY_LOCK",
      LOCK_DROP()	=> "UNLOCK"},
     {LOCK_IMMEDIATE()	=> "ALOCK",
      LOCK_DELAYED()	=> "TRY_ALOCK",
      LOCK_QUERY()	=> "QUERY_ALOCK",
      LOCK_DROP()	=> "UNALOCK"});

sub new {
    my ($class) = @_;
    return bless {
	request_id => REQUEST_START,
	# Invariant: ![REPLY] => [REQUEST_ID]
	queue	=> [],
	sent	=> 0,
	replied	=> 0,
	client	=> undef,
    }, $class;
}

# Transit/command elements:
#  has T_REPLY: prepared fixed answer to the client
#      T_REQUEST_ID == 0: doesn't correspond to a remote command
#      otherwise: drop T_REPLY, negate REQUEST_ID (wait for remote answer)
#  no T_REPLY: prepared fixed answer to the client
sub flush {
    my ($transit) = @_;
    while (my $current = $transit->{queue}[$transit->{sent}]) {
	unless ($current->[REQUEST_ID]) {
	    $transit->{sent}++;
	    next;
	}
	my $client = $transit->{client} || last;

	my $code = $current->[CODE];
	my $command;
	if ($code == LOCK_DROP  || $code == LOCK_IMMEDIATE ||
	    $code == LOCK_QUERY || $code == LOCK_DELAYED) {
	    $command = $code_to_remote_command[$current->[ATOMIC]]{$code} ||
		croak "Unknown lock_request code '$code' (atomic $current->[ATOMIC])";
	    $command .= " $current->[REQUEST_ID] $current->[CONNECTION]->{peer_id} $current->[NAME]";
	    if ($current->[FROM] || $current->[TO] != INFINITY) {
		$command .= " $current->[FROM]";
		$command .= " $current->[TO]" if $current->[TO] != INFINITY;
	    }
	} elsif ($code == DROP) {
	    $command = "DROP $current->[REQUEST_ID] $current->[CONNECTION]{peer_id}";
	} else {
	    croak "Assertion: Unknown code '$code'";
	}
	$client->send($command);
	$transit->{sent}++;
	# $client->close if rand(6) < 1;
    }

    while (my $current = $transit->{queue}[$transit->{replied}]) {
	my $reply = $current->[REPLY] || last;
	my $connection = $current->[CONNECTION];
	$connection->send($reply);
	$transit->{replied}++;
	$connection->close_on_empty if $current->[CODE] == LOCAL_QUIT;
    }

    while (my $current = $transit->{queue}[0]) {
	last if $current->[REQUEST_ID];
	shift @{$transit->{queue}};
	$transit->{replied}--;
	$transit->{sent}--;
    }
}

my %modes =
    (""		=> 1,
     reply	=> 2,
     pre_reply	=> 3);

sub send : method {
    my $transit = shift;
    my $code    = shift;
    if ($code == LOCK_DROP  || $code == LOCK_IMMEDIATE ||
	$code == LOCK_QUERY || $code == LOCK_DELAYED) {
	@_ == 5 || @_ == 7 || croak "Wrong number of arguments";
	my ($connection, $name, $from, $to, $atomic, $mode, $reply) = @_;
	$mode ||= "";
	$mode = $modes{lc $mode} || croak "Unknown mode '$mode'";
	push(@{$transit->{queue}},
	     [$code, $reply, $mode == 2 ? 0 : $transit->{request_id}++,
	      $connection, $name, $from, $to, $atomic]);
    } elsif ($code == DROP) {
	my ($connection) = @_;
	push(@{$transit->{queue}},
	     [DROP, undef, $transit->{request_id}++, $connection]);
    } elsif ($code == LOCAL_QUIT) {
	my ($connection, $reply) = @_;
	push(@{$transit->{queue}}, [LOCAL_QUIT, $reply, undef, $connection]);
    } else {
	croak "Unknown code '$code'";
    }
    $transit->flush;
}

sub direct_send {
    my ($transit, $command) = @_;
    croak "queue is not empty" if @{shift->{queue}};
    croak "No active remote handle" if !$transit->{client};
    $transit->{client}->send($command);
}

# Caller is responsible to do a $transit->flush after answering to local client
sub shift_lock {
    my ($transit, $req_id) = @_;
    $transit->{replied}-- if $transit->{replied};
    my $first = shift @{$transit->{queue}} || croak "Answer without command";
    my $rid = $first->[REQUEST_ID] || croak "No request id";
    $rid == $req_id || croak "Expected a response to $rid, got $req_id";
    # Must have been sent, otherwise how can we have an answer ?
    $transit->{sent}--;
    $transit->{client}->send("ACK $req_id") if $transit->{client};
    my $code = $first->[CODE];
    croak "Expected an answer to '$code'" unless
	$code == LOCK_DROP  || $code == LOCK_IMMEDIATE ||
	$code == LOCK_QUERY || $code == LOCK_DELAYED;

    return @$first[CONNECTION, NAME, FROM, TO, ATOMIC, REPLY];
}

# Caller is responsible to do a $transit->flush after answering to local client
sub shift_drop {
    my ($transit, $req_id) = @_;

    $transit->{replied}-- if $transit->{replied};
    my $first = shift @{$transit->{queue}} || croak "Answer without command";
    my $rid = $first->[REQUEST_ID] || croak "No request id";
    $rid == $req_id || croak "Expected a response to $rid, got $req_id";
    # Must have been sent, otherwise how can we have an answer ?
    $transit->{sent}--;
    $transit->{client}->send("ACK $req_id") if $transit->{client};
    $first->[CODE] == DROP || croak "Expected an answer to '$first->[CODE]'";

    return @$first[CONNECTION, REPLY];
}

sub set_client {
    my ($transit, $client) = @_;
    if ($client) {
	croak "Already have a client" if $transit->{client};
	$transit->{client} = $client;
	$transit->flush;
    } else {
	$transit->{client} = undef;
    }
}

sub resend {
    my ($transit, $from) = @_;
    if ($from eq $transit->{request_id}) {
	$transit->{sent} = @{$transit->{queue}};
	return;
    }
    my $sent = 0;
    for my $current (@{$transit->{queue}}) {
	if ($current->[REQUEST_ID] eq $from) {
	    $transit->{sent} = $sent;
	    return;
	}
	$sent++;
    }
    croak "Could not find queue element with request $from";
}

sub next_request_id {
    return shift->{request_id};
}

sub first_request_id {
    my $first = shift->{queue}[0] || return;
    return $first->[REQUEST_ID];
}

sub is_empty {
    return !@{shift->{queue}};
}

sub client {
    return shift->{client};
}

1;
