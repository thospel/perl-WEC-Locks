package WEC::Locks::RemoteServer;
use 5.006;
use strict;
use warnings;
use Carp;
use Sys::Hostname;

my $host = hostname;
$host =~ s/\..*//s;

use WEC::Locks::Connection::RemoteServer;
use WEC::Locks::Constants qw(REMOTE_PORT);
use WEC::Locks::Utils qw(check_options);

our $VERSION = '1.000';
our @CARP_NOT	= qw(WEC::Server WEC::FieldServer WEC::Locks::Utils);

use base qw(WEC::Server);

my $default_options = {
    %{__PACKAGE__->SUPER::server_options},
    ServerSoftware	=> "WEC::Locks",
    ServerVersion	=> $WEC::Locks::Connection::RemoteServer::VERSION,
    ServerId		=> $host,
    InstanceId		=> $host . "-" . time() . "-" . $$,
    SendPeriod		=> 10,
    ReceivePeriod	=> 30,
    LockRequest		=> undef,
    AllowConnection	=> undef,
    Quit		=> undef,
    Drop		=> undef,
    New			=> undef,
    Sync		=> undef,
    Replace		=> undef,
};

sub default_options {
    return $default_options;
}

sub default_port {
    return REMOTE_PORT;
}

sub connection_class {
    return "WEC::Locks::Connection::RemoteServer";
}

sub init {
    my ($server, $params) = @_;
    my $options = $server->options;
    check_options($options, qw(ServerSoftware ServerVersion ServerId
			       InstanceId));
    $server->{locks}   = {};
    $server->{clients} = {};
}

sub try_lock {
    my ($server, $client_id, $pid, $name) = @_;
    return 0 if $server->{locks}{$name};
    $server->{locks}{$name} = [$pid, $client_id];
    $server->{clients}{$client_id}{$pid}{$name} = 1;
    return 1;
}

1;
