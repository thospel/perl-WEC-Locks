package WEC::Locks::LocalServer;
use 5.006;
use strict;
use warnings;
use Carp;
use Sys::Hostname;

my $host = hostname;
$host =~ s/\..*//s;

use WEC::Locks::Connection::LocalServer;
use WEC::Locks::Constants qw(LOCAL_PORT);
use WEC::Locks::Utils qw(check_options);

our $VERSION = '0.01';
our @CARP_NOT	= qw(WEC::Server WEC::FieldServer WEC::Locks::Utils);

use base qw(WEC::Server);

my $default_options = {
    %{__PACKAGE__->SUPER::server_options},
    ServerSoftware	=> "WEC::Locks",
    ServerVersion	=> $WEC::Locks::Connection::LocalServer::VERSION,
    ServerId		=> $host,
    LockRequest		=> undef,
    Quit		=> undef,
    AllowConnection	=> undef,
};

sub default_options {
    return $default_options;
}

sub default_port {
    return LOCAL_PORT;
}

sub connection_class {
    return "WEC::Locks::Connection::LocalServer";
}

sub init {
    my ($server, $params) = @_;
    my $options = $server->options;
    check_options($options, qw(ServerSoftware ServerVersion ServerId));
}

1;
