package WEC::Locks::RemoteClient;
use 5.006;
use strict;
use warnings;
use Carp;
use Sys::Hostname;

my $host = hostname;
$host =~ s/\..*//s;

use WEC::Locks::Connection::RemoteClient;
use WEC::Locks::Constants qw(REMOTE_PORT);
use WEC::Locks::Utils qw(check_options);

our $VERSION = '0.01';

our @CARP_NOT	= qw(WEC::Client WEC::FieldClient WEC::Locks::Utils);

use base qw(WEC::Client);

my $default_options = {
    %{__PACKAGE__->SUPER::client_options},
    Greeting		=> undef,
    ClientSoftware	=> "WEC::Locks",
    ClientVersion	=> $WEC::Locks::Connection::RemoteClient::VERSION,
    ClientId		=> $host,
    SendPeriod		=> 10,
    ReceivePeriod	=> 30,
    Sync		=> undef,
    LockResponse	=> undef,
    LockAcquired	=> undef,
    Dropped		=> undef,
};

sub default_options {
    return $default_options;
}

sub connection_class {
    return "WEC::Locks::Connection::RemoteClient";
}

sub init {
    my ($client, $params) = @_;
    my $options = $client->options;
    check_options($options, qw(ClientSoftware ClientVersion ClientId));
}

1;
