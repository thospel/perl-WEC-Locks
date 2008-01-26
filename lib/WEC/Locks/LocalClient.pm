package WEC::Locks::LocalClient;
use 5.006;
use strict;
use warnings;
use Carp;

use WEC::Locks::Connection::LocalClient;
use WEC::Locks::Constants qw(LOCAL_PORT);
use WEC::Locks::Utils qw(check_options);

our $VERSION = '0.01';

our @CARP_NOT	= qw(WEC::Client WEC::FieldClient WEC::Locks::Utils);

use base qw(WEC::Client);

my $default_options = {
    %{__PACKAGE__->SUPER::client_options},
    Greeting		=> undef,
    ClientSoftware	=> "WEC::Locks",
    ClientVersion	=> $WEC::Locks::Connection::LocalClient::VERSION,
    ClientId		=> "$>:$$",
    LockAcquired	=> undef,
    Quit		=> undef,
    Terminate		=> undef,
};

sub default_options {
    return $default_options;
}

sub connection_class {
    return "WEC::Locks::Connection::LocalClient";
}

sub init {
    my ($server, $params) = @_;
    my $options = $server->options;
    check_options($options, qw(ClientSoftware ClientVersion ClientId));
}

1;
