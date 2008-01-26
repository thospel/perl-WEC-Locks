package WEC::Locks::Constants;
use 5.008;
use warnings;
use strict;

use Exporter::Tidy
    lock_types	=> [qw(LOCK_IMMEDIATE LOCK_DELAYED LOCK_QUERY LOCK_DROP)],
    response_types => [qw(RESPONSE_NOT_LOCKED RESPONSE_QUEUED
			  RESPONSE_LOCKED RESPONSE_UNLOCKED
			  RESPONSE_LOCK_DENIED RESPONSE_UNLOCK_DENIED
			  RESPONSE_DROPPED)],
    other	=> [qw(LOCAL_PORT REMOTE_PORT LOCAL_M_PORT REMOTE_M_PORT
		       LOCAL_QUIT DROP QUIT INFINITY REQUEST_START)];

use constant {
    # Default port
    LOCAL_PORT	=> "/var/run/LocalLockServer",
    REMOTE_PORT => 4387,
    LOCAL_M_PORT=> 5386,
    REMOTE_M_PORT=> 5387,

    LOCK_IMMEDIATE	=> 1,
    LOCK_DELAYED	=> 2,
    LOCK_QUERY		=> 3,
    LOCK_DROP		=> 4,
    LOCAL_QUIT		=> 5,
    DROP		=> 6,
    QUIT		=> 7,

    RESPONSE_NOT_LOCKED	=> 1,	# 255
    RESPONSE_QUEUED	=> 2,	# 253
    RESPONSE_LOCKED	=> 3,	# 252
    RESPONSE_UNLOCKED	=> 4,	# 254
    RESPONSE_LOCK_DENIED=> 5,	# 451
    RESPONSE_UNLOCK_DENIED=> 6,	# 452
    RESPONSE_DROPPED	=> 7,	# 256

    INFINITY		=> 9**9**9,
    REQUEST_START	=> "1",
};

1;
