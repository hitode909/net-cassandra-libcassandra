package Net::Cassandra::libcassandra::Column;

use 5.008008;
use strict;
use warnings;
use Net::Cassandra::libcassandra;

sub value {
    my ($self) = @_;
    Net::Cassandra::libcassandra::column_getValue($self);
}

sub name {
    my ($self) = @_;
    Net::Cassandra::libcassandra::column_getName($self);
}

sub timestamp {
    my ($self) = @_;
    Net::Cassandra::libcassandra::column_getTimestamp($self);
}

1;
