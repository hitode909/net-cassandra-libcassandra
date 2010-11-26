package Net::Cassandra::libcassandra::SuperColumn;

use 5.008008;
use strict;
use warnings;
use Net::Cassandra::libcassandra;

sub name {
    my ($self) = @_;
    Net::Cassandra::libcassandra::supercolumn_getName($self);
}

sub columns {
    my ($self) = @_;
    Net::Cassandra::libcassandra::supercolumn_getColumns($self);
}

1;
