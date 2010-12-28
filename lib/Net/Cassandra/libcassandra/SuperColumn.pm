package Net::Cassandra::libcassandra::SuperColumn;

use 5.008008;
use strict;
use warnings;
use Net::Cassandra::libcassandra;

sub name {
    my ($self) = @_;
    $self->{name};
}

sub columns {
    my ($self) = @_;
    $self->{columns};
}

1;
