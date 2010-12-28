package Net::Cassandra::libcassandra::Column;

use 5.008008;
use strict;
use warnings;
use Net::Cassandra::libcassandra;

sub value {
    my ($self) = @_;
    $self->{value};
}

sub name {
    my ($self) = @_;
    $self->{name};
}

sub timestamp {
    my ($self) = @_;
    $self->{timestamp};
}

1;
