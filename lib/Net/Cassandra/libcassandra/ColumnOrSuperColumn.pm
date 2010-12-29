package Net::Cassandra::libcassandra::ColumnOrSuperColumn;

use 5.008008;
use strict;
use warnings;
use Net::Cassandra::libcassandra;

sub column {
    my ($self) = @_;
    $self->{column};
}

sub super_column {
    my ($self) = @_;
    $self->{super_column};
}

1;
