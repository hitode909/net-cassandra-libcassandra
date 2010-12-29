package Net::Cassandra::libcassandra::Keyspace;

use 5.008008;
use strict;
use warnings;
use Net::Cassandra::libcassandra;

sub insertColumn {
    my ($self, $key, $cf, $scn, $cn, $value) = @_;
    Net::Cassandra::libcassandra::keyspace_insertColumn($self, $key, $cf, $scn, $cn, $value);
}

sub remove {
    my ($self, $key, $cf, $scn, $cn) = @_;
    Net::Cassandra::libcassandra::keyspace_remove($self, $key, $cf, $scn, $cn);
}

sub getColumn {
    my ($self, $key, $cf, $scn, $cn) = @_;
    Net::Cassandra::libcassandra::keyspace_getColumn($self, $key, $cf, $scn, $cn);
}

sub getColumnOrSuperColumn {
    my ($self, $key, $cf, $scn, $cn) = @_;
    Net::Cassandra::libcassandra::keyspace_getColumnOrSuperColumn($self, $key, $cf, $scn, $cn);
}

sub getColumnValue {
    my ($self, $key, $cf, $scn, $cn) = @_;
    Net::Cassandra::libcassandra::keyspace_getColumnValue($self, $key, $cf, $scn, $cn);
}

sub getSuperColumn {
    my ($self, $key, $cf, $scn) = @_;
    Net::Cassandra::libcassandra::keyspace_getSuperColumn($self, $key, $cf, $scn);
}

sub getSliceNames {
    my ($self, $key, $col_parent, $pred) = @_;
    Net::Cassandra::libcassandra::keyspace_getSliceNames($self, $key, $col_parent, $pred);
}

sub getSliceRange {
    my ($self, $key, $column_family, $super_column, $start, $finish, $reversed, $count) = @_;
    Net::Cassandra::libcassandra::keyspace_getSliceRange($self, $key, $column_family, $super_column, $start, $finish, $reversed, $count);
}

sub getSuperSliceRange {
    my ($self, $key, $column_family, $super_column, $start, $finish, $reversed, $count) = @_;
    Net::Cassandra::libcassandra::keyspace_getSuperSliceRange($self, $key, $column_family, $super_column, $start, $finish, $reversed, $count);
}

sub getCount {
    my ($self, $key, $column_family, $super_column) = @_;
    Net::Cassandra::libcassandra::keyspace_getCount($self, $key, $column_family, $super_column);
}

sub getName {
    my ($self) = @_;
    Net::Cassandra::libcassandra::keyspace_getName($self);
}

sub getDescription {
    my ($self) = @_;
    Net::Cassandra::libcassandra::keyspace_getDescription($self);
}

1;
