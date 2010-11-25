package test::Net::Cassandra::libcassandra;
use strict;
use warnings;
use base qw(Test::Class);
use Path::Class;
use lib file(__FILE__)->dir->subdir('lib')->stringify;

use Test::More;

my $cassnadra;

sub _use: Test(1) {
    use_ok 'Net::Cassandra::libcassandra'
}

sub test_connect : Test(1) {
    my $self = shift;
    $self->{cassandra} = Net::Cassandra::libcassandra::new('localhost', 9160);
    isa_ok($self->{cassandra}, 'Net::Cassandra::libcassandra');
}



sub test_connect_to_nonexist_server : Test(1) {
    my $cassandra_non;
    eval {
        $cassandra_non = Net::Cassandra::libcassandra::new('localhost', 9161);
    };
    warn $@ if $@;
    is($cassandra_non, undef);
}

sub test_get_nonexist_column : Test(1) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");

    my $res_not_exist = eval {
        $keyspace->getColumnValue("key", "Standard1", "", "not_exist_column");
    };
    warn $@ if $@;
    is ($res_not_exist, undef);
}

sub set_get_delete_value : Test(2) {
    my $self = shift;
    $self->{cassandra} = Net::Cassandra::libcassandra::new('localhost', 9160);
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $name = rand;
    my $value = rand;
    $keyspace->insertColumn($key, "Standard1", "", $name, $value);

    my $res = $keyspace->getColumnValue($key, "Standard1", "", $name);
    is($res, $value);

    $keyspace->remove($key, "Standard1", "", $name);

    my $res_not_exist = eval {
        $keyspace->getColumnValue($key, "Standard1", "", $name);
    };
    is ($res_not_exist, undef);
}

__PACKAGE__->runtests;

1;
