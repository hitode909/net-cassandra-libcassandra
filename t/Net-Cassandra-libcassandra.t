package test::Net::Cassandra::libcassandra;
use strict;
use warnings;
use base qw(Test::Class);
use Path::Class;
use lib file(__FILE__)->dir->parent->subdir('lib')->stringify;

use Test::More;

my $cassnadra;

sub _use: Test(setup => 1) {
    use_ok 'Net::Cassandra::libcassandra'
}

sub test_connect : Test(setup => 1) {
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

sub count_column : Test(2) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;

    my $count = $keyspace->getCount($key, "Standard1", "");
    is($count, 0);

    for(1..5) {
        $keyspace->insertColumn($key, "Standard1", "", $_, $_);
    }

    $count = $keyspace->getCount($key, "Standard1", "");
    is($count, 5);
}

sub count_super_column : Tests {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;

    for(1..5) {
        $keyspace->insertColumn($key, "Super1", "super_column".$_ , 'name'.$_, 'value'.$_);
    }

    my $count = $keyspace->getCount($key, "Super1", '');
    is($count, 5);

    $count = $keyspace->getCount($key, "Super1", "super_column3");
    is($count, 1);
}

sub get_column : Test(4) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $name = rand;
    my $value = rand;
    $keyspace->insertColumn($key, "Standard1", "", $name, $value);

    my $res = $keyspace->getColumn($key, "Standard1", "", $name);

    is($res->value, $value, 'value');
    is($res->name, $name, 'name');
    ok(abs($res->timestamp / 10**8) - time < 1, 'timestamp');

    $keyspace->remove($key, "Standard1", "", $name);

    my $res_not_exist = eval {
        $keyspace->getColumnValue($key, "Standard1", "", $name);
    };
    is ($res_not_exist, undef, 'deleted');
}

sub get_super_column : Test(23) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $super_column_name = rand;
    for(0..4) {
        $keyspace->insertColumn($key, "Super1", $super_column_name, 'name'.$_, 'value'.$_);
    }

    my $res = $keyspace->getSuperColumn($key, "Super1", $super_column_name);
    isa_ok $res, 'Net::Cassandra::libcassandra::SuperColumn';
    is $res->name, $super_column_name;
    for(0..4) {
        isa_ok $res->columns->[$_], 'Net::Cassandra::libcassandra::Column';
        is $res->columns->[$_]->name, 'name'.$_, 'name';
        is $res->columns->[$_]->value, 'value'.$_, 'value';
        ok abs($res->columns->[$_]->timestamp / 10**8) - time < 1, 'timestamp';
    }
    ok($res->columns);
}

sub slice_standard_column : Test(5) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    for(0..4) {
        $keyspace->insertColumn($key, "Standard1", "", 'name'.$_, 'value'.$_);
    }
    my $res = $keyspace->getSliceRange($key, "Standard1", "", "", "", 0, 3);
    isa_ok $res->[0], 'Net::Cassandra::libcassandra::Column';
    is scalar @$res, 3;
    is $res->[0]->name, 'name0';
    is $res->[1]->name, 'name1';
    is $res->[2]->name, 'name2';
}

sub slice_super_column_column : Tests {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    for(0..4) {
        $keyspace->insertColumn($key, "Super1", "super_column".$_ , 'name1-'.$_, 'value1-'.$_);
        $keyspace->insertColumn($key, "Super1", "super_column".$_ , 'name2-'.$_, 'value2-'.$_);
    }
    my $res = $keyspace->getSliceRange($key, "Super1", "super_column0", "", "", 0, 3);
    ok $res;
    is scalar @$res, 2;
    isa_ok $res->[0], 'Net::Cassandra::libcassandra::Column';
    is $res->[0]->value, 'value1-0';
}

sub slice_super_column_super_column : Tests {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    for(0..4) {
        $keyspace->insertColumn($key, "Super1", "super_column".$_ , 'name1-'.$_, 'value1-'.$_);
        $keyspace->insertColumn($key, "Super1", "super_column".$_ , 'name2-'.$_, 'value2-'.$_);
    }
    my $res = $keyspace->getSuperSliceRange($key, "Super1", "", "", "", 0, 3);
    ok $res;
    is scalar @$res, 3;
    isa_ok $res->[0], 'Net::Cassandra::libcassandra::SuperColumn';
    is $res->[0]->name, 'super_column0';
    is $res->[0]->columns->[0]->value, 'value1-0';
}

__PACKAGE__->runtests;

1;
