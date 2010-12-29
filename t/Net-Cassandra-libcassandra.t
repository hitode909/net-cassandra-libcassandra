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

sub test_get_nonexist_column : Test(2) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");

    my $res_not_exist = eval {
        $keyspace->getColumnValue("key", "Standard1", "", "not_exist_column");
    };
    like $@, qr/NotFoundException/;
    is ($res_not_exist, undef);
}

sub test_get_column_value : Test(1) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $name = rand;
    my $value = rand;
    $keyspace->insertColumn($key, "Standard1", "", $name, $value);

    my $res = $keyspace->getColumnValue($key, "Standard1", "", $name);
    is($res, $value);
}

sub test_remove_column : Test(3) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $name = rand;
    my $value = rand;
    $keyspace->insertColumn($key, "Standard1", "", $name, $value);

    my $res = $keyspace->getColumn($key, "Standard1", "", $name);
    is $res->value, $value;

    $keyspace->remove($key, "Standard1", "", $name);

    my $res_not_exist = eval {
        $keyspace->getColumn($key, "Standard1", "", $name);
    };
    like $@, qr/NotFoundException/;
    is ($res_not_exist, undef);
}

sub test_remove_super_column : Test(3) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $scn = rand;
    my $name = rand;
    my $value = rand;
    $keyspace->insertColumn($key, "Super1", $scn, $name, $value);

    my $res = $keyspace->getColumn($key, "Super1", $scn, $name);
    is $res->value, $value;

    $keyspace->remove($key, "Super1", $scn, "");

    my $res_not_exist = eval {
        $keyspace->getColumn($key, "Super1", $scn, $name);
    };
    like $@, qr/NotFoundException/;
    is ($res_not_exist, undef);
}

sub test_count_column : Test(2) {
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

sub test_count_super_column : Tests {
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

sub test_get_column_or_super_column_column : Test(4) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $name = rand;
    my $value = rand;
    $keyspace->insertColumn($key, "Standard1", "", $name, $value);
    my $res = $keyspace->getColumnOrSuperColumn($key, "Standard1", "", $name);

    isa_ok $res, 'Net::Cassandra::libcassandra::ColumnOrSuperColumn';
    isa_ok $res->column, 'Net::Cassandra::libcassandra::Column';
    is $res->column->name, $name;
    is $res->column->value, $value;
}

sub test_get_column_or_super_column_super_column : Test(4) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    my $scn = rand;
    my $name = rand;
    my $value = rand;
    $keyspace->insertColumn($key, "Super1", $scn, $name, $value);
    my $res = $keyspace->getColumnOrSuperColumn($key, "Super1", $scn, "");

    isa_ok $res, 'Net::Cassandra::libcassandra::ColumnOrSuperColumn';
    isa_ok $res->super_column, 'Net::Cassandra::libcassandra::SuperColumn';
    is $res->super_column->columns->[0]->name, $name;
    is $res->super_column->columns->[0]->value, $value;
}

sub test_get_column : Test(4) {
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

sub test_get_super_column : Test(23) {
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

sub test_slice_standard_column : Test(5) {
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

sub test_slice_cosc_standard_column : Test(5) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    for(0..4) {
        $keyspace->insertColumn($key, "Standard1", "", 'name'.$_, 'value'.$_);
    }
    my $res = $keyspace->getColumnOrSuperColumnSliceRange($key, "Standard1", "", "", "", 0, 3);
    isa_ok $res->[0], 'Net::Cassandra::libcassandra::ColumnOrSuperColumn';
    is scalar @$res, 3;
    is $res->[0]->column->name, 'name0';
    is $res->[1]->column->name, 'name1';
    is $res->[2]->column->name, 'name2';
}

sub test_slice_cosc_super_column : Test(5) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    for(0..4) {
        $keyspace->insertColumn($key, "Super1", 'scn'.$_, 'name'.$_, 'value'.$_);
    }
    my $res = $keyspace->getColumnOrSuperColumnSliceRange($key, "Super1", "", "", "", 0, 3);
    isa_ok $res->[0], 'Net::Cassandra::libcassandra::ColumnOrSuperColumn';
    is scalar @$res, 3;
    is $res->[0]->super_column->name, 'scn0';
    is $res->[1]->super_column->name, 'scn1';
    is $res->[2]->super_column->name, 'scn2';
}

sub test_slice_standard_column_reverse : Test(5) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $key = rand;
    for(0..4) {
        $keyspace->insertColumn($key, "Standard1", "", 'name'.$_, 'value'.$_);
    }
    my $res = $keyspace->getSliceRange($key, "Standard1", "", "", "", 1, 3);
    isa_ok $res->[0], 'Net::Cassandra::libcassandra::Column';
    is scalar @$res, 3;
    is $res->[0]->name, 'name4';
    is $res->[1]->name, 'name3';
    is $res->[2]->name, 'name2';
}

sub test_slice_super_column_column : Tests {
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

sub test_slice_super_column_super_column : Tests {
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

sub test_description : Test(7) {
    my $self = shift;
    my $keyspace = $self->{cassandra}->getKeyspace("Keyspace1");
    my $description = $keyspace->getDescription;
    ok $description;
    ok $description->{Standard1};
    ok $description->{Super1};
    is $description->{Standard1}->{Type}, 'Standard';
    is $description->{Standard1}->{CompareWith}, 'org.apache.cassandra.db.marshal.BytesType';
    is $description->{Super2}->{Type}, 'Super';
    is $description->{Super2}->{CompareWith}, 'org.apache.cassandra.db.marshal.UTF8Type';
}

__PACKAGE__->runtests;

1;
