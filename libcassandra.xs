
#ifdef __cplusplus
extern "C" {
#endif

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#ifdef __cplusplus
}
#endif

#ifdef vform
#undef vform
#endif

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

/*
#include <libcassandra/libgenthrift/Cassandra.h>
#include <libcassandra/libgenthrift/cassandra_types.h>
#include <libcassandra/libcassandra/cassandra.h>
#include <libcassandra/libcassandra/keyspace.h>
*/
#include <libgenthrift/Cassandra.h>
#include <libgenthrift/cassandra_types.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/keyspace.h>

#include "ppport.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace std;
using namespace org::apache::cassandra;
using namespace libcassandra;

typedef set<string> StringSet;
typedef StringSet::iterator StringSetIt;

typedef map<string, string> StringMap;
typedef StringMap::iterator StringMapIt;

typedef map< string, map<string, string> > StringMapMap;
typedef StringMapMap::iterator StringMapMapIt;

typedef vector<Column> ColumnVector;
typedef ColumnVector::iterator ColumnVectorIt;

typedef vector<SuperColumn> SuperColumnVector;
typedef SuperColumnVector::iterator SuperColumnVectorIt;

typedef vector<ColumnOrSuperColumn> ColumnOrSuperColumnVector;
typedef ColumnOrSuperColumnVector::iterator ColumnOrSuperColumnVectorIt;

string svToString(SV* sv) {
    int len = SvIOK(sv) || SvNOK(sv) ? strlen(SvPV_nolen(sv)) : SvCUR(sv);
    return string(SvPV_nolen(sv), len);
}

HV* columnToHV(Column* column) {
  HV *stash = (HV *)sv_2mortal((SV *)newHV());
  hv_store(stash, "value", strlen("value"), newSVpv(column->value.c_str(), column->value.size()), 0);
  hv_store(stash, "name", strlen("name"), newSVpv(column->name.c_str(), column->name.size()), 0);
  hv_store(stash, "timestamp", strlen("timestamp"), newSViv(column->timestamp), 0);

  sv_bless( newRV_noinc((SV*)stash), gv_stashpv( "Net::Cassandra::libcassandra::Column", 1 ));
  return stash;
}

AV* columnVectorToAV(ColumnVector* columns) {
  AV *stash = (AV *)sv_2mortal((SV *)newAV());

  if (columns->empty()) {
    return stash;
  }

  for(ColumnVectorIt it = columns->begin(); it != columns->end(); it++) {
    av_push(stash, newRV((SV *)columnToHV(&*it)));
  }
  return stash;
}

HV* superColumnToHV(SuperColumn* super_column) {
  HV *stash = (HV *)sv_2mortal((SV *)newHV());
  hv_store(stash, "name", strlen("name"), newSVpv(super_column->name.c_str(), super_column->name.size()), 0);
  hv_store(stash, "columns", strlen("columns"), newRV((SV *)columnVectorToAV(&super_column->columns)), 0);

  sv_bless( newRV_noinc((SV*)stash), gv_stashpv( "Net::Cassandra::libcassandra::SuperColumn", 1 ));
  return stash;
}

HV* columnOrSuperColumnToHV(ColumnOrSuperColumn* cosc) {
  HV *stash = (HV *)sv_2mortal((SV *)newHV());
  if (cosc->__isset.column) {
    hv_store(stash, "column", strlen("column"), newRV((SV *)columnToHV(&(cosc->column))), 0);
  }
  if (cosc->__isset.super_column) {
    hv_store(stash, "super_column", strlen("super_column"), newRV((SV *)superColumnToHV(&(cosc->super_column))), 0);
  }

  sv_bless( newRV_noinc((SV*)stash), gv_stashpv( "Net::Cassandra::libcassandra::ColumnOrSuperColumn", 1 ));
  return stash;
}

AV* columnOrSuperColumnVectorToAV(ColumnOrSuperColumnVector* coscs) {
  AV *stash = (AV *)sv_2mortal((SV *)newAV());

  if (coscs->empty()) {
    return stash;
  }

  for(ColumnOrSuperColumnVectorIt it = coscs->begin(); it != coscs->end(); it++) {
    av_push(stash, newRV((SV *)columnOrSuperColumnToHV(&*it)));
  }
  return stash;
}

AV* superColumnVectorToAV(SuperColumnVector* super_columns) {
  AV *stash = (AV *)sv_2mortal((SV *)newAV());

  if (super_columns->empty()) {
    return stash;
  }

  for(SuperColumnVectorIt it = super_columns->begin(); it != super_columns->end(); it++) {
    av_push(stash, newRV((SV *)superColumnToHV(&*it)));
  }
  return stash;
}

HV* stringMapMapToHV(StringMapMap* string_map_map) {
  HV *stash = (HV *)sv_2mortal((SV *)newHV());

  for (StringMapMapIt it1 = string_map_map->begin(); it1 != string_map_map->end(); it1++) {
    HV *stash2 = (HV *)sv_2mortal((SV *)newHV());
    for (StringMapIt it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
      hv_store(stash2, it2->first.c_str(), it2->first.size(), newSVpv(it2->second.c_str(), it2->second.size()), 0);
    }
    hv_store(stash, it1->first.c_str(), it1->first.size(), newRV((SV *)stash2), 0);
  }

  return stash;
}


MODULE = Net::Cassandra::libcassandra	PACKAGE = Net::Cassandra::libcassandra PREFIX=xs_cassandra_
##

Cassandra *
xs_cassandra_new(const string in_host, int in_port)
CODE:
  boost::shared_ptr<TTransport> socket(new TSocket(in_host, in_port));
  boost::shared_ptr<TTransport> transport;
  transport= boost::shared_ptr<TTransport> (new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

  CassandraClient *client= new(std::nothrow) CassandraClient(protocol);

  try {
    transport->open(); /* throws an exception */
  } catch (TTransportException &e) {
    croak("TTransportException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
  const char *CLASS = (char*)"Net::Cassandra::libcassandra";
  RETVAL = new Cassandra(client, in_host, in_port);
OUTPUT:
  RETVAL

string
Cassandra::getClusterName()

StringSet
Cassandra::getKeyspaces()

StringMap
Cassandra::getTokenMap(bool fresh)

Keyspace *
Cassandra::getKeyspace(const string name)
CODE:
  const char *CLASS = (char*)"Net::Cassandra::libcassandra::Keyspace";
  RETVAL = &*(THIS->getKeyspace(name));
OUTPUT:
  RETVAL

void
xs_cassandra_keyspace_insertColumn(Keyspace *ks, const string key, const string column_family, const string super_column_name, const string column_name, const string value)
CODE:
  try {
    ks->insertColumn(key, column_family, super_column_name, column_name, value);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }

void
xs_cassandra_keyspace_remove(Keyspace *ks, const string key, const string column_family, const string super_column_name, const string column_name)
CODE:
  try {
    ks->remove(key, column_family, super_column_name, column_name);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }

Column
xs_cassandra_keyspace_getColumn(Keyspace *ks, const string key, const string column_family, const string super_column_name, const string column_name)
CODE:
  const char *CLASS = (char*)(char*)"Net::Cassandra::libcassandra::Column";
  try {
    RETVAL = ks->getColumn(key, column_family, super_column_name, column_name);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

ColumnOrSuperColumn
xs_cassandra_keyspace_getColumnOrSuperColumn(Keyspace *ks, const string key, const string column_family, const string super_column_name, const string column_name)
CODE:
  const char *CLASS = (char*)(char*)"Net::Cassandra::libcassandra::Column";
  try {
    RETVAL = ks->getColumnOrSuperColumn(key, column_family, super_column_name, column_name);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

string
xs_cassandra_keyspace_getColumnValue(Keyspace *ks, const string key, const string column_family, const string super_column_name, const string column_name)
CODE:
  try {
    RETVAL = ks->getColumnValue(key, column_family, super_column_name, column_name);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

SuperColumn
xs_cassandra_keyspace_getSuperColumn(Keyspace *ks, const string key, const string column_family, const string super_column_name)
CODE:
  char *CLASS = (char*)"Net::Cassandra::libcassandra::SuperColumn";
  try {
    RETVAL = ks->getSuperColumn(key, column_family, super_column_name);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

ColumnVector
xs_cassandra_keyspace_getSliceNames(Keyspace *ks, const string key, const ColumnParent *col_parent, SlicePredicate *pred)
CODE:
  try {
    RETVAL = ks->getSliceNames(key, *col_parent, *pred);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

ColumnVector
xs_cassandra_keyspace_getSliceRange(Keyspace *ks, const string key, const string column_family, const string super_column, const string start, const string finish, int reversed, int count)
CODE:
  try {
     /* StringVector column_names */
    ColumnParent* col_parent = new ColumnParent();
    col_parent->column_family = column_family;
    col_parent->super_column = super_column;
    if (super_column.length() > 0) {
        col_parent->__isset.super_column = true;
    }

    SlicePredicate *pred = new SlicePredicate();
    pred->slice_range.start = start;
    pred->slice_range.finish = finish;
    pred->slice_range.reversed = reversed;
    pred->slice_range.count = count;
    RETVAL = ks->getSliceRange(key, *col_parent, *pred);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

ColumnOrSuperColumnVector
xs_cassandra_keyspace_getColumnOrSuperColumnSliceRange(Keyspace *ks, const string key, const string column_family, const string super_column, const string start, const string finish, int reversed, int count)
CODE:
  try {
     /* StringVector column_names */
    ColumnParent* col_parent = new ColumnParent();
    col_parent->column_family = column_family;
    col_parent->super_column = super_column;
    if (super_column.length() > 0) {
        col_parent->__isset.super_column = true;
    }

    SlicePredicate *pred = new SlicePredicate();
    pred->slice_range.start = start;
    pred->slice_range.finish = finish;
    pred->slice_range.reversed = reversed;
    pred->slice_range.count = count;
    RETVAL = ks->getColumnOrSuperColumnSliceRange(key, *col_parent, *pred);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

int
xs_cassandra_keyspace_getCount(Keyspace *ks, const string key, const string column_family, const string super_column)
CODE:
  try {
    ColumnParent* col_parent = new ColumnParent();
    col_parent->column_family = column_family;
    col_parent->super_column = super_column;
    if (super_column.length() > 0) {
        col_parent->__isset.super_column = true;
    }
    RETVAL = ks->getCount(key, *col_parent);
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

string
xs_cassandra_keyspace_getName(Keyspace *ks)
CODE:
  try {
    RETVAL = ks->getName();
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

StringMapMap
xs_cassandra_keyspace_getDescription(Keyspace *ks)
CODE:
  try {
    RETVAL = ks->getDescription();
  } catch (InvalidRequestException &e) {
    croak("InvalidRequestException: %s", e.what());
  } catch (UnavailableException &e) {
    croak("UnavailableException: %s", e.what());
  } catch (TimedOutException &e) {
    croak("TimedOutException: %s", e.what());
  } catch (TProtocolException &e) {
    croak("TProtocolException: %s", e.what());
  } catch (NotFoundException &e) {
    croak("NotFoundException: %s", e.what());
  } catch (TException &e) {
    croak("TException: %s", e.what());
  }
OUTPUT:
  RETVAL

