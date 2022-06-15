@0xa9ab30b6c567e6ae;

struct Tuple(T, U) {
  # generic pair

  fst @0 :T;
  snd @1 :U;
}

struct Map(Key, Value) {
  # generic map

  entries @0 :List(Entry);

  struct Entry {
    key   @0 :Key;
    value @1 :Value;
  }
}

struct Date {
  # A standard Gregorian calendar date

  year  @0 :Int16;
  # The year. Must include the century.
  # Negative value indicates BC.

  month @1 :UInt8; # Month number, 1-12.
  day   @2 :UInt8; # Day number, 1-31.
}

enum FieldType {
  # support just only signed/unsigned integer family, varchar, bool and date, now

  word      @0;
  uWord     @1;
  short     @2;
  uShort    @3;
  integer   @4;
  uInteger  @5;
  varchar   @6;
  bool      @7;
  date      @8;
}

struct FieldInfo {
  # field's information

  type   @0 :FieldType;
  length @1 :Int32;      # for varchar
}

struct Schema {
  # table schema

  fields @0 :List(Text);
  info   @1 :Map(Text, FieldInfo);
}

struct ViewDef {
  # view definition

  vwname @0 :Text;  # view name
  vwdef  @1 :Text;  # sql as view definition
}

struct IndexInfo {
  idxname @0 :Text; # index name
  fldname @1 :Text; # field name
}


interface RemoteDriver {
  # driver

  connect    @0 (dbname :Text) -> (conn :RemoteConnection);
  getVersion @1 () -> (ver :Version);

  struct Version {
    majorVer @0 :Int32; # major version
    minorVer @1 :Int32; # minor version
  }
}

interface TxBox {
  read @0 () -> (tx :Int32);
}

interface RemoteConnection {
  # connection

  createStatement   @0 (sql :Text) -> (stmt :RemoteStatement);
  close             @1 () -> (res :TxBox);
  commit            @2 () -> (tx :Int32);
  rollback          @3 () -> (tx :Int32);

  getTableSchema    @4 (tblname :Text) -> (sch :Schema);
  getViewDefinition @5 (viewname :Text) -> (vwdef :ViewDef);
  getIndexInfo      @6 (tblname :Text) -> (ii :Map(Text, IndexInfo));

  numsOfReadWrittenBlocks @7 () -> (r: UInt32, w: UInt32); # extends for statistics by exercise 3.15
}

interface RemoteStatement {
  # statement

  struct PlanRepr {
    # representation for plan

    operation :union {
      indexJoinScan          @0  :IndexJoinScan;
      indexSelectScan        @1  :IndexSelectScan;
      groupByScan            @2  :GroupByScan;
      materialize            @3  :Materialize;
      mergeJoinScan          @4  :MergeJoinScan;
      sortScan               @5  :SortScan;
      multibufferProductScan @6  :MultibufferProductScan;
      productScan            @7  :ProductScan;
      projectScan            @8  :ProjectScan;
      selectScan             @9  :SelectScan;
      tableScan              @10 :TableScan;
    }
    reads                    @11 :Int32;
    writes                   @12 :Int32;
    subPlanReprs             @13 :List(PlanRepr);
  }

  struct IndexJoinScan {
    idxname    @0 :Text; # index name
    idxfldname @1 :Text; # index field
    joinfld    @2 :Text; # join key
  }
  struct IndexSelectScan {
    idxname    @0 :Text;     # index name
    idxfldname @1 :Text;     # index field
    val        @2 :Constant; # value
  }
  struct GroupByScan {
    fields @0 :List(Text);                  # group by these fields
    aggfns @1 :List(Tuple(Text, Constant)); # aggregation functions
  }
  struct Materialize {
  }
  struct MergeJoinScan {
    fldname1 @0 :Text; # field name 1
    fldname2 @1 :Text; # field name 2
  }
  struct SortScan {
    compflds @0 :List(Text); # compared fields
  }
  struct MultibufferProductScan {
  }
  struct ProductScan {
  }
  struct ProjectScan {
  }
  struct SelectScan {
    pred @0 :Predicate;
  }
  struct TableScan {
    tblname @0 :Text;
  }

  struct Constant {
    union {
      int8    @0 :Int8;
      uint8   @1 :UInt8;
      int16   @2 :Int16;
      uint16  @3 :UInt16;
      int32   @4 :Int32;
      uint32  @5 :UInt32;
      string  @6 :Text;
      bool    @7 :Bool;
      date    @8 :Date;
    }
  }
  struct Predicate {
    terms @0 :List(Term);
  }
  struct Term {
    lhs @0 :Expression; # left hand side
    rhs @1 :Expression; # right hand side
  }
  struct Expression {
    union {
      val     @0 :Constant; # value
      fldname @1 :Text;     # field name
    }
  }

  executeQuery  @0 () -> (result :RemoteResultSet);
  executeUpdate @1 () -> (affected :Affected);
  close         @2 () -> (res :TxBox);
  explainPlan   @3 () -> (planrepr :PlanRepr);
}

interface Affected {
  read        @0 () -> (affected :Int32);
  committedTx @1 () -> (tx :Int32);
}

interface Int8Box {
  read @0 () -> (val :Int8);
}
interface UInt8Box {
  read @0 () -> (val :UInt8);
}
interface Int16Box {
  read @0 () -> (val :Int16);
}
interface UInt16Box {
  read @0 () -> (val :UInt16);
}
interface Int32Box {
  read @0 () -> (val :Int32);
}
interface UInt32Box {
  read @0 () -> (val :UInt32);
}
interface StringBox {
  read @0 () -> (val :Text);
}
interface BoolBox {
  read @0 () -> (val :Bool);
}
interface DateBox {
  read @0 () -> (val :Date);
}


interface RemoteResultSet {
  # result set

  next        @0  () -> (val :BoolBox);
  close       @1  () -> (res :TxBox);
  getMetadata @2  () -> (metadata :RemoteMetaData);
  getInt8     @3  (fldname :Text) -> (val :Int8Box);
  getUInt8    @4  (fldname :Text) -> (val :UInt8Box);
  getInt16    @5  (fldname :Text) -> (val :Int16Box);
  getUInt16   @6  (fldname :Text) -> (val :UInt16Box);
  getInt32    @7  (fldname :Text) -> (val :Int32Box);
  getUInt32   @8  (fldname :Text) -> (val :UInt32Box);
  getString   @9  (fldname :Text) -> (val :StringBox);
  getBool     @10 (fldname :Text) -> (val :BoolBox);
  getDate     @11 (fldname :Text) -> (val :DateBox);
  getRow      @12 () -> (row :Row); # get one record
  getRows     @13 (limit :UInt32) -> (count :UInt32, rows :List(Row)); # get records up to limit

  struct Row {
    # record

    map @0 :Map(Text, Value);
  }
  struct Value {
    union {
      int8    @0 :Int8;
      uint8   @1 :UInt8;
      int16   @2 :Int16;
      uint16  @3 :UInt16;
      int32   @4 :Int32;
      uint32  @5 :UInt32;
      string  @6 :Text;
      bool    @7 :Bool;
      date    @8 :Date;
    }
  }
}

interface RemoteMetaData {
  # metadata

  getSchema @0 () -> (sch :Schema);
}
