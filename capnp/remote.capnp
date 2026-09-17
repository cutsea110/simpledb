@0xa9ab30b6c567e6ae;

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

  smallInt  @0;
  integer   @1;
  varchar   @2;
  bool      @3;
  date      @4;
}

struct Column {
  name   @0 :Text;
  type   @1 :FieldType;
  length @2 :UInt32; # for varchar
}

struct Schema {
  # table schema

  columns @0 :List(Column);
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

interface RemoteConnection {
  # connection

  createStatement   @0 (sql :Text) -> (stmt :RemoteStatement);
  close             @1 () -> (tx :Int32);
  commit            @2 () -> (tx :Int32);
  rollback          @3 () -> (tx :Int32);

  getTableSchema    @4 (tblname :Text) -> (sch :Schema);
  getViewDefinition @5 (viewname :Text) -> (vwdef :ViewDef);
  getIndexInfo      @6 (tblname :Text) -> (indexes :List(IndexInfo));

  numsOfReadWrittenBlocks   @7 () -> (r: UInt32, w: UInt32);
  # extends for statistics by exercise 3.15
  numsOfTotalPinnedUnpinned @8 () -> (pinned: UInt32, unpinned: UInt32);
  # extends for statistics by exercise 4.18
  bufferCacheHitAssigned    @9 () -> (hit: UInt32, assigned: UInt32);
  # extends for statistics by exercise 4.18
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
    fields @0 :List(Text);       # group by these fields
    aggfns @1 :List(Aggregation); # aggregation functions
  }
  struct Aggregation {
    field @0 :Text;
    value @1 :Constant;
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
      int16   @0 :Int16;
      int32   @1 :Int32;
      string  @2 :Text;
      bool    @3 :Bool;
      date    @4 :Date;
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

  executeQuery  @0 () -> (result :RemoteResultSet, schema :Schema);
  executeUpdate @1 () -> (affected :Int32, committedTx :Int32);
  close         @2 () -> (tx :Int32);
  explainPlan   @3 () -> (planrepr :PlanRepr);
}


interface RemoteResultSet {
  # result set

  close   @0 () -> (tx :Int32);
  getRows @1 (limit :UInt16) -> (rows :List(Row)); # get records up to limit

  struct Row {
    # record

    values @0 :List(Value); # values follow Schema.columns order
  }
  struct Value {
    union {
      int16   @0 :Int16;
      int32   @1 :Int32;
      string  @2 :Text;
      bool    @3 :Bool;
      date    @4 :Date;
    }
  }
}
