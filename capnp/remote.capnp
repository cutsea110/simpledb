@0xa9ab30b6c567e6ae;

struct Tuple(T, U) {
  fst @0 :T;
  snd @1 :U;
}

struct Map(Key, Value) {
  entries @0 :List(Entry);

  struct Entry {
    key   @0 :Key;
    value @1 :Value;
  }
}


enum FieldType {
  integer @0;
  varchar @1;
}

struct FieldInfo {
  type   @0 :FieldType;
  length @1 :Int32;
}

struct Schema {
  fields @0 :List(Text);
  info   @1 :Map(Text, FieldInfo);
}

struct ViewDef {
  vwname @0 :Text;
  vwdef  @1 :Text;
}

struct IndexInfo {
  idxname @0 :Text;
  fldname @1 :Text;
}


interface RemoteDriver {
  connect    @0 (dbname :Text) -> (conn :RemoteConnection);
  getVersion @1 () -> (ver :Version);

  struct Version {
    majorVer @0 :Int32;
    minorVer @1 :Int32;
  }
}

interface VoidBox {
  read @0 () -> (void :Void);
}

interface RemoteConnection {
  createStatement   @0 (sql :Text) -> (stmt :RemoteStatement);
  close             @1 () -> (res :VoidBox);
  commit            @2 () -> (tx :Int32);
  rollback          @3 () -> (tx :Int32);

  getTableSchema    @4 (tblname :Text) -> (sch :Schema);
  getViewDefinition @5 (viewname :Text) -> (vwdef :ViewDef);
  getIndexInfo      @6 (tblname :Text) -> (ii :Map(Text, IndexInfo));
}

interface RemoteStatement {
  struct PlanRepr {
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
    idxname    @0 :Text;
    idxfldname @1 :Text;
    joinfld    @2 :Text;
  }
  struct IndexSelectScan {
    idxname    @0 :Text;
    idxfldname @1 :Text;
    val        @2 :Constant;
  }
  struct GroupByScan {
    fields @0 :List(Text);
    aggfns @1 :List(Tuple(Text, Constant));
  }
  struct Materialize {
  }
  struct MergeJoinScan {
    fldname1 @0 :Text;
    fldname2 @1 :Text;
  }
  struct SortScan {
    compflds @0 :List(Text);
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
      int32  @0 :Int32;
      string @1 :Text;
    }
  }
  struct Predicate {
    terms @0 :List(Term);
  }
  struct Term {
    lhs @0 :Expression;
    rhs @1 :Expression;
  }
  struct Expression {
    union {
      val     @0 :Constant;
      fldname @1 :Text;
    }
  }

  executeQuery  @0 () -> (result :RemoteResultSet);
  executeUpdate @1 () -> (affected :Affected);
  close         @2 () -> (res :VoidBox);
  explainPlan   @3 () -> (planrepr :PlanRepr);
}

interface Affected {
  read @0 () -> (affected :Int32);
}

interface BoolBox {
  read @0 () -> (val :Bool);
}

interface Int32Box {
  read @0 () -> (val :Int32);
}
interface StringBox {
  read @0 () -> (val :Text);
}

interface RemoteResultSet {
  next        @0 () -> (val :BoolBox);
  close       @1 () -> (res :VoidBox);
  getMetadata @2 () -> (metadata :RemoteMetaData);
  getInt32    @3 (fldname :Text) -> (val :Int32Box);
  getString   @4 (fldname :Text) -> (val :StringBox);
  getRow      @5 () -> (row :Row);
  getRows     @6 (limit :UInt32) -> (count :UInt32, rows :List(Row));

  struct Row {
    map @0 :Map(Text, Value);
  }
  struct Value {
    union {
      int32  @0 :Int32;
      string @1 :Text;
    }
  }
}

interface RemoteMetaData {
  getSchema @0 () -> (sch :Schema);
}
