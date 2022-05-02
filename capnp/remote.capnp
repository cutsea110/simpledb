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

interface RemoteDriver {
  connect    @0 (connString: Text) -> (conn: RemoteConnection);
  getVersion @1 () -> (ver: Version);

  struct Version {
    majorVer @0 :Int32;
    minorVer @1 :Int32;
  }
}

interface RemoteConnection {
  struct Transaction {
    txNum @0 :Int32;
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

  create @0 (sql: Text) -> (stmt: RemoteStatement);
  close    @1();
  commit   @2 ();
  rollback @3 ();

  getTransaction    @4 () -> (tx: Transaction);
  getTableSchema    @5 () -> (sch: Schema);
  getViewDefinition @6 () -> (vwdef: ViewDef);
  getIndexInfo      @7 () -> (ii: Map(Text, IndexInfo));
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
    reads        @11 :Int32;
    writes       @12 :Int32;
    subPlanReprs @13 :List(PlanRepr);
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

  executeQuery  @0 () -> (result: RemoteResultSet);
  executeUpdate @1 () -> (affected: Int32);
  close         @2 ();
  explainPlan   @3 () -> (planrepr: PlanRepr);
}

interface RemoteResultSet {
  next          @0 () -> (exists: Bool);
  close         @1 ();
  getRecordsAll @2 () -> (results: Record);

  struct Results {
    count   @0 :Int32;
    records @1 :List(Record);
  }
  struct Record {
    map @0 :Map(Text, Value);
  }
  struct Value {
    union {
      int32  @0 :Int32;
      string @1 :Text;
    }
  }
}
