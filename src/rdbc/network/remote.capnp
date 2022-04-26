@0xa9ab30b6c567e6ae;

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
  create @0 (sql: Text) -> (stmt: RemoteStatement);
  close    @1();
  commit   @2 ();
  rollback @3 ();

  getTransaction    @4 () -> (tx: Transaction);
  getTableSchema    @5 () -> (sch: Schema);
  getViewDefinition @6 () -> (vwdef: ViewDef);
  getIndexInfo      @7 () -> (ii: Map(Text, IndexInfo));

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
}

interface RemoteStatement {
  executeQuery  @0 () -> (result: RemoteResultSet);
  executeUpdate @1 () -> (affected: Int32);
  close         @2 ();
  explainPlan   @3 () -> (planrepr: RemotePlanRepr);
}

interface RemoteResultSet {
  next        @0 () -> (exists: Bool);
  close       @1 ();
  getI32      @2 (fldname: Text) -> (val: Int32);
  getString   @3 (fldname: Text) -> (val: Text);
  getMetaData @4 () -> (meta: RemoteMetaData);
}

interface RemoteMetaData {
  getColumnCount       @0 () -> (size: Int32);
  getColumnName        @1 (column: Int32) -> (name: Text);
  getColumnType        @2 (column: Int32) -> (type: DataType);
  getColumnDisplaySize @3 (column: Int32) -> (size: Int32);

  enum DataType {
    int32   @0;
    varchar @1;
  }
}

interface RemotePlanRepr {
  operation @0 () -> (op: Operation);
  reads     @1 () -> (result: Int32);
  writes    @2 () -> (result: Int32);
  subPlanReprs @3 () -> (children: List(RemotePlanRepr));
  
  enum Operation {
    indexJoinScan          @0;
    indexSelectScan        @1;
    groupByScan            @2;
    materialize            @3;
    mergeJoinScan          @4;
    sortScan               @5;
    multibufferProductScan @6;
    productScan            @7;
    projectScan            @8;
    selectScan             @9;
    tableScan              @10;
  }
}