@0xa9ab30b6c567e6ae;

interface RemoteDriver {
  connect @0 (connString: Text) -> (conn: RemoteConnection);
}

interface RemoteConnection {
  connectionId @0 () -> (connId: UInt32);
}
