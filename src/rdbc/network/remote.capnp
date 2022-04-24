@0xa9ab30b6c567e6ae;

interface DriverAdapter(T) {
  connect @0 (connString: Text) -> (conn: ConnectionAdapter(T));
}

interface ConnectionAdapter(T) {}