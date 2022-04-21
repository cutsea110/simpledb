@0xf8b0374744cc9ebf;

interface Ping {
  struct PingRequest {
    name @0 :Text;
  }
  struct PingReply {
    message @0 :Text;
  }

  ping @0 (request: PingRequest) -> (reply: PingReply);
}
