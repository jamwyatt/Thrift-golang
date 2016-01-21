
# Thrift and golang experiment

struct Work {
    1: string source,
    2: i32 priority,
    3: i32 timeout,
}

enum Status {
  good = 1,
  bad = 2,
}

struct Result {
    1: Status status,
    2: i32 duration,
}

service producer {
    oneway  void    SendWorkAsync(1:Work w),
            Result  SendWorkSync(1:Work w),
}
