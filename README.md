### Proof-of-Concept Rust Client for Deephaven Community Core

This is a proof-of-concept repository to showcase how a Rust client for
[Deephaven Community Core](https://github.com/deephaven/deephaven-core/)
might be structured.

It does run and can perform some table operations on a server at `localhost:10000`:

```bash
$ cargo run
```

However, there is no guaranteed API,
it only implements a small handful of functions,
and generally is not fit for actual use.
It is, for now, solely for example purposes and for possible future work.