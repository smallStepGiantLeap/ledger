# ledger

A gRPC service on the vikrant platform, serving
`ledger.v1.LedgerService`.

Write your code in [`internal/handlers/`](internal/handlers/). Everything
around it is already in place:

- a hardened server that protects itself from toxic clients;
- a drain that moves long-lived streams to other replicas on shutdown;
- a canonical client for your callers;
- a rootless image;
- a canary rollout;
- deny-by-default networking;
- autoscaling on in-flight RPCs;
- CI that proves all of it on every push.

## Who owns what

| Path | Owner | Changes when |
|---|---|---|
| `service.yaml`, `proto/` | you | you edit them |
| `internal/handlers/`, `cmd/`, `internal/platform/` | you, from day one | you edit them; the platform never writes here again |
| `client/` (canonical client and stubs), `deploy/`, `Dockerfile`, `Makefile`, `buf*.yaml`, `.github/` | the platform | regenerated from `service.yaml` and the proto |

To change who may call you, or what you may call, edit `service.yaml` and
push. The platform regenerates the derived files and pushes a commit to the
same branch, so your PR shows the intent and its effect together. CI's
`check-stamp` fails if a platform-owned file is edited by hand.

## Run it

```sh
make run                  # the server on :50051, metrics on :9090
make test                 # unit tests + the toxic-client conformance suite
make tools check-generated  # stubs match the proto
make image                # the rootless production image
```

## Calling this service from another one

```go
import (
	ledgerv1 "github.com/smallStepGiantLeap/ledger/client/gen/ledger/v1"
	"github.com/smallStepGiantLeap/ledger/client"
)

cc, err := client.Dial(client.Target)
c := ledgerv1.NewLedgerServiceClient(cc)
```

Add the caller to this service's `inbound` list, and this service to the
caller's `outbound` list. Both sides are checked: an access rule that only
one side declares is reported when the platform renders either repository.
